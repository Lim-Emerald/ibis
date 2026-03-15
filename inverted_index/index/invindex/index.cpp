#include "index/invindex/index.h"

#include <index/common/utils.h>
#include <strutext/morpho/models/rus_model.h>

#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/locale.hpp>
#include <boost/regex.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

#include "index/common/types.h"

namespace invindex {

namespace {

using RussianPos = strutext::morpho::RussianPos;
using RussianPosSerializer = strutext::morpho::russian::PosSerializer;

class InvertedIndexImpl : public IInvertedIndex {
   public:
    InvertedIndexImpl(const IndexConfig& config) : config_(config) {
        std::filesystem::create_directory(config_.index_dir);
        lsm_ = lsm::MakeIndexLsm(config_.index_dir + "/lsm", config_.lsm_options);
        doc_block_count_ = (config_.doc_count - 1) / config_.doc_block_size + 1;
    }

    void Put(const IndexKey& index_key, const PostingList& posting_list) { lsm_->Put(index_key, posting_list); }

    PostingList GetDocId(IndexKey index_key) const override {
        auto posting_list = lsm_->Get(index_key);
        if (posting_list) {
            return *posting_list;
        } else {
            return {};
        }
    }

    PostingList Scan(const std::optional<IndexKey>& start_key, const std::optional<IndexKey>& end_key) const override {
        auto scan = lsm_->Scan(start_key, end_key);
        auto object = scan->Next();
        PostingList result;
        while (object.has_value()) {
            for (const auto& doc_block_id : object->second) {
                result.add(object->first.back() * config_.doc_block_size + doc_block_id);
            }
            object = scan->Next();
        }
        return result;
    }

    virtual ~InvertedIndexImpl() { std::filesystem::remove_all(config_.index_dir); }

   private:
    uint32_t doc_block_count_;
    IndexConfig config_;
    std::unique_ptr<lsm::ILSM<lsm::IndexValue>> lsm_;
};

class InvertedIndexBuilderImpl : public IInvertedIndexBuilder {
   public:
    InvertedIndexBuilderImpl(const IndexConfig& config) { index_ = std::make_shared<InvertedIndexImpl>(config); }

    void Put(IndexKey index_key, PostingList posting_list) { index_->Put(index_key, posting_list); }

    std::shared_ptr<const IInvertedIndex> GetInvertedIndex() const { return index_; }

   private:
    std::shared_ptr<InvertedIndexImpl> index_;
};

std::shared_ptr<IInvertedIndexBuilder> MakeInvertedIndexBuilder(const IndexConfig& config) { return std::make_shared<InvertedIndexBuilderImpl>(config); }

static const std::regex kDateAttributePatternt(R"(^\$[A-Za-z_]+:\d{4}-\d{2}-\d{2}$)");

std::string FilterText(const std::string& text) {
    if (std::regex_match(text, kDateAttributePatternt)) {
        return text;
    }
    auto is_rus = [](char32_t ch) { return (ch >= U'А' && ch <= U'Я') || (ch >= U'а' && ch <= U'я') || ch == U'Ё' || ch == U'ё'; };
    std::u32string in = boost::locale::conv::utf_to_utf<char32_t>(text), out;
    for (size_t i = 0; i < in.size(); ++i) {
        if (is_rus(in[i])) {
            out.push_back(in[i]);
        } else {
            out.push_back(U' ');
        }
    }
    return boost::locale::conv::utf_to_utf<char>(out);
}

std::vector<std::string> GetMainForms(const std::string& word, const std::shared_ptr<RussianMorpher>& russian_morpher, std::locale loc) {
    strutext::morpho::MorphologistBase::LemList lemm_list;
    russian_morpher->Analize(boost::locale::to_lower(word, loc), lemm_list);
    if (lemm_list.empty()) {
        return {boost::locale::to_lower(word, loc)};
    }
    bool skip = true;
    std::vector<std::string> main_forms;
    for (const auto& lemma : lemm_list) {
        if (skip) {
            RussianPos::Ptr pos = RussianPosSerializer::Deserialize(lemma.attr_);
            if (pos->GetPosTag() > RussianPos::PREDICATE_PS) {
                break;
            }
            skip = false;
        }
        Token main_form;
        russian_morpher->GenMainForm(lemma.id_, main_form);
        main_forms.push_back(main_form);
    }
    return main_forms;
}

std::string AddSpecialSymbols(const std::string& token) { return "^" + token + "$"; }

std::vector<std::string> GetKGrams(const std::string& word, size_t k, std::locale loc) {
    std::vector<std::string> out;
    std::u32string in = boost::locale::conv::utf_to_utf<char32_t>(boost::locale::to_lower(word, loc));
    for (size_t len = 3; len <= std::min(k, in.size()); ++len) {
        for (size_t i = 0; i + len <= in.size(); ++i) {
            out.push_back(boost::locale::conv::utf_to_utf<char>(in.substr(i, len)));
        }
    }
    return out;
}

std::string GetDateAttributeName(std::string name, uint64_t pos) { return name + (pos < 10 ? "0" : "") + std::to_string(pos); }

uint64_t GetDate(const std::string& date) {
    std::tm tm = {};
    std::stringstream ss(date);
    ss >> std::get_time(&tm, "%Y-%m-%d");
    auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
    return static_cast<uint64_t>(tp.time_since_epoch().count() + 10800000000000ll);
}

class CrawlerImpl : public ICrawler {
   public:
    CrawlerImpl(const CrawlerConfig& config, const std::shared_ptr<RussianMorpher>& russian_morpher) : config_(config), russian_morpher_(russian_morpher) {
        config_.index_config.index_dir = config.work_dir + "/" + config_.index_config.index_dir;
    }

    void Process() override {
        if (config_.index_config.use_word_dictionary) {
            BuildWordDictionary();
        } else {
            BuildDocList();
        }
        BuildIndex();
    }

    std::vector<std::string> GetDocList() const override { return docs_; }

    std::shared_ptr<lsm::ILSM<lsm::DefaultValue>> GetWordDictionary() const override { return word_dictionary_; }

    std::shared_ptr<const IInvertedIndex> GetInvertedIndex() const override { return index_builder_->GetInvertedIndex(); }

   private:
    void BuildDocList() {
        for (const std::filesystem::directory_entry& e : std::filesystem::recursive_directory_iterator(config_.docbase_dir, std::filesystem::directory_options::skip_permission_denied)) {
            if (e.is_regular_file()) {
                docs_.push_back(e.path().string());
            }
        }
    }

    void BuildWordDictionary() {
        auto word_set = lsm::MakeDefaultLsm(config_.work_dir + "/word_set", config_.index_config.lsm_options);
        for (const std::filesystem::directory_entry& e : std::filesystem::recursive_directory_iterator(config_.docbase_dir, std::filesystem::directory_options::skip_permission_denied)) {
            if (e.is_regular_file()) {
                ReadDoc(e.path().string(), word_set);
            }
        }
        word_dictionary_ = lsm::MakeDefaultLsm(config_.work_dir + "/word_dictionary", config_.index_config.lsm_options);
        uint32_t feature_id = 0;
        auto scan = word_set->Scan(std::nullopt, std::nullopt);
        auto object = scan->Next();
        while (object.has_value()) {
            auto word = object->first;
            word_dictionary_->Put(word, SerializeUint32(feature_id++));
            object = scan->Next();
        }
    }

    class DocReader {
       public:
        DocReader(const std::string& path) : file_(path), iss_() {}

        std::optional<std::string> GetWord() {
            std::string word;
            if (iss_ >> word) {
                return word;
            } else {
                std::string line;
                while (std::getline(file_, line)) {
                    boost::algorithm::trim(line, loc_);
                    line = FilterText(line);
                    if (!line.empty()) {
                        iss_ = std::istringstream(line);
                        return GetWord();
                    }
                }
                return std::nullopt;
            }
        }

       private:
        std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
        std::ifstream file_;
        std::istringstream iss_;
    };

    std::vector<Token> GetTokens(const std::string& word) {
        if (std::regex_match(word, kDateAttributePatternt)) {
            std::vector<std::string> tokens;
            auto delim = word.find(':');
            std::string name = word.substr(1, delim - 1);
            uint64_t date = GetDate(word.substr(delim + 1, 10));
            for (uint64_t i = 0; i < 64 && date; ++i, date /= 2ull) {
                if (date % 2ull) {
                    tokens.push_back(GetDateAttributeName(name, i));
                }
            }
            tokens.push_back(GetDateAttributeName(name, 64));
            return tokens;
        }
        if (config_.index_config.use_k_gram) {
            if (GetMainForms(word, russian_morpher_, loc_).empty()) {
                return {};
            }
            return GetKGrams(AddSpecialSymbols(word), config_.index_config.use_k_gram, loc_);
        } else {
            auto tokens = GetMainForms(word, russian_morpher_, loc_);
            for (auto& token : tokens) {
                token = AddSpecialSymbols(token);
            }
            return tokens;
        }
    }

    void ReadDoc(const std::string& path, const std::unique_ptr<lsm::ILSM<lsm::DefaultValue>>& word_set) {
        std::cout << "crawler read doc " << docs_.size() << ": " << path << '\n';
        docs_.push_back(path);
        auto doc_reader = DocReader(path);

        std::optional<std::string> word;
        while ((word = doc_reader.GetWord())) {
            for (const auto& token : GetTokens(*word)) {
                word_set->Put(lsm::UserKey(token.begin(), token.end()), {});
            }
        }
    }

    void BuildIndex() {
        config_.index_config.doc_count = docs_.size();
        index_builder_ = MakeInvertedIndexBuilder(config_.index_config);
        for (uint32_t doc_id = 0; doc_id < docs_.size(); ++doc_id) {
            IndexDoc(doc_id);
        }
    }

    void IndexDoc(uint32_t doc_id) {
        std::cout << "crawler indexes doc " << doc_id << ": " << docs_[doc_id] << '\n';
        auto doc_reader = DocReader(docs_[doc_id]);

        std::optional<std::string> word;
        while ((word = doc_reader.GetWord())) {
            for (const auto& token : GetTokens(*word)) {
                if (config_.index_config.use_word_dictionary) {
                    auto feature_id = DeserializeUint32(*word_dictionary_->Get(lsm::UserKey(token.begin(), token.end())));
                    index_builder_->Put(TokenId{doc_id / config_.index_config.doc_block_size, feature_id}.Serialize(), PostingList({doc_id % config_.index_config.doc_block_size}));
                } else {
                    index_builder_->Put(TokenWithBlockId{doc_id / config_.index_config.doc_block_size, token}.Serialize(), PostingList({doc_id % config_.index_config.doc_block_size}));
                }
            }
        }
    }

   private:
    std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
    CrawlerConfig config_;
    std::vector<std::string> docs_;
    std::shared_ptr<IInvertedIndexBuilder> index_builder_;
    std::shared_ptr<RussianMorpher> russian_morpher_;
    std::shared_ptr<lsm::ILSM<lsm::DefaultValue>> word_dictionary_;
};

class SearchEngineImpl : public ISearchEngine {
   public:
    SearchEngineImpl(const IndexConfig& index_config, const std::shared_ptr<const IInvertedIndex>& index, const std::shared_ptr<const lsm::ILSM<lsm::DefaultValue>> word_dictionary,
                     const std::shared_ptr<RussianMorpher>& russian_morpher)
        : index_config_(index_config), index_(index), word_dictionary_(word_dictionary), russian_morpher_(russian_morpher) {
        doc_block_count_ = (index_config_.doc_count - 1) / index_config_.doc_block_size + 1;
    }

    PostingList Search(const std::string& request_line) const override {
        std::string request = request_line;
        boost::algorithm::trim(request, loc_);
        if (request.front() == '{' && request.back() == '}') {
            std::stringstream ss(request);
            std::string part;
            ss >> part >> part;
            std::optional<uint64_t> left_date, right_date;
            if (part != "*") {
                left_date = GetDate(part);
            }
            ss >> part;
            if (part.front() != '$') {
                throw "Search parsing error: invalid attribute style";
            }
            std::string name = part.substr(1);
            ss >> part;
            if (part != "*") {
                right_date = GetDate(part);
            }
            return DateRangeSearch(name, left_date, right_date);
        }
        if (request.front() == '[' && request.back() == ']') {
            std::stringstream ss(request);
            std::string part;
            ss >> part >> part;
            auto left_date = GetDate(part);
            ss >> part;
            if (part != ":") {
                throw "Search parsing error: invalid date segment style";
            }
            ss >> part;
            auto right_date = GetDate(part);
            return DateRangeSearch("creation_date", std::nullopt, left_date) & DateRangeSearch("deletion_date", right_date, std::nullopt);
        }
        if (request.front() == '(' && request.back() == ')') {
            auto is_op = [](char ch) { return ch == '&' || ch == '|' || ch == '!'; };
            std::string request_part;
            char op = '-';
            PostingList left_response;
            for (size_t i = 1; i < request.size(); ++i) {
                if (is_op(request[i]) || i + 1 == request.size()) {
                    if (is_op(request[i]) && op != request[i] && op != '-') {
                        throw "Search parsing error: different operations in ()-expression";
                    }
                    if (op == '-') {
                        if (op != '!') {
                            left_response = Search(request_part);
                            request_part.clear();
                        }
                    } else if (op == '!') {
                        if (i + 1 != request.size()) {
                            throw "Search parsing error: yet another ! in ()-expression";
                        }
                        auto right_response = Search(request_part);
                        right_response.flip(0, index_config_.doc_count);
                        return right_response;
                    } else {
                        auto right_response = Search(request_part);
                        if (op == '&') {
                            left_response &= right_response;
                        } else {
                            left_response |= right_response;
                        }
                        request_part.clear();
                    }
                    op = request[i];
                } else {
                    request_part.push_back(request[i]);
                }
            }
            return left_response;
        }
        auto first_wildcard = request.find('*');
        if (first_wildcard == std::variant_npos) {
            return SimpleSearch(request);
        } else if (first_wildcard + 1 == request.size()) {
            return PrefixSearch(request);
        } else {
            return WildcardSearch(request);
        }
    }

   private:
    PostingList GetDocId(const std::string& token) const {
        PostingList response;
        if (index_config_.use_word_dictionary) {
            auto s_feature_id = word_dictionary_->Get(lsm::UserKey(token.begin(), token.end()));
            if (s_feature_id) {
                for (uint32_t block_id = 0; block_id < doc_block_count_; ++block_id) {
                    auto doc_block_ids = index_->GetDocId(TokenId{block_id, DeserializeUint32(*s_feature_id)}.Serialize());
                    for (const auto& doc_block_id : doc_block_ids) {
                        response.add(block_id * index_config_.doc_block_size + doc_block_id);
                    }
                }
            }
        } else {
            for (uint32_t block_id = 0; block_id < doc_block_count_; ++block_id) {
                auto doc_block_ids = index_->GetDocId(TokenWithBlockId{block_id, token}.Serialize());
                for (const auto& doc_block_id : doc_block_ids) {
                    response.add(block_id * index_config_.doc_block_size + doc_block_id);
                }
            }
        }
        return response;
    }

    PostingList DateRangeSearch(const std::string& name, const std::optional<uint64_t>& left_date, const std::optional<uint64_t>& right_date, int pos = 63) const {
        uint64_t left, right;
        if (!left_date) {
            left = 0;
        } else {
            left = *left_date;
        }
        if (!right_date) {
            right = UINT64_MAX;
        } else {
            right = *right_date;
        }
        PostingList response = GetDocId(GetDateAttributeName(name, 64));
        for (; pos >= 0; --pos) {
            uint64_t upos = static_cast<uint64_t>(pos);
            bool lb = (left >> upos) % 2;
            bool rb = (right >> upos) % 2;
            if (lb == rb) {
                auto doc_ids = GetDocId(GetDateAttributeName(name, upos));
                if (!lb) {
                    doc_ids.flip(0, index_config_.doc_count);
                }
                response &= doc_ids;
            } else if (upos != 0) {
                auto doc_ids = GetDocId(GetDateAttributeName(name, upos));
                auto doc_ids_flip = doc_ids;
                doc_ids_flip.flip(0, index_config_.doc_count);
                auto left_response = LeftDateRangeSearch(name, left_date, pos - 1);
                auto right_response = RightDateRangeSearch(name, right_date, pos - 1);
                response &= (doc_ids_flip & left_response) | (doc_ids & right_response);
                break;
            }
        }
        if (!right_date) {
            auto add_docs = GetDocId(GetDateAttributeName(name, 64));
            add_docs.flip(0, index_config_.doc_count);
            response |= add_docs;
        }
        return response;
    }

    PostingList LeftDateRangeSearch(const std::string& name, const std::optional<uint64_t>& left_date, int pos = 65) const {
        uint64_t left;
        if (!left_date) {
            left = 0;
        } else {
            left = *left_date;
        }
        PostingList response = GetDocId(GetDateAttributeName(name, 64));
        for (; pos >= 0; --pos) {
            uint64_t upos = static_cast<uint64_t>(pos);
            bool lb = (left >> upos) % 2;
            auto doc_ids = GetDocId(GetDateAttributeName(name, upos));
            if (lb) {
                response &= doc_ids;
            } else if (pos != 0) {
                auto doc_ids_flip = doc_ids;
                doc_ids_flip.flip(0, index_config_.doc_count);
                response &= doc_ids | (doc_ids_flip & LeftDateRangeSearch(name, left_date, pos - 1));
                break;
            }
        }
        return response;
    }

    PostingList RightDateRangeSearch(const std::string& name, const std::optional<uint64_t>& right_date, int pos = 65) const {
        if (!right_date) {
            return {};
        }
        uint64_t right = *right_date;
        PostingList response = GetDocId(GetDateAttributeName(name, 64));
        for (; pos >= 0; --pos) {
            uint64_t upos = static_cast<uint64_t>(pos);
            bool rb = (right >> upos) % 2;
            if (!rb) {
                auto doc_ids = GetDocId(GetDateAttributeName(name, upos));
                doc_ids.flip(0, index_config_.doc_count);
                response &= doc_ids;
            } else if (upos != 0) {
                auto doc_ids = GetDocId(GetDateAttributeName(name, upos));
                auto doc_ids_flip = doc_ids;
                doc_ids_flip.flip(0, index_config_.doc_count);
                response &= doc_ids_flip | (doc_ids & RightDateRangeSearch(name, right_date, pos - 1));
                break;
            }
        }
        return response;
    }

    PostingList SimpleSearch(const std::string& request) const {
        auto main_forms = GetMainForms(request, russian_morpher_, loc_);
        if (main_forms.empty()) {
            auto response = PostingList();
            response.flip(0, index_config_.doc_count);
            return response;
        }
        PostingList response;
        for (const auto& token : main_forms) {
            auto special_token = AddSpecialSymbols(token);
            auto doc_ids = GetDocId(special_token);
            response |= doc_ids;
        }
        // std::cout << "simple search: " << request << '\n';
        // for (const auto& doc_id : response[0]) {
        //     std::cout << doc_id << ' ';
        // }
        // std::cout << '\n';
        return response;
    }

    PostingList PrefixSearch(const std::string& request) const {
        std::optional<IndexKey> start_key, end_key;
        std::string small_request = "^" + request;
        small_request = boost::locale::to_lower(small_request, loc_);
        small_request.pop_back();
        // std::cout << small_request << '\n';
        if (index_config_.use_word_dictionary) {
            lsm::DefaultValue left_value;
            auto left_key = word_dictionary_->LowerBound(lsm::UserKey(small_request.begin(), small_request.end()), left_value);
            if (left_key) {
                // std::cout << DeserializeUint32(left_value) << '\n';
                start_key = TokenId{0, DeserializeUint32(left_value)}.Serialize();
            }
        } else {
            start_key = TokenWithBlockId{0, small_request}.Serialize();
        }
        small_request.pop_back();
        small_request.pop_back();
        small_request += "я";
        // std::cout << small_request << '\n';
        if (index_config_.use_word_dictionary) {
            lsm::DefaultValue right_value;
            auto right_key = word_dictionary_->LowerBound(lsm::UserKey(small_request.begin(), small_request.end()), right_value);
            if (right_key) {
                // std::cout << DeserializeUint32(right_value) << '\n';
                end_key = TokenId{0, DeserializeUint32(right_value)}.Serialize();
            }
        } else {
            end_key = TokenWithBlockId{0, small_request}.Serialize();
        }
        return index_->Scan(start_key, end_key);
    }

    PostingList WildcardSearch(const std::string& request) const {
        if (!index_config_.use_k_gram) {
            throw "Search error: you use wildcard search, but index does not use k-grams";
        }
        PostingList response;
        response.flip(0, index_config_.doc_count);
        std::u32string in = boost::locale::conv::utf_to_utf<char32_t>(boost::locale::to_lower("^" + request + "$", loc_));
        std::u32string part;
        for (auto& sym : in) {
            if (sym == U'*' || sym == U'$') {
                if (sym == U'$') {
                    part.push_back(sym);
                }
                if (part.size() >= 3) {
                    std::string token = boost::locale::conv::utf_to_utf<char>(part);
                    auto doc_ids = GetDocId(token);
                    // std::cout << "wildcard search: " << token << '\n';
                    // for (const auto& doc_id : doc_ids[0]) {
                    //     std::cout << doc_id << ' ';
                    // }
                    // std::cout << '\n';
                    response &= doc_ids;
                }
                part.clear();
            } else {
                part.push_back(sym);
            }
        }
        return response;
    }

   private:
    uint32_t doc_block_count_;
    IndexConfig index_config_;
    std::locale loc_ = boost::locale::generator()("ru_RU.UTF-8");
    std::shared_ptr<const IInvertedIndex> index_;
    std::shared_ptr<const lsm::ILSM<lsm::DefaultValue>> word_dictionary_;
    std::shared_ptr<RussianMorpher> russian_morpher_;
};

}  // namespace

std::unique_ptr<ICrawler> MakeCrawler(const CrawlerConfig& config, const std::shared_ptr<RussianMorpher>& russian_morpher) { return std::make_unique<CrawlerImpl>(config, russian_morpher); }

std::unique_ptr<ISearchEngine> MakeSearchEngine(const IndexConfig& index_config, const std::shared_ptr<const IInvertedIndex>& index,
                                                const std::shared_ptr<const lsm::ILSM<lsm::DefaultValue>> word_dictionary, const std::shared_ptr<RussianMorpher>& russian_morpher) {
    return std::make_unique<SearchEngineImpl>(index_config, index, word_dictionary, russian_morpher);
}

}  // namespace invindex