#include "index/invindex/index.h"

#include <index/common/utils.h>
#include <strutext/morpho/models/rus_model.h>

#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/locale.hpp>
#include <boost/regex.hpp>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
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

    std::vector<PostingList> Scan(const std::optional<IndexKey>& start_key, const std::optional<IndexKey>& end_key) const override {
        auto scan = lsm_->Scan(start_key, end_key);
        auto object = scan->Next();
        std::vector<PostingList> result(config_.doc_blocks_count);
        while (object.has_value()) {
            result[object->first.back()] |= object->second;
            object = scan->Next();
        }
        return result;
    }

    virtual ~InvertedIndexImpl() { std::filesystem::remove_all(config_.index_dir); }

   private:
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

std::string FilterText(const std::string& text) {
    auto is_rus = [](char32_t ch) { return (ch >= U'А' && ch <= U'Я') || (ch >= U'а' && ch <= U'я') || ch == U'Ё' || ch == U'ё'; };
    std::u32string in = boost::locale::conv::utf_to_utf<char32_t>(text), out;
    for (const auto& ch : in) {
        if (is_rus(ch) || (ch >= U'0' && ch <= U'9') || ch == U' ' || ch == U'\x00a0') {
            out.push_back(ch);
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
                    line = FilterText(line);
                    boost::algorithm::trim(line, loc_);
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
        config_.index_config.doc_blocks_count = (docs_.size() - 1) / config_.index_config.doc_block_size + 1;
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
        : index_config_(index_config), index_(index), word_dictionary_(word_dictionary), russian_morpher_(russian_morpher) {}

    PostingList Search(const std::string& query) const override {
        auto first_wildcard = query.find('*');
        if (first_wildcard == query.size()) {
            return SimpleSearch(query);
        } else if (first_wildcard + 1 == query.size()) {
            return PrefixSearch(query);
        } else {
            return WildcardSearch(query);
        }
    }

   private:
    PostingList SimpleSearch(const std::string& query) const {
        std::string line = query;
        std::vector<std::optional<PostingList>> response(index_config_.doc_blocks_count, std::nullopt);
        line = FilterText(line);
        boost::algorithm::trim(line, loc_);
        if (!line.empty()) {
            std::istringstream iss(line);
            std::string word;
            while (iss >> word) {
                auto main_forms = GetMainForms(word, russian_morpher_, loc_);
                if (main_forms.empty()) {
                    continue;
                }
                std::vector<PostingList> doc_ids(index_config_.doc_blocks_count);
                for (const auto& token : main_forms) {
                    auto special_token = AddSpecialSymbols(token);
                    if (index_config_.use_word_dictionary) {
                        auto s_feature_id = word_dictionary_->Get(lsm::UserKey(special_token.begin(), special_token.end()));
                        if (s_feature_id) {
                            for (uint32_t block_id = 0; block_id < index_config_.doc_blocks_count; ++block_id) {
                                auto doc_block_ids = index_->GetDocId(TokenId{block_id, DeserializeUint32(*s_feature_id)}.Serialize());
                                doc_ids[block_id] |= doc_block_ids;
                            }
                        }
                    } else {
                        for (uint32_t block_id = 0; block_id < index_config_.doc_blocks_count; ++block_id) {
                            auto doc_block_ids = index_->GetDocId(TokenWithBlockId{block_id, special_token}.Serialize());
                            doc_ids[block_id] |= doc_block_ids;
                        }
                    }
                }
                // std::cout << "search: " << word << '\n';
                // for (const auto& doc_id : doc_ids[0]) {
                //     std::cout << doc_id << ' ';
                // }
                // std::cout << '\n';
                for (uint32_t block_id = 0; block_id < index_config_.doc_blocks_count; ++block_id) {
                    if (response[block_id]) {
                        response[block_id] = *response[block_id] & doc_ids[block_id];
                    } else {
                        response[block_id] = doc_ids[block_id];
                    }
                }
            }
        }
        PostingList whole_response;
        for (uint64_t block_id = 0; block_id < index_config_.doc_blocks_count; ++block_id) {
            if (!response[block_id]) {
                continue;
            }
            for (const auto& doc_block_id : *response[block_id]) {
                whole_response.add(block_id * index_config_.doc_blocks_count + doc_block_id);
            }
        }
        return whole_response;
    }

    PostingList PrefixSearch(const std::string& query) const {
        std::optional<IndexKey> start_key, end_key;
        std::vector<PostingList> doc_ids(index_config_.doc_blocks_count);
        std::string request = "^" + query;
        request = boost::locale::to_lower(request, loc_);
        request.pop_back();
        if (index_config_.use_word_dictionary) {
            lsm::DefaultValue left_value;
            auto left_key = word_dictionary_->LowerBound(lsm::UserKey(request.begin(), request.end()), left_value);
            std::cout << request << '\n';
            if (left_key) {
                std::cout << DeserializeUint32(left_value) << '\n';
                start_key = TokenId{0, DeserializeUint32(left_value)}.Serialize();
            }
        } else {
            start_key = TokenWithBlockId{0, request}.Serialize();
        }
        request.pop_back();
        request.pop_back();
        request += "я";
        if (index_config_.use_word_dictionary) {
            lsm::DefaultValue right_value;
            auto right_key = word_dictionary_->LowerBound(lsm::UserKey(request.begin(), request.end()), right_value);
            std::cout << request << '\n';
            if (right_key) {
                std::cout << DeserializeUint32(right_value) << '\n';
                end_key = TokenId{0, DeserializeUint32(right_value)}.Serialize();
            }
        } else {
            end_key = TokenWithBlockId{0, request}.Serialize();
        }
        auto result = index_->Scan(start_key, end_key);
        PostingList whole_result;
        for (uint64_t block_id = 0; block_id < index_config_.doc_blocks_count; ++block_id) {
            for (const auto& doc_block_id : result[block_id]) {
                whole_result.add(block_id * index_config_.doc_blocks_count + doc_block_id);
            }
        }
        return whole_result;
    }

    PostingList WildcardSearch(const std::string& query) const {
        std::vector<std::optional<PostingList>> response(index_config_.doc_blocks_count, std::nullopt);
        std::string request = "^" + query + "$";
        request = boost::locale::to_lower(request, loc_);
        std::u32string in = boost::locale::conv::utf_to_utf<char32_t>(request);
        std::u32string part;
        for (auto& sym : in) {
            if (sym == U'*' || sym == U'$') {
                if (sym == U'$') {
                    part.push_back(sym);
                }
                if (part.size() >= 3) {
                    std::string token = boost::locale::conv::utf_to_utf<char>(part);
                    std::vector<PostingList> doc_ids(index_config_.doc_blocks_count);
                    if (index_config_.use_word_dictionary) {
                        auto s_feature_id = word_dictionary_->Get(lsm::UserKey(token.begin(), token.end()));
                        if (s_feature_id) {
                            for (uint32_t block_id = 0; block_id < index_config_.doc_blocks_count; ++block_id) {
                                auto doc_block_ids = index_->GetDocId(TokenId{block_id, DeserializeUint32(*s_feature_id)}.Serialize());
                                doc_ids[block_id] |= doc_block_ids;
                            }
                        }
                    } else {
                        for (uint32_t block_id = 0; block_id < index_config_.doc_blocks_count; ++block_id) {
                            auto doc_block_ids = index_->GetDocId(TokenWithBlockId{block_id, token}.Serialize());
                            doc_ids[block_id] |= doc_block_ids;
                        }
                    }
                    std::cout << "search: " << token << '\n';
                    for (const auto& doc_id : doc_ids[0]) {
                        std::cout << doc_id << ' ';
                    }
                    std::cout << '\n';
                    for (uint32_t block_id = 0; block_id < index_config_.doc_blocks_count; ++block_id) {
                        if (response[block_id]) {
                            response[block_id] = *response[block_id] & doc_ids[block_id];
                        } else {
                            response[block_id] = doc_ids[block_id];
                        }
                    }
                }
                part.clear();
            } else {
                part.push_back(sym);
            }
        }
        PostingList whole_response;
        for (uint64_t block_id = 0; block_id < index_config_.doc_blocks_count; ++block_id) {
            if (!response[block_id]) {
                continue;
            }
            for (const auto& doc_block_id : *response[block_id]) {
                whole_response.add(block_id * index_config_.doc_blocks_count + doc_block_id);
            }
        }
        return whole_response;
    }

   private:
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