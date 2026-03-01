#include "index/invindex/index.h"

#include <strutext/morpho/models/rus_model.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/locale.hpp>
#include <boost/regex.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace invindex {

using RussianPos = strutext::morpho::RussianPos;
using RussianPosSerializer = strutext::morpho::russian::PosSerializer;

class InvertedIndexImpl : public IInvertedIndex {
   public:
    InvertedIndexImpl(const std::string& index_dir, const lsm::GranularLsmOptions& options) : index_dir_(index_dir) {
        std::filesystem::create_directory(index_dir_);
        lsm_ = lsm::MakeLeveledLsm(index_dir_ + "/lsm", options, lsm::MakeLevelsProvider(), lsm::MakeSSTableFileFactory());
    }

    void Put(const Token& token, const PostingList& posting_list) { lsm_->Put(token, posting_list); }

    PostingList GetDocId(const Token& token) const override {
        auto posting_list = lsm_->Get(token);
        if (posting_list) {
            return *posting_list;
        } else {
            return {};
        }
    }

    virtual ~InvertedIndexImpl() { std::filesystem::remove_all(index_dir_); }

   private:
    std::string index_dir_;
    std::unique_ptr<lsm::ILSM> lsm_;
};

class InvertedIndexBuilderImpl : public IInvertedIndexBuilder {
   public:
    InvertedIndexBuilderImpl(const std::string& index_dir, const lsm::GranularLsmOptions& options) { index_ = std::make_shared<InvertedIndexImpl>(index_dir, options); }

    void Put(const Token& token, const PostingList& posting_list) override { index_->Put(token, posting_list); }

    std::shared_ptr<const IInvertedIndex> GetInvertedIndex() const override { return index_; }

    virtual ~InvertedIndexBuilderImpl() = default;

   private:
    std::shared_ptr<InvertedIndexImpl> index_;
};

std::shared_ptr<IInvertedIndexBuilder> MakeInvertedIndexBuilder(const std::string& index_dir, const lsm::GranularLsmOptions& options) {
    return std::make_shared<InvertedIndexBuilderImpl>(index_dir, options);
}

std::string FilterText(const std::string& text) {
    auto is_rus = [](char32_t ch) {
        return (ch >= U'А' && ch <= U'Я') || (ch >= U'а' && ch <= U'я') || ch == U'Ё' || ch == U'ё';
    };
    std::u32string in = boost::locale::conv::utf_to_utf<char32_t>(text), out;
    for (char32_t ch : in) {
        if (is_rus(ch) || (ch >= U'0' && ch <= U'9') || ch == U' ') {
            out.push_back(ch);
        }
    }
    return boost::locale::conv::utf_to_utf<char>(out);
}

std::unordered_set<std::string> GetMainForms(const std::string& word, const std::shared_ptr<RussianMorpher>& russian_morpher, std::locale loc) {
    strutext::morpho::MorphologistBase::LemList lemm_list;
    russian_morpher->Analize(boost::locale::to_lower(word, loc), lemm_list);
    if (lemm_list.empty()) {
        return {boost::locale::to_lower(word, loc)};
    }
    bool skip = true;
    std::unordered_set<std::string> main_forms;
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
        main_forms.insert(main_form);
    }
    return main_forms;
}

class CrawlerImpl : public ICrawler {
   public:
    CrawlerImpl(const std::string& docbase_dir, const std::shared_ptr<IInvertedIndexBuilder>& index_builder, const std::shared_ptr<RussianMorpher>& russian_morpher)
        : docbase_dir_(docbase_dir), index_builder_(index_builder), russian_morpher_(russian_morpher) {
        boost::locale::generator gen;
        loc_ = gen("ru_RU.UTF-8");
    }

    void Process() override {
        for (const std::filesystem::directory_entry& e : std::filesystem::recursive_directory_iterator(docbase_dir_, std::filesystem::directory_options::skip_permission_denied)) {
            if (e.is_regular_file()) {
                ProcessDoc(e.path().string());
            }
        }
    }

    std::vector<std::string> GetDocList() const override { return docs_; }

    virtual ~CrawlerImpl() = default;

   private:
    void ProcessDoc(const std::string& path) {
        std::cout << "crawler process doc: " << path << '\n';
        std::ifstream file(path);

        uint64_t doc_id = docs_.size();
        docs_.push_back(path);

        std::string line;
        while (std::getline(file, line)) {
            line = FilterText(line);
            boost::algorithm::trim(line, loc_);
            if (!line.empty()) {
                std::istringstream iss(line);
                std::string word;
                while (iss >> word) {
                    for (const auto& token : GetMainForms(word, russian_morpher_, loc_)) {
                        index_builder_->Put(token, PostingList({doc_id}));
                    }
                }
            }
        }
    }

   private:
    std::locale loc_;
    std::string docbase_dir_;
    std::vector<std::string> docs_;
    std::shared_ptr<IInvertedIndexBuilder> index_builder_;
    std::shared_ptr<RussianMorpher> russian_morpher_;
};

std::unique_ptr<ICrawler> MakeCrawler(const std::string& docbase_dir, const std::shared_ptr<IInvertedIndexBuilder>& index_builder, const std::shared_ptr<RussianMorpher>& russian_morpher) {
    return std::make_unique<CrawlerImpl>(docbase_dir, index_builder, russian_morpher);
}

class SearchEngineImpl : public ISearchEngine {
   public:
    SearchEngineImpl(const std::shared_ptr<const IInvertedIndex>& index, const std::shared_ptr<RussianMorpher>& russian_morpher) : index_(index), russian_morpher_(russian_morpher) {
        boost::locale::generator gen;
        loc_ = gen("ru_RU.UTF-8");
    }

    PostingList Search(const std::string& query) const override {
        std::string line = query;
        std::optional<PostingList> response;
        line = FilterText(line);
        boost::algorithm::trim(line, loc_);
        if (!line.empty()) {
            std::istringstream iss(line);
            std::string word;
            while (iss >> word) {
                PostingList doc_ids;
                auto main_forms = GetMainForms(word, russian_morpher_, loc_);
                if (main_forms.empty()) {
                    continue;
                }
                for (const auto& token : main_forms) {
                    doc_ids |= index_->GetDocId(token);
                }
                // std::cout << "search: " << word << '\n';
                // for (const auto& doc_id : doc_ids) {
                //     std::cout << doc_id << ' ';
                // }
                // std::cout << '\n';
                if (response) {
                    response = *response & doc_ids;
                } else {
                    response = doc_ids;
                }
            }
        }
        if (response) {
            return *response;
        } else {
            return {};
        }
    }

   private:
    std::locale loc_;
    std::shared_ptr<const IInvertedIndex> index_;
    std::shared_ptr<RussianMorpher> russian_morpher_;
};

std::unique_ptr<ISearchEngine> MakeSearchEngine(const std::shared_ptr<const IInvertedIndex>& index, const std::shared_ptr<RussianMorpher>& russian_morpher) {
    return std::make_unique<SearchEngineImpl>(index, russian_morpher);
}

}  // namespace invindex