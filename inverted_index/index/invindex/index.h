#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "index/common/morpher.h"
#include "index/common/types.h"
#include "index/lsm/lsm.h"

namespace invindex {

struct IndexConfig {
    bool use_word_dictionary = true;
    uint8_t doc_blocks_count = 0;
    uint8_t use_k_gram = 0;
    uint32_t doc_block_size = 1024;
    std::string index_dir = "index";
    lsm::GranularLsmOptions lsm_options;
};

class IInvertedIndex {
   public:
    virtual PostingList GetDocId(IndexKey index_key) const = 0;

    // [start_token_id, end_token_id)
    virtual std::vector<PostingList> Scan(const std::optional<IndexKey>& start_key, const std::optional<IndexKey>& end_key) const = 0;

    virtual ~IInvertedIndex() = default;
};

class IInvertedIndexBuilder {
   public:
    virtual void Put(IndexKey index_key, PostingList posting_list) = 0;

    virtual std::shared_ptr<const IInvertedIndex> GetInvertedIndex() const = 0;

    virtual ~IInvertedIndexBuilder() = default;
};

struct CrawlerConfig {
    IndexConfig index_config;
    std::string work_dir;
    std::string docbase_dir;
};

class ICrawler {
   public:
    virtual void Process() = 0;

    virtual std::vector<std::string> GetDocList() const = 0;

    virtual std::shared_ptr<lsm::ILSM<lsm::DefaultValue>> GetWordDictionary() const = 0;

    virtual std::shared_ptr<const IInvertedIndex> GetInvertedIndex() const = 0;

    virtual ~ICrawler() = default;
};

class ISearchEngine {
   public:
    virtual PostingList Search(const std::string& query) const = 0;

    virtual ~ISearchEngine() = default;
};

std::unique_ptr<ICrawler> MakeCrawler(const CrawlerConfig& config, const std::shared_ptr<RussianMorpher>& russian_morpher);

std::unique_ptr<ISearchEngine> MakeSearchEngine(const IndexConfig& index_config, const std::shared_ptr<const IInvertedIndex>& index,
                                                const std::shared_ptr<const lsm::ILSM<lsm::DefaultValue>> word_dictionary, const std::shared_ptr<RussianMorpher>& russian_morpher);

}  // namespace invindex