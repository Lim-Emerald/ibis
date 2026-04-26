#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "index/common/morpher.h"
#include "index/common/types.h"
#include "index/lsm/lsm.h"

namespace invindex {

enum IndexType {
    IT_POSITIONAL,
    IT_RANKED_TFIDF,
    IT_RANKED_BM25,
};

struct CommonConfig {
    IndexType index_type;
    uint16_t doc_block_size = 1024;
};

struct IndexConfig {
    std::string index_dir = "index";
    lsm::GranularLsmOptions lsm_options;
};

template<typename IndexKey, typename IndexValue>
class IInvertedIndex {
   public:
    virtual std::optional<IndexValue> GetDocId(IndexKey index_key) const = 0;

    virtual std::shared_ptr<IStream<std::pair<IndexKey, IndexValue>>> Scan() const = 0;

    virtual ~IInvertedIndex() = default;
};

class IIndexState {
public:
    virtual IndexType GetIndexType() const = 0;

    virtual void StartDoc() = 0;

    virtual void AddToken(const Token& token) = 0;

    virtual void FinishDoc() = 0;

    virtual void Finish() = 0;

    virtual std::vector<uint64_t> Search(const std::string& query) const = 0;
};

struct IndexerConfig : public CommonConfig {
    std::string work_dir;
    std::string docbase_dir;
    lsm::GranularLsmOptions lsm_options;
};

class IIndexer {
   public:
    virtual void Process() = 0;

    virtual std::vector<std::string> GetDocList() const = 0;

    virtual std::shared_ptr<const IIndexState> GetIndexState() const = 0;

    virtual ~IIndexer() = default;
};

std::unique_ptr<IIndexer> MakeIndexer(const IndexerConfig& config, const std::shared_ptr<RussianMorpher>& russian_morpher);

struct SearchEngineConfig : public CommonConfig {
};

}  // namespace invindex