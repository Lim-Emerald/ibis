#pragma once

#include "index/common/morpher.h"

#include <index/common/types.h>
#include <index/lsm/lsm.h>

#include <memory>
#include <string>
#include <vector>

namespace invindex {

class IInvertedIndex {
   public:
    virtual PostingList GetDocId(const Token& token) const = 0;

    virtual ~IInvertedIndex() = default;
};

class IInvertedIndexBuilder {
   public:
    virtual void Put(const Token& token, const PostingList& posting_list) = 0;

    virtual std::shared_ptr<const IInvertedIndex> GetInvertedIndex() const = 0;

    virtual ~IInvertedIndexBuilder() = default;
};

std::shared_ptr<IInvertedIndexBuilder> MakeInvertedIndexBuilder(const std::string& index_dir, const lsm::GranularLsmOptions& options);

class ICrawler {
   public:
    virtual void Process() = 0;

    virtual std::vector<std::string> GetDocList() const = 0;

    virtual ~ICrawler() = default;
};

std::unique_ptr<ICrawler> MakeCrawler(const std::string& docbase_dir, const std::shared_ptr<IInvertedIndexBuilder>& index_builder, const std::shared_ptr<RussianMorpher>& russian_morpher);

class ISearchEngine {
   public:
    virtual PostingList Search(const std::string& query) const = 0;

    virtual ~ISearchEngine() = default;
};

std::unique_ptr<ISearchEngine> MakeSearchEngine(const std::shared_ptr<const IInvertedIndex>& index, const std::shared_ptr<RussianMorpher>& russian_morpher);

}  // namespace invindex