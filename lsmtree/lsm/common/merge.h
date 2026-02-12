#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "lsm/common/stream.h"

namespace lsm {

// K-way merger that merges multiple sorted streams into one sorted stream
// using a min-heap (priority queue).
//
// Template parameters:
//   T: element type
//   Compare: comparison function (default: std::less<T> for ascending order)
//
// Algorithm:
//   1. Initialize heap with first element from each stream
//   2. Extract minimum element from heap
//   3. Advance the stream that produced the minimum
//   4. Insert next element from that stream into heap
//   5. Repeat until all streams are exhausted
//
// Complexity:
//   Time: O(N log K), where N = total elements, K = number of streams
//   Space: O(K) for the heap
//
// Example:
//   auto s1 = MakeStream({1, 3, 5});
//   auto s2 = MakeStream({2, 4, 6});
//   auto merger = MakeMerger<int>({s1, s2});
//   while (auto val = merger->Next()) {
//     // Process *val (produces: 1, 2, 3, 4, 5, 6)
//   }
template <typename T, typename Compare = std::less<T>>
class IMerger : public IStream<T> {
   public:
    // Returns next element in sorted order, or nullopt if all streams exhausted
    std::optional<T> Next() override = 0;

    virtual ~IMerger() = default;
};

namespace internal {

// Implementation of K-way merger
template <typename T, typename Compare>
class KWayMerger final : public IMerger<T, Compare> {
   public:
    explicit KWayMerger(std::vector<std::shared_ptr<IStream<T>>> sources, Compare comp) : sources_(std::move(sources)), comp_(comp) {
        for (size_t ind = 0; ind < sources_.size(); ++ind) {
            auto value = sources_[ind]->Next();
            if (value.has_value()) {
                heap_.insert({*value, ind});
            }
        }
    }

    std::optional<T> Next() override {
        if (heap_.empty()) {
            return std::nullopt;
        }
        auto result = *heap_.begin();
        heap_.erase(heap_.begin());
        auto new_value = sources_[result.second]->Next();
        if (new_value.has_value()) {
            heap_.insert({*new_value, result.second});
        }
        return result.first;
    }

   private:
    class BigCompare {
       public:
        bool operator()(const std::pair<T, size_t> f, const std::pair<T, size_t> s) const { return Compare()(f.first, s.first); }
    };

    std::multiset<std::pair<T, size_t>, BigCompare> heap_;
    std::vector<std::shared_ptr<IStream<T>>> sources_;
    Compare comp_;
};

}  // namespace internal

// Factory function to create a K-way merger
//
// Parameters:
//   sources: vector of sorted streams to merge (each must be sorted according to comp)
//   comp: comparison function for ordering (default: std::less<T>)
//
// Returns:
//   Shared pointer to merger stream that produces elements in sorted order
template <typename T, typename Compare = std::less<T>>
std::shared_ptr<IMerger<T, Compare>> MakeMerger(std::vector<std::shared_ptr<IStream<T>>> sources, Compare comp = Compare()) {
    return std::make_shared<internal::KWayMerger<T, Compare>>(std::move(sources), comp);
}

}  // namespace lsm
