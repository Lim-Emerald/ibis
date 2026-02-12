#include </home/lim/HSE/Projects/IBIS/contrib/gtest/gtest.h>
#include <lsm/common/merge.h>
#include <lsm/common/stream.h>

#include <memory>
#include <optional>
#include <vector>

namespace lsm {
namespace {

// Simple stream implementation backed by a vector
template <typename T>
class VectorStream : public IStream<T> {
   public:
    explicit VectorStream(std::vector<T> data) : data_(std::move(data)), index_(0) {}

    std::optional<T> Next() override {
        ++calls_count_;
        if (index_ >= data_.size()) {
            return std::nullopt;
        }
        return data_[index_++];
    }

    uint64_t CallsCount() const { return calls_count_; }

   private:
    uint64_t calls_count_ = 0;

    std::vector<T> data_;
    size_t index_;
};

// Helper function to create a stream from a vector
template <typename T>
std::shared_ptr<VectorStream<T>> MakeVectorStream(std::vector<T> data) {
    return std::make_shared<VectorStream<T>>(std::move(data));
}

// Helper to collect all elements from a stream into a vector
template <typename T>
std::vector<T> CollectAll(IStream<T>& stream) {
    std::vector<T> result;
    while (auto val = stream.Next()) {
        result.push_back(*val);
    }
    return result;
}

template <typename T>
std::vector<T> CollectWithLimit(IStream<T>& stream, uint64_t limit) {
    std::vector<T> result;
    while (limit > 0) {
        auto val = stream.Next();
        if (!val) {
            break;
        }
        result.push_back(*val);
        --limit;
    }
    return result;
}

TEST(KWayMerge, EmptyStreams) {
    std::vector<std::shared_ptr<IStream<int>>> empty;
    auto merger = MakeMerger<int>(empty);

    EXPECT_FALSE(merger->Next().has_value());
}

TEST(KWayMerge, SingleStream) {
    auto s1 = MakeVectorStream<int>({1, 2, 3, 4, 5});
    auto merger = MakeMerger<int>({s1});

    std::vector<int> result = CollectAll(*merger);
    std::vector<int> expected = {1, 2, 3, 4, 5};

    EXPECT_EQ(result, expected);
}

TEST(KWayMerge, SingleEmptyStream) {
    auto s1 = MakeVectorStream<int>({});
    auto merger = MakeMerger<int>({s1});

    EXPECT_FALSE(merger->Next().has_value());
}

TEST(KWayMerge, TwoStreams) {
    auto s1 = MakeVectorStream<int>({1, 3, 5, 7});
    auto s2 = MakeVectorStream<int>({2, 4, 6, 8});
    auto merger = MakeMerger<int>({s1, s2});

    std::vector<int> result = CollectAll(*merger);
    std::vector<int> expected = {1, 2, 3, 4, 5, 6, 7, 8};

    EXPECT_EQ(result, expected);
}

TEST(KWayMerge, ThreeStreams) {
    auto s1 = MakeVectorStream<int>({1, 4, 7});
    auto s2 = MakeVectorStream<int>({2, 5, 8});
    auto s3 = MakeVectorStream<int>({3, 6, 9});
    auto merger = MakeMerger<int>({s1, s2, s3});

    std::vector<int> result = CollectAll(*merger);
    std::vector<int> expected = {1, 2, 3, 4, 5, 6, 7, 8, 9};

    EXPECT_EQ(result, expected);
}

TEST(KWayMerge, ManyStreams) {
    std::vector<std::shared_ptr<IStream<int>>> streams;

    // Create 10 streams: [0, 10, 20], [1, 11, 21], ..., [9, 19, 29]
    for (int i = 0; i < 10; ++i) {
        streams.push_back(MakeVectorStream<int>({i, i + 10, i + 20}));
    }

    auto merger = MakeMerger<int>(streams);
    std::vector<int> result = CollectAll(*merger);

    // Expected: 0, 1, 2, ..., 9, 10, 11, ..., 19, 20, 21, ..., 29
    std::vector<int> expected;
    for (int i = 0; i < 30; ++i) {
        expected.push_back(i);
    }

    EXPECT_EQ(result, expected);
}

TEST(KWayMerge, AllEmptyStreams) {
    auto s1 = MakeVectorStream<int>({});
    auto s2 = MakeVectorStream<int>({});
    auto s3 = MakeVectorStream<int>({});
    auto merger = MakeMerger<int>({s1, s2, s3});

    EXPECT_FALSE(merger->Next().has_value());
}

TEST(KWayMerge, SomeEmptyStreams) {
    auto s1 = MakeVectorStream<int>({});
    auto s2 = MakeVectorStream<int>({1, 2, 3});
    auto s3 = MakeVectorStream<int>({});
    auto s4 = MakeVectorStream<int>({4, 5, 6});
    auto merger = MakeMerger<int>({s1, s2, s3, s4});

    std::vector<int> result = CollectAll(*merger);
    std::vector<int> expected = {1, 2, 3, 4, 5, 6};

    EXPECT_EQ(result, expected);
}

TEST(KWayMerge, NonOverlappingRanges) {
    auto s1 = MakeVectorStream<int>({1, 2, 3});
    auto s2 = MakeVectorStream<int>({10, 20, 30});
    auto s3 = MakeVectorStream<int>({100, 200});
    auto merger = MakeMerger<int>({s1, s2, s3});

    std::vector<int> result = CollectAll(*merger);
    std::vector<int> expected = {1, 2, 3, 10, 20, 30, 100, 200};

    EXPECT_EQ(result, expected);
}

TEST(KWayMerge, EqualValues) {
    auto s1 = MakeVectorStream<int>({1, 3, 5, 7});
    auto s2 = MakeVectorStream<int>({1, 3, 6, 8});
    auto merger = MakeMerger<int>({s1, s2});

    std::vector<int> result = CollectAll(*merger);

    std::vector<int> expected = {1, 1, 3, 3, 5, 6, 7, 8};
    EXPECT_EQ(result, expected);
}

TEST(KWayMerge, ReverseOrder) {
    auto s1 = MakeVectorStream<int>({9, 7, 5, 3, 1});
    auto s2 = MakeVectorStream<int>({10, 8, 6, 4, 2});
    auto merger = MakeMerger<int, std::greater<int>>({s1, s2}, std::greater<int>());

    std::vector<int> result = CollectAll(*merger);

    std::vector<int> expected = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
    EXPECT_EQ(result, expected);
}

TEST(KWayMerge, LargeStreams) {
    const int num_streams = 10;
    const int elements_per_stream = 1000;

    std::vector<std::shared_ptr<IStream<int>>> streams;

    for (int i = 0; i < num_streams; ++i) {
        std::vector<int> data;
        for (int j = 0; j < elements_per_stream; ++j) {
            data.push_back(i + j * num_streams);
        }
        streams.push_back(MakeVectorStream<int>(std::move(data)));
    }

    auto merger = MakeMerger<int>(streams);
    std::vector<int> result = CollectAll(*merger);

    EXPECT_EQ(result.size(), num_streams * elements_per_stream);

    for (size_t i = 1; i < result.size(); ++i) {
        EXPECT_LE(result[i - 1], result[i]);
    }
}

TEST(KWayMerge, WithLimit) {
    auto s1 = MakeVectorStream<int>({1, 2, 3});
    auto s2 = MakeVectorStream<int>({10, 20, 30, 40, 50, 60, 70, 80, 90});
    auto s3 = MakeVectorStream<int>({100, 200});
    auto merger = MakeMerger<int>({s1, s2, s3});

    std::vector<int> result = CollectWithLimit(*merger, 5);
    std::vector<int> expected = {1, 2, 3, 10, 20};

    EXPECT_EQ(result, expected);

    EXPECT_EQ(s1->CallsCount(), 4);
    EXPECT_LE(s2->CallsCount(), 3);
    EXPECT_LE(s3->CallsCount(), 1);
}

}  // namespace
}  // namespace lsm
