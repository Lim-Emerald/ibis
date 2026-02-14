#include </home/lim/HSE/Projects/IBIS/contrib/gtest/gtest.h>
#include <lsm/sstable.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <random>
#include <vector>

#include "lsm/storage/buffer_pool.h"

namespace lsm {
namespace {

TEST(SSTable, Scan) {
    auto factory = MakeSSTableFileFactory();

    std::filesystem::create_directory("test");
    auto buffer_pool = storage::MakeReadBufferPool("test", 16348);
    auto file = std::make_shared<storage::BufferedMemoryFile>("test", 1, buffer_pool);

    UserKey a{'a'};
    UserKey b{'b'};
    Value v1{1};
    Value v2{2};
    Value v3{3};

    {
        // file must contain serialized sstable after this block
        auto builder = factory->NewFileBuilder(file);

        builder->Add(InternalKey{.user_key = a, .sequence_number = 5, .type = ValueType::kValue}, v3);
        builder->Add(InternalKey{.user_key = a, .sequence_number = 2, .type = ValueType::kDeletion}, {});
        builder->Add(InternalKey{.user_key = a, .sequence_number = 1, .type = ValueType::kValue}, v1);
        builder->Add(InternalKey{.user_key = b, .sequence_number = 6, .type = ValueType::kDeletion}, {});
        builder->Add(InternalKey{.user_key = b, .sequence_number = 4, .type = ValueType::kValue}, v2);
        builder->Finish();
    }

    auto sstable = factory->FromFile(file);

    auto it = sstable->MakeScan();
    std::vector<std::pair<InternalKey, Value>> all;
    while (true) {
        auto n = it->Next();
        if (!n.has_value()) {
            break;
        }
        all.push_back(*n);
    }

    std::vector<std::pair<InternalKey, Value>> expected{
        {InternalKey{.user_key = a, .sequence_number = 5, .type = ValueType::kValue}, v3}, {InternalKey{.user_key = a, .sequence_number = 2, .type = ValueType::kDeletion}, {}},
        {InternalKey{.user_key = a, .sequence_number = 1, .type = ValueType::kValue}, v1}, {InternalKey{.user_key = b, .sequence_number = 6, .type = ValueType::kDeletion}, {}},
        {InternalKey{.user_key = b, .sequence_number = 4, .type = ValueType::kValue}, v2},
    };

#if 1
    ASSERT_EQ(all.size(), expected.size());
    for (size_t i = 0; i < all.size(); ++i) {
        EXPECT_EQ(all[i], expected[i]) << "i = " << i;
    }
#else
    EXPECT_EQ(all, expected);
#endif
    std::filesystem::remove_all("test");
}

TEST(SSTable, Get) {
    auto factory = MakeSSTableFileFactory();

    std::filesystem::create_directory("test");
    auto buffer_pool = storage::MakeReadBufferPool("test", 16348);
    auto file = std::make_shared<storage::BufferedMemoryFile>("test", 1, buffer_pool);

    UserKey a{'a'};
    UserKey b{'b'};
    UserKey c{'c'};
    Value v1{1};
    Value v2{2};
    Value v3{3};

    {
        // file must contain serialized sstable after this block
        auto builder = factory->NewFileBuilder(file);

        builder->Add(InternalKey{.user_key = a, .sequence_number = 5, .type = ValueType::kValue}, v3);
        builder->Add(InternalKey{.user_key = a, .sequence_number = 2, .type = ValueType::kDeletion}, {});
        builder->Add(InternalKey{.user_key = a, .sequence_number = 1, .type = ValueType::kValue}, v1);
        builder->Add(InternalKey{.user_key = b, .sequence_number = 6, .type = ValueType::kDeletion}, {});
        builder->Add(InternalKey{.user_key = b, .sequence_number = 4, .type = ValueType::kValue}, v2);
        builder->Finish();
    }

    auto sstable = factory->FromFile(file);

    {
        Value value;
        ISSTableReader::GetKind result_type = sstable->Get(a, &value);

        EXPECT_EQ(result_type, ISSTableReader::GetKind::kFound);
        EXPECT_EQ(value, v3);
    }
    {
        Value value;
        ISSTableReader::GetKind result_type = sstable->Get(b, &value);

        EXPECT_EQ(result_type, ISSTableReader::GetKind::kDeletion);
    }
    {
        Value value;
        ISSTableReader::GetKind result_type = sstable->Get(c, &value);

        EXPECT_EQ(result_type, ISSTableReader::GetKind::kNotFound);
    }
    std::filesystem::remove_all("test");
}

class TrackingFile : public storage::IFile {
   public:
    explicit TrackingFile(std::shared_ptr<storage::IFile> inner) : inner_(std::move(inner)) {}

    std::vector<uint8_t> Read(uint64_t offset, uint64_t bytes) const override {
        reads.emplace_back(offset, bytes);
        return inner_->Read(offset, bytes);
    }

    void Write(const uint8_t* data, uint64_t size) override {
        writes.emplace_back(0, size);
        inner_->Write(data, size);
    }

    uint64_t Size() const override { return inner_->Size(); }

    mutable std::vector<std::pair<uint64_t, uint64_t>> reads;
    mutable std::vector<std::pair<uint64_t, uint64_t>> writes;

   private:
    std::shared_ptr<storage::IFile> inner_;
};

UserKey GenerateRandomKey(std::mt19937& rng, int min_len = 7, int max_len = 11) {
    std::uniform_int_distribution<int> len_dist(min_len, max_len);
    std::uniform_int_distribution<uint8_t> byte_dist(0, 255);

    int length = len_dist(rng);
    UserKey key;
    key.reserve(length);
    for (int i = 0; i < length; i++) {
        key.push_back(byte_dist(rng));
    }
    return key;
}

TEST(SSTable, ReaderUsesSmallReads) {
    std::filesystem::create_directory("test");
    auto buffer_pool = storage::MakeReadBufferPool("test", 16348);
    auto memory_file = std::make_shared<storage::BufferedMemoryFile>("test", 1, buffer_pool);
    auto tracking_file = std::make_shared<TrackingFile>(memory_file);

    auto ff = MakeSSTableFileFactory();

    std::vector<std::pair<UserKey, Value>> key_values;

    {
        std::mt19937 rng(42);
        const int num_keys = 100'000;
        std::set<UserKey> added_keys_set;

        auto builder = ff->NewFileBuilder(tracking_file);

        for (int i = 0; i < num_keys; i++) {
            UserKey key = GenerateRandomKey(rng);
            Value value = GenerateRandomKey(rng);
            while (added_keys_set.count(key) > 0) {
                key = GenerateRandomKey(rng);
            }
            added_keys_set.insert(key);
            key_values.emplace_back(std::move(key), std::move(value));
        }

        std::sort(key_values.begin(), key_values.end());

        for (int i = 0; i < num_keys; ++i) {
            const auto& [key, value] = key_values.at(i);
            builder->Add(InternalKey{.user_key = key, .sequence_number = static_cast<uint64_t>(i), .type = ValueType::kValue}, value);
        }

        builder->Finish();
    }

    tracking_file->reads.clear();

    auto reader = ff->FromFile(tracking_file);

    Value out;
    ASSERT_EQ(reader->Get(key_values.at(2101).first, &out), ISSTableReader::GetKind::kFound);
    EXPECT_EQ(out, key_values.at(2101).second);

    size_t total_read_bytes = 0;
    for (const auto& [_, len] : tracking_file->reads) {
        total_read_bytes += len;
    }
    EXPECT_LT(total_read_bytes, 1000);
    std::filesystem::remove_all("test");
}

TEST(SSTable, GetWithSequenceNumber) {
    auto factory = MakeSSTableFileFactory();

    std::filesystem::create_directory("test");
    auto buffer_pool = storage::MakeReadBufferPool("test", 16348);
    auto file = std::make_shared<storage::BufferedMemoryFile>("test", 1, buffer_pool);

    UserKey k{1, 2, 3};
    Value v1{10};
    Value v2{20};
    Value v3{30};
    Value out;

    {
        auto builder = factory->NewFileBuilder(file);

        builder->Add(InternalKey{.user_key = k, .sequence_number = 7, .type = ValueType::kValue}, v3);
        builder->Add(InternalKey{.user_key = k, .sequence_number = 5, .type = ValueType::kDeletion}, {});
        builder->Add(InternalKey{.user_key = k, .sequence_number = 3, .type = ValueType::kValue}, v2);
        builder->Add(InternalKey{.user_key = k, .sequence_number = 1, .type = ValueType::kValue}, v1);
        builder->Finish();
    }

    auto sstable = factory->FromFile(file);

    EXPECT_EQ(sstable->Get(k, &out, 0), ISSTableReader::GetKind::kNotFound);
    EXPECT_EQ(out, Value{});

    EXPECT_EQ(sstable->Get(k, &out, 1), ISSTableReader::GetKind::kFound);
    EXPECT_EQ(out, v1);

    EXPECT_EQ(sstable->Get(k, &out, 2), ISSTableReader::GetKind::kFound);
    EXPECT_EQ(out, v1);

    EXPECT_EQ(sstable->Get(k, &out, 3), ISSTableReader::GetKind::kFound);
    EXPECT_EQ(out, v2);

    EXPECT_EQ(sstable->Get(k, &out, 4), ISSTableReader::GetKind::kFound);
    EXPECT_EQ(out, v2);

    EXPECT_EQ(sstable->Get(k, &out, 5), ISSTableReader::GetKind::kDeletion);
    EXPECT_EQ(out, Value{});

    EXPECT_EQ(sstable->Get(k, &out, 6), ISSTableReader::GetKind::kDeletion);
    EXPECT_EQ(out, Value{});

    EXPECT_EQ(sstable->Get(k, &out, 7), ISSTableReader::GetKind::kFound);
    EXPECT_EQ(out, v3);

    EXPECT_EQ(sstable->Get(k, &out), ISSTableReader::GetKind::kFound);
    EXPECT_EQ(out, v3);

    std::filesystem::remove_all("test");
}

}  // namespace
}  // namespace lsm
