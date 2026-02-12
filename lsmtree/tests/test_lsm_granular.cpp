#include </home/lim/HSE/Projects/IBIS/contrib/gtest/gtest.h>
#include <lsm/common/types.h>
#include <lsm/lsm.h>
#include <lsm/sstable.h>
#include <lsm/utils/lsm_utils.h>

#include <map>
#include <random>
#include <vector>

namespace lsm {
namespace {

TEST(LSMGranular, PutGet) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(GranularLsmOptions{}, files_provider, sstable_factory);

    UserKey k1{'a'};
    Value v1{1};

    EXPECT_EQ(lsm->Get(k1), std::nullopt);

    lsm->Put(k1, v1);
    EXPECT_EQ(lsm->Get(k1), v1);
}

TEST(LSMGranular, Delete) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(GranularLsmOptions{}, files_provider, sstable_factory);

    UserKey k1{'a'};
    Value v1{1};

    lsm->Put(k1, v1);
    EXPECT_EQ(lsm->Get(k1), v1);

    lsm->Delete(k1);
    EXPECT_EQ(lsm->Get(k1), std::nullopt);

    lsm->Put(k1, v1);
    EXPECT_EQ(lsm->Get(k1), v1);
}

TEST(LSMGranular, MultipleFlushesLatestWins) {
    GranularLsmOptions options;
    options.memtable_bytes = 1'000;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(options, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 1'000;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng));
        values.push_back(GenerateRandomKey(rng));
    }

    std::map<UserKey, Value> expected_state;

    const int operations = 5'000;
    for (int i = 0; i < operations; ++i) {
        int operation = rng() % 10;
        UserKey key = keys[rng() % keys.size()];
        if (operation <= 7) {
            Value value = values[rng() % values.size()];
            lsm->Put(key, value);
            expected_state[key] = value;
        } else if (operation == 8) {
            lsm->Delete(key);
            expected_state.erase(key);
        } else {
            std::optional<Value> result = lsm->Get(key);
            if (expected_state.count(key) > 0) {
                ASSERT_EQ(result, expected_state[key]);
            } else {
                ASSERT_EQ(result, std::nullopt);
            }
        }
    }
}

TEST(LSMGranular, WriteAmplificationBounded) {
    GranularLsmOptions options;
    options.memtable_bytes = 1024;
    options.max_sstable_size = 4096;
    options.bloom_filter_size = 1024;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(options, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 2'000;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, 5, 7));
        values.push_back(GenerateRandomKey(rng, 10, 20));
    }

    uint64_t bytes_written_in_ideal_world = 0;

    const int operations = 6'000;
    for (int i = 0; i < operations; ++i) {
        UserKey key = keys[rng() % keys.size()];
        Value value = values[rng() % values.size()];
        lsm->Put(key, value);

        const uint32_t internal_key_size = sizeof(InternalKey::sequence_number) + sizeof(InternalKey::type) + sizeof(uint32_t) + key.size();
        const uint32_t value_size = sizeof(uint32_t) + value.size();

        bytes_written_in_ideal_world += internal_key_size + value_size;
    }

    uint64_t bytes_written_in_real_world = files_provider->TotalBytesInserted();
    std::cerr << "bytes written = " << bytes_written_in_real_world << std::endl;

    double write_amplification = static_cast<double>(bytes_written_in_real_world) / bytes_written_in_ideal_world;
    std::cerr << "write amplification = " << write_amplification << std::endl;
    EXPECT_LT(write_amplification, 8 * log2(operations));

    uint64_t bytes_read_in_read_world = files_provider->TotalBytesRead();
    EXPECT_LT(bytes_read_in_read_world, bytes_written_in_real_world);
}

TEST(LSMGranular, SearchComplexityByKeyAge) {
    GranularLsmOptions options;
    options.memtable_bytes = 128;
    options.max_sstable_size = 512;
    options.bloom_filter_size = 128;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(options, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 2'000;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, 5, 7));
        values.push_back(GenerateRandomKey(rng, 10, 20));
    }

    for (int i = 0; i < k_keys_count; ++i) {
        lsm->Put(keys[i], values[i]);
    }

    struct TestInfo {
        UserKey key;
        uint64_t reads_lower_bound;
        uint64_t reads_upper_bound;
    };

    std::vector<TestInfo> test_cases = {
        TestInfo{.key = keys[k_keys_count - 1], .reads_lower_bound = 0u, .reads_upper_bound = 1u},  // in memtable or in first level
        TestInfo{.key = keys[k_keys_count - 256], .reads_lower_bound = 1, .reads_upper_bound = 6}, TestInfo{.key = keys[0], .reads_lower_bound = 2, .reads_upper_bound = 10}  // oldest key
    };

    for (auto [key, min_expected_reads, max_expected_reads] : test_cases) {
        files_provider->ResetVisitCounters();

        auto result = lsm->Get(key);

        ASSERT_TRUE(result.has_value());

        uint64_t actual_visits = files_provider->TotalVisits();

        EXPECT_LE(min_expected_reads, actual_visits);
        EXPECT_LE(actual_visits, max_expected_reads);
    }
}

TEST(LSMGranular, GetWithSequenceNumber) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(GranularLsmOptions{.memtable_bytes = 100}, files_provider, sstable_factory);

    UserKey k{1, 2, 3};
    Value v1{10};
    Value v2{20};
    Value v3{30};

    ASSERT_EQ(lsm->GetCurrentSequenceNumber(), 0);

    lsm->Put(k, v1);
    ASSERT_EQ(lsm->GetCurrentSequenceNumber(), 1);

    lsm->Put(k, v2);
    ASSERT_EQ(lsm->GetCurrentSequenceNumber(), 2);

    lsm->Delete(k);
    ASSERT_EQ(lsm->GetCurrentSequenceNumber(), 3);

    lsm->Put(k, v3);
    ASSERT_EQ(lsm->GetCurrentSequenceNumber(), 4);

    EXPECT_EQ(lsm->Get(k, 0), std::nullopt);
    EXPECT_EQ(lsm->Get(k, 1), v1);
    EXPECT_EQ(lsm->Get(k, 2), v2);
    EXPECT_EQ(lsm->Get(k, 3), std::nullopt);
    EXPECT_EQ(lsm->Get(k, 4), v3);
    EXPECT_EQ(lsm->Get(k), v3);
}

TEST(LSMGranular, ScanMultipleKeys) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(GranularLsmOptions{}, files_provider, sstable_factory);

    UserKey k1{1};
    UserKey k2{2};
    UserKey k3{3};
    Value v1{10};
    Value v2{20};
    Value v3{30};

    lsm->Put(k3, v3);
    lsm->Put(k1, v1);
    lsm->Put(k2, v2);

    auto scan = lsm->Scan(std::nullopt, std::nullopt);
    auto result = CollectAll(*scan);
    std::vector<std::pair<UserKey, Value>> expected = {{k1, v1}, {k2, v2}, {k3, v3}};

    ASSERT_EQ(result, expected);
}

TEST(LSMGranular, ScanWithRange) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(GranularLsmOptions{}, files_provider, sstable_factory);

    for (int i = 0; i < 10; ++i) {
        lsm->Put(UserKey{static_cast<uint8_t>(i)}, Value{static_cast<uint8_t>(i * 10)});
    }

    auto scan = lsm->Scan(UserKey{3}, UserKey{7});
    auto result = CollectAll(*scan);
    std::vector<std::pair<UserKey, Value>> expected = {
        {UserKey{3}, Value{30}},
        {UserKey{4}, Value{40}},
        {UserKey{5}, Value{50}},
        {UserKey{6}, Value{60}},
    };

    ASSERT_EQ(result, expected);
}

TEST(LSMGranular, ScanSkipsTombstones) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(GranularLsmOptions{}, files_provider, sstable_factory);

    UserKey k1{1};
    UserKey k2{2};
    UserKey k3{3};
    Value v1{10};
    Value v2{20};
    Value v3{30};

    lsm->Put(k1, v1);
    lsm->Put(k2, v2);
    lsm->Put(k3, v3);

    lsm->Delete(k2);

    auto scan = lsm->Scan(std::nullopt, std::nullopt);
    auto result = CollectAll(*scan);
    std::vector<std::pair<UserKey, Value>> expected = {{k1, v1}, {k3, v3}};

    ASSERT_EQ(result, expected);
}

TEST(LSMGranular, ScanDeduplicatesVersions) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(GranularLsmOptions{}, files_provider, sstable_factory);

    UserKey k{1};
    Value v1{10};
    Value v2{20};
    Value v3{30};

    lsm->Put(k, v1);
    lsm->Put(k, v2);
    lsm->Put(k, v3);

    auto scan = lsm->Scan(std::nullopt, std::nullopt);
    auto result = CollectAll(*scan);
    std::vector<std::pair<UserKey, Value>> expected = {{k, v3}};

    ASSERT_EQ(result, expected);
}

TEST(LSMGranular, ScanAcrossLevels) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(GranularLsmOptions{.memtable_bytes = 128}, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 500;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, 5, 7));
        values.push_back(GenerateRandomKey(rng, 10, 20));
    }

    std::map<UserKey, Value> expected_state;

    const int operations = 1500;
    for (int i = 0; i < operations; ++i) {
        UserKey key = keys[rng() % keys.size()];
        Value value = values[rng() % values.size()];
        lsm->Put(key, value);
        expected_state[key] = value;
    }

    auto scan = lsm->Scan(std::nullopt, std::nullopt);
    auto result = CollectAll(*scan);

    std::vector<std::pair<UserKey, Value>> expected;
    for (const auto& [key, value] : expected_state) {
        expected.emplace_back(key, value);
    }

    ASSERT_EQ(result, expected);
}

TEST(LSMGranular, ScanWithSequenceNumber) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(GranularLsmOptions{.memtable_bytes = 128}, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 500;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, 5, 7));
        values.push_back(GenerateRandomKey(rng, 10, 20));
    }

    std::map<UserKey, Value> expected_state;

    const int operations = 1000;
    for (int i = 0; i < operations; ++i) {
        UserKey key = keys[rng() % keys.size()];
        Value value = values[rng() % values.size()];
        lsm->Put(key, value);
        expected_state[key] = value;
    }

    auto sequence_number = lsm->GetCurrentSequenceNumber();
    for (int i = 0; i < operations / 2; ++i) {
        UserKey key = keys[rng() % keys.size()];
        Value value = values[rng() % values.size()];
        lsm->Put(key, value);
    }

    auto scan = lsm->Scan(std::nullopt, std::nullopt, sequence_number);
    auto result = CollectAll(*scan);

    std::vector<std::pair<UserKey, Value>> expected;
    for (const auto& [key, value] : expected_state) {
        expected.emplace_back(key, value);
    }

    ASSERT_EQ(result, expected);
}

TEST(LSMGranular, Structure) {
    GranularLsmOptions options;
    options.memtable_bytes = 1024;
    options.max_sstable_size = 4096;
    options.l0_capacity = 2;
    options.level_size_multiplier = 2;
    options.bloom_filter_size = 1024;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(options, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 3'000;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, 5, 7));
        values.push_back(GenerateRandomKey(rng, 10, 20));
    }

    const int operations = 6'000;
    for (int i = 0; i < operations; ++i) {
        UserKey key = keys[rng() % keys.size()];
        Value value = values[rng() % values.size()];
        lsm->Put(key, value);
    }

    EXPECT_GE(files_provider->NumLevels(), 5);
    for (uint32_t level = 0; level < files_provider->NumLevels(); ++level) {
        if (level + 1 != files_provider->NumLevels()) {
            ASSERT_EQ(files_provider->NumTables(level), (1 << (level + 1)) - 1);
        }
        std::vector<SSTableMetadata> metadata;
        for (uint32_t i = 0; i < files_provider->NumTables(level); ++i) {
            auto one_metadata = files_provider->GetTableMetadata(level, i);
            ASSERT_TRUE(one_metadata.has_value()) << "level = " << level << ", i = " << i;

            EXPECT_LE(one_metadata->file_size, 2 * options.max_sstable_size) << "level = " << level << ", i = " << i;
            metadata.emplace_back(std::move(*one_metadata));
        }

        for (uint32_t i = 0; i + 1 < metadata.size(); ++i) {
            EXPECT_LE(metadata[i].max_key, metadata[i + 1].min_key) << "level = " << level << ", i = " << i;
        }
    }
}

TEST(LSMGranular, CompactionIsGranular) {
    GranularLsmOptions options;
    options.memtable_bytes = 1024;
    options.max_sstable_size = 4096;
    options.bloom_filter_size = 1024;
    options.l0_capacity = 2;
    options.level_size_multiplier = 2;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(options, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 3'000;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, 5, 7));
        values.push_back(GenerateRandomKey(rng, 10, 20));
    }

    const int operations = 6'000;
    for (int i = 0; i < operations; ++i) {
        UserKey key = keys[rng() % keys.size()];
        Value value = values[rng() % values.size()];
        lsm->Put(key, value);

        uint64_t bytes_inserted = files_provider->TotalBytesInserted();
        EXPECT_LE(bytes_inserted, options.max_sstable_size * files_provider->NumLevels() * 20) << "levels = " << files_provider->NumLevels() << ", i = " << i << std::endl;

        files_provider->ResetBytesInserted();
    }
}

}  // namespace
}  // namespace lsm
