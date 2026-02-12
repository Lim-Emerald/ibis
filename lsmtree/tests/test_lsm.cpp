#include </home/lim/HSE/Projects/IBIS/contrib/gtest/gtest.h>
#include <lsm/common/types.h>
#include <lsm/lsm.h>
#include <lsm/sstable.h>

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

#include <lsm/utils/lsm_utils.h>

namespace lsm {
namespace {

TEST(LSM, PutGet) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(LsmOptions{}, files_provider, sstable_factory);

    UserKey k1{'a'};
    Value v1{1};

    EXPECT_EQ(lsm->Get(k1), std::nullopt);

    lsm->Put(k1, v1);
    EXPECT_EQ(lsm->Get(k1), v1);
}

TEST(LSM, Delete) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(LsmOptions{}, files_provider, sstable_factory);

    UserKey k1{'a'};
    Value v1{1};

    lsm->Put(k1, v1);
    EXPECT_EQ(lsm->Get(k1), v1);

    lsm->Delete(k1);
    EXPECT_EQ(lsm->Get(k1), std::nullopt);

    lsm->Put(k1, v1);
    EXPECT_EQ(lsm->Get(k1), v1);
}

TEST(LSM, PutGetWithFlushing) {
    LsmOptions options;
    options.memtable_bytes = 32;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(options, files_provider, sstable_factory);

    uint32_t total_keys = 0;
    for (int i = 0; i <= 10; ++i) {
        UserKey key{static_cast<uint8_t>('a' + i)};
        Value value{static_cast<uint8_t>(i)};
        lsm->Put(key, value);
        ++total_keys;
        if (files_provider->NumLevels() > 0) {
            break;
        }
    }

    ASSERT_EQ(files_provider->NumLevels(), 1);
    ASSERT_EQ(files_provider->NumTables(0), 1);

    for (uint32_t i = 0; i < total_keys; ++i) {
        UserKey key{static_cast<uint8_t>('a' + i)};
        Value value{static_cast<uint8_t>(i)};
        ASSERT_EQ(lsm->Get(key), value) << "i = " << i;
    }
}

TEST(LSM, MultipleFlushesLatestWins) {
    LsmOptions options;
    options.memtable_bytes = 16'000;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(options, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 1'000;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng));
        values.push_back(GenerateRandomKey(rng));
    }

    std::map<UserKey, Value> expected_state;

    const int operations = 20'000;
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

TEST(LSM, LevelsStructureScalesCorrectly) {
    LsmOptions options;
    options.memtable_bytes = 50;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();

    for (int n : {(1 << 7), (1 << 10), (1 << 12)}) {
        auto files_provider = std::make_shared<TestVectorLevelsProvider>();
        std::shared_ptr<ILSM> lsm = MakeLsm(options, files_provider, sstable_factory);

        for (int i = 0; i < n; i++) {
            UserKey k{static_cast<uint8_t>(i)};
            Value v{static_cast<uint8_t>(i)};
            lsm->Put(k, v);
        }

        size_t levels = files_provider->NumLevels();
        size_t max_tables_per_level = 0;
        for (size_t lvl = 0; lvl < levels; lvl++) {
            max_tables_per_level = std::max(max_tables_per_level, files_provider->NumTables(lvl));
        }

        double min_expected_levels = log2(n / options.memtable_bytes) - 1;
        double expected_levels = log2(n);
        EXPECT_LE(min_expected_levels, levels) << "n = " << n << ", levels = " << levels;
        EXPECT_LE(levels, expected_levels) << "n = " << n << ", levels = " << levels;

        EXPECT_LE(max_tables_per_level, options.compaction_trigger_files - 1) << "n = " << n << ", max_tables =" << max_tables_per_level;
    }
}

TEST(LSM, WriteAmplificationBounded) {
    LsmOptions options;
    options.memtable_bytes = 1024;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(options, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 3'000;
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

    double write_amplification = static_cast<double>(bytes_written_in_real_world) / bytes_written_in_ideal_world;
    std::cerr << "write amplification = " << write_amplification << std::endl;
    EXPECT_LT(write_amplification, log2(operations));

    uint64_t bytes_read_in_read_world = files_provider->TotalBytesRead();
    EXPECT_LT(bytes_read_in_read_world, bytes_written_in_real_world);
}

TEST(LSM, SearchComplexityByKeyAge) {
    LsmOptions options;
    options.memtable_bytes = 124;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(options, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 3'000;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, 5, 7));
        values.push_back(GenerateRandomKey(rng, 15, 15));
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
        TestInfo{.key = keys[k_keys_count - 256], .reads_lower_bound = 1, .reads_upper_bound = 5}, TestInfo{.key = keys[0], .reads_lower_bound = 2, .reads_upper_bound = 10}  // oldest key
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

TEST(LSM, GetWithSequenceNumber) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(LsmOptions{.memtable_bytes = 100}, files_provider, sstable_factory);

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
TEST(LSM, ScanMultipleKeys) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(LsmOptions{}, files_provider, sstable_factory);

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

TEST(LSM, ScanWithRange) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(LsmOptions{}, files_provider, sstable_factory);

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

TEST(LSM, ScanSkipsTombstones) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(LsmOptions{}, files_provider, sstable_factory);

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

TEST(LSM, ScanDeduplicatesVersions) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(LsmOptions{}, files_provider, sstable_factory);

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

TEST(LSM, ScanAcrossLevels) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(LsmOptions{.memtable_bytes = 1000}, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 1000;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, 5, 7));
        values.push_back(GenerateRandomKey(rng, 100, 200));
    }

    std::map<UserKey, Value> expected_state;

    const int operations = 3000;
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

TEST(LSM, ScanWithSequenceNumber) {
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeLsm(LsmOptions{.memtable_bytes = 1000}, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 1000;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, 5, 7));
        values.push_back(GenerateRandomKey(rng, 100, 200));
    }

    std::map<UserKey, Value> expected_state;

    const int operations = 3000;
    for (int i = 0; i < operations; ++i) {
        UserKey key = keys[rng() % keys.size()];
        Value value = values[rng() % values.size()];
        lsm->Put(key, value);
        expected_state[key] = value;
    }

    auto sequence_number = lsm->GetCurrentSequenceNumber();
    for (int i = 0; i < operations; ++i) {
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

}  // namespace
}  // namespace lsm
