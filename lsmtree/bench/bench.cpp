#include </home/lim/HSE/Projects/IBIS/contrib/benchmark/include/benchmark/benchmark.h>

#include <lsm/lsm.h>
#include <lsm/utils/lsm_utils.h>
#include <sys/types.h>
#include <cstdint>
#include <memory>

namespace lsm {

using Clock = std::chrono::steady_clock;

struct Options {
    int min_key_len;
    int max_key_len;
    int min_value_len;
    int max_value_len;
    int keys_count;
    int operations;
    int scan_segment_size = 0;
    int scan_operations = 0;
};

struct Results {
    uint64_t bytes_written_in_ideal_world = 0;
    uint64_t bytes_written_in_real_world = 0;
    uint64_t bytes_read_in_ideal_world = 0;
    uint64_t bytes_read_in_real_world = 0;
    double write_time = 0;
    double read_time = 0;
    uint64_t lsmtree_max_level = 0;
};

void SetCounters(benchmark::State& state, Options options, Results results) {

    double w_s_per_op = results.write_time / options.operations;
    state.counters["w(ns/op)"] = benchmark::Counter(w_s_per_op * 1e9);

    double w_b_per_op = results.bytes_written_in_real_world / options.operations;
    state.counters["w(b/op)"] = benchmark::Counter(w_b_per_op);

    double write_amplification = static_cast<double>(results.bytes_written_in_real_world) / results.bytes_written_in_ideal_world;
    state.counters["WA"] = benchmark::Counter(write_amplification);

    double r_s_per_op = results.read_time / options.operations;
    state.counters["r(ns/op)"] = benchmark::Counter(r_s_per_op * 1e9);

    double r_b_per_op = results.bytes_read_in_real_world / options.operations;
    state.counters["r(b/op)"] = benchmark::Counter(r_b_per_op);

    double read_amplification = static_cast<double>(results.bytes_read_in_real_world) / results.bytes_read_in_ideal_world;
    state.counters["RA"] = benchmark::Counter(read_amplification);

    state.counters["lvl"] = benchmark::Counter(results.lsmtree_max_level);
}

Results TestWriteRead(Options rt_options, lsm::GranularLsmOptions options) {
    Results results;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(options, files_provider, sstable_factory, &results.bytes_read_in_real_world);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    for (int i = 0; i < rt_options.keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, rt_options.min_key_len, rt_options.max_key_len));
        values.push_back(GenerateRandomKey(rng, rt_options.min_value_len, rt_options.max_value_len));
    }

    results.bytes_written_in_ideal_world = 0;
    for (int i = 0; i < rt_options.operations; ++i) {
        UserKey key = keys[rng() % keys.size()];
        Value value = values[rng() % values.size()];

        auto t0 = Clock::now();
        lsm->Put(key, value);
        auto t1 = Clock::now();

        results.write_time += std::chrono::duration<double>(t1 - t0).count();

        const uint32_t internal_key_size = sizeof(InternalKey::sequence_number) + sizeof(uint64_t) + key.size();
        const uint32_t value_size = sizeof(uint64_t) + value.size();

        results.bytes_written_in_ideal_world += internal_key_size + value_size;
    }
    results.bytes_written_in_real_world = files_provider->TotalBytesInserted();
    results.lsmtree_max_level = files_provider->NumLevels();

    results.bytes_read_in_ideal_world = 0;
    results.bytes_read_in_real_world = 0;
    for (int i = 0; i < rt_options.operations; ++i) {
        UserKey key = keys[rng() % keys.size()];

        auto t0 = Clock::now();
        auto value = lsm->Get(key);
        auto t1 = Clock::now();

        results.read_time += std::chrono::duration<double>(t1 - t0).count();

        const uint32_t internal_key_size = sizeof(InternalKey::sequence_number) + sizeof(uint64_t) + key.size();
        const uint32_t value_size = sizeof(uint64_t) + (value ? value->size() : 0);

        results.bytes_read_in_ideal_world += internal_key_size + value_size;
    }

    return results;
}

void MemTable(benchmark::State& state) {
    while (state.KeepRunning()) {
        Options options = {
            5, 7, 10, 20,
            static_cast<int>(state.range(0)),
            static_cast<int>(state.range(1)),
        };
        GranularLsmOptions lsm_options;
        lsm_options.memtable_bytes = 64ull * 1024 * 1024;
        auto results = TestWriteRead(options, lsm_options);
        SetCounters(state, options, results);
    }
}

void Hard(benchmark::State& state) {
    while (state.KeepRunning()) {
        Options options = {
            5, 7, 10, 20,
            static_cast<int>(state.range(0)),
            static_cast<int>(state.range(1)),
        };
        GranularLsmOptions lsm_options;
        lsm_options.memtable_bytes = 1024;
        lsm_options.max_sstable_size = 4096;
        lsm_options.buffer_pool_size = 4096;
        lsm_options.frame_size = 32;
        lsm_options.bloom_filter_size = 1024;
        auto results = TestWriteRead(options, lsm_options);
        SetCounters(state, options, results);
    }
}

void HardWithoutFilter(benchmark::State& state) {
    while (state.KeepRunning()) {
        Options options = {
            5, 7, 10, 20,
            static_cast<int>(state.range(0)),
            static_cast<int>(state.range(1)),
        };
        GranularLsmOptions lsm_options;
        lsm_options.memtable_bytes = 1024;
        lsm_options.max_sstable_size = 4096;
        lsm_options.buffer_pool_size = 4096;
        lsm_options.frame_size = 32;
        lsm_options.bloom_filter_size = 0;
        auto results = TestWriteRead(options, lsm_options);
        SetCounters(state, options, results);
    }
}

void BigTables(benchmark::State& state) {
    while (state.KeepRunning()) {
        Options options = {
            30, 40, 200'000, 400'000,
            static_cast<int>(state.range(0)),
            static_cast<int>(state.range(1)),
        };
        GranularLsmOptions lsm_options;
        auto results = TestWriteRead(options, lsm_options);
        SetCounters(state, options, results);
    }
}

void BigTablesWithoutFilter(benchmark::State& state) {
    while (state.KeepRunning()) {
        Options options = {
            30, 40, 200'000, 400'000,
            static_cast<int>(state.range(0)),
            static_cast<int>(state.range(1)),
        };
        GranularLsmOptions lsm_options;
        lsm_options.bloom_filter_size = 0;
        auto results = TestWriteRead(options, lsm_options);
        SetCounters(state, options, results);
    }
}

Results TestChaos(Options rt_options, lsm::GranularLsmOptions options) {
    Results results;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(options, files_provider, sstable_factory, &results.bytes_read_in_real_world);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    for (int i = 0; i < rt_options.keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, rt_options.min_key_len, rt_options.max_key_len));
        values.push_back(GenerateRandomKey(rng, rt_options.min_value_len, rt_options.max_value_len));
    }

    results.bytes_read_in_ideal_world = 0;
    results.bytes_read_in_real_world = 0;
    results.bytes_written_in_ideal_world = 0;
    for (int i = 0; i < rt_options.operations; ++i) {
        int operation = rng() % 10;
        UserKey key = keys[rng() % keys.size()];
        if (operation <= 7) {
            Value value = values[rng() % values.size()];
            auto t0 = Clock::now();
            lsm->Put(key, value);
            auto t1 = Clock::now();
            results.write_time += std::chrono::duration<double>(t1 - t0).count();

            const uint32_t internal_key_size = sizeof(InternalKey::sequence_number) + sizeof(uint64_t) + key.size();
            const uint32_t value_size = sizeof(uint64_t) + value.size();

            results.bytes_written_in_ideal_world += internal_key_size + value_size;
        } else if (operation == 8) {
            auto t0 = Clock::now();
            lsm->Delete(key);
            auto t1 = Clock::now();
            results.write_time += std::chrono::duration<double>(t1 - t0).count();

            const uint32_t internal_key_size = sizeof(InternalKey::sequence_number) + sizeof(uint64_t) + key.size();
            const uint32_t value_size = sizeof(uint64_t);

            results.bytes_written_in_ideal_world += internal_key_size + value_size;
        } else {
            auto t0 = Clock::now();
            auto value = lsm->Get(key);
            auto t1 = Clock::now();
            results.read_time += std::chrono::duration<double>(t1 - t0).count();

            const uint32_t internal_key_size = sizeof(InternalKey::sequence_number) + sizeof(uint64_t) + key.size();
            const uint32_t value_size = sizeof(uint64_t) + (value ? value->size() : 0);

            results.bytes_read_in_ideal_world += internal_key_size + value_size;
        }
    }
    results.bytes_written_in_real_world = files_provider->TotalBytesInserted();
    results.lsmtree_max_level = files_provider->NumLevels();

    return results;
}

void MemTableChaos(benchmark::State& state) {
    while (state.KeepRunning()) {
        Options options = {
            5, 7, 10, 20,
            static_cast<int>(state.range(0)),
            static_cast<int>(state.range(1)),
        };
        GranularLsmOptions lsm_options;
        lsm_options.memtable_bytes = 64ull * 1024 * 1024;
        auto results = TestChaos(options, lsm_options);
        SetCounters(state, options, results);
    }
}

void HardChaos(benchmark::State& state) {
    while (state.KeepRunning()) {
        Options options = {
            5, 7, 10, 20,
            static_cast<int>(state.range(0)),
            static_cast<int>(state.range(1)),
        };
        GranularLsmOptions lsm_options;
        lsm_options.memtable_bytes = 1024;
        lsm_options.max_sstable_size = 4096;
        lsm_options.buffer_pool_size = 4096;
        lsm_options.frame_size = 32;
        lsm_options.bloom_filter_size = 1024;
        auto results = TestChaos(options, lsm_options);
        SetCounters(state, options, results);
    }
}

void BigTablesChaos(benchmark::State& state) {
    while (state.KeepRunning()) {
        Options options = {
            30, 40, 200'000, 400'000,
            static_cast<int>(state.range(0)),
            static_cast<int>(state.range(1)),
        };
        GranularLsmOptions lsm_options;
        auto results = TestChaos(options, lsm_options);
        SetCounters(state, options, results);
    }
}

struct EnvLSM {
    std::shared_ptr<TestVectorLevelsProvider> files_provider;
    std::shared_ptr<ILSM> lsm;
    std::vector<UserKey> keys;
    std::vector<Value> values;
    uint64_t bytes_read_in_real_world = 0;
};

EnvLSM GenerateLSM(Options rt_options, lsm::GranularLsmOptions options) {
    EnvLSM env;
    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    env.files_provider = std::make_shared<TestVectorLevelsProvider>();
    env.lsm = MakeGranularLsm(options, env.files_provider, sstable_factory, &env.bytes_read_in_real_world);

    std::mt19937 rng(42);

    for (int i = 0; i < rt_options.keys_count; ++i) {
        env.keys.push_back(GenerateRandomKey(rng, rt_options.min_key_len, rt_options.max_key_len));
        env.values.push_back(GenerateRandomKey(rng, rt_options.min_value_len, rt_options.max_value_len));
    }

    for (int i = 0; i < rt_options.operations; ++i) {
        UserKey key = env.keys[rng() % env.keys.size()];
        Value value = env.values[rng() % env.values.size()];
        env.lsm->Put(key, value);
    }

    return env;
}

Results TestScan(EnvLSM env, Options rt_options, lsm::GranularLsmOptions options) {
    Results results;
    results.lsmtree_max_level = env.files_provider->NumLevels();

    std::mt19937 rng(42);

    results.bytes_read_in_ideal_world = 0;
    results.bytes_read_in_real_world = 0;
    for (int i = 0; i < rt_options.scan_operations; ++i) {
        UserKey key1 = env.keys[rng() % env.keys.size()];
        UserKey key2 = env.keys[std::min(static_cast<int>(rng() % env.keys.size()) + rt_options.scan_segment_size, static_cast<int>(env.keys.size()) - 1)];
        if (key1 > key2) {
            swap(key1, key2);
        }

        auto t0 = Clock::now();
        auto scaner = env.lsm->Scan(key1, key2);
        auto t1 = Clock::now();

        results.read_time += std::chrono::duration<double>(t1 - t0).count();

        auto kv = scaner->Next();
        while (kv) {
            const uint32_t internal_key_size = sizeof(InternalKey::sequence_number) + sizeof(uint64_t) + kv->first.size();
            const uint32_t value_size = sizeof(uint64_t) + kv->second.size();
            results.bytes_read_in_ideal_world += internal_key_size + value_size;
            kv = scaner->Next();
        }
    }

    return results;
}

void HardScan(benchmark::State& state) {
    Options options = {
        5, 7, 10, 20,
        static_cast<int>(state.range(0)),
        static_cast<int>(state.range(1)),
        static_cast<int>(state.range(2)),
        static_cast<int>(state.range(3)),
    };
    GranularLsmOptions lsm_options;
    lsm_options.memtable_bytes = 1024;
    lsm_options.max_sstable_size = 4096;
    lsm_options.buffer_pool_size = 4096;
    lsm_options.frame_size = 16;
    lsm_options.bloom_filter_size = 1024;
    auto env = GenerateLSM(options, lsm_options);
    while (state.KeepRunning()) {
        env.bytes_read_in_real_world = 0;
        auto results = TestScan(env, options, lsm_options);
        results.bytes_read_in_real_world = env.bytes_read_in_real_world;
        SetCounters(state, options, results);
    }
}

void HardScanWithoutFilter(benchmark::State& state) {
    Options options = {
        5, 7, 10, 20,
        static_cast<int>(state.range(0)),
        static_cast<int>(state.range(1)),
        static_cast<int>(state.range(2)),
        static_cast<int>(state.range(3)),
    };
    GranularLsmOptions lsm_options;
    lsm_options.memtable_bytes = 1024;
    lsm_options.max_sstable_size = 4096;
    lsm_options.buffer_pool_size = 4096;
    lsm_options.frame_size = 16;
    lsm_options.bloom_filter_size = 0;
    auto env = GenerateLSM(options, lsm_options);
    while (state.KeepRunning()) {
        env.bytes_read_in_real_world = 0;
        auto results = TestScan(env, options, lsm_options);
        results.bytes_read_in_real_world = env.bytes_read_in_real_world;
        SetCounters(state, options, results);
    }
}

void BigTablesScan(benchmark::State& state) {
    Options options = {
            30, 40, 200'000, 400'000,
        1000,
        3000,
        static_cast<int>(state.range(1)),
        static_cast<int>(state.range(2)),
    };
    GranularLsmOptions lsm_options;
    auto env = *reinterpret_cast<EnvLSM*>(state.range(0));
    while (state.KeepRunning()) {
        env.bytes_read_in_real_world = 0;
        auto results = TestScan(env, options, lsm_options);
        results.bytes_read_in_real_world = env.bytes_read_in_real_world;
        SetCounters(state, options, results);
    }
}

BENCHMARK(MemTable)
    ->UseRealTime()
    ->Args({300, 1000})
    ->Args({2000, 6000})
    ->Args({10000, 50000})
    ->Unit(benchmark::kMillisecond)
    ;

BENCHMARK(MemTableChaos)
    ->UseRealTime()
    ->Args({300, 1000})
    ->Args({2000, 6000})
    ->Args({10000, 50000})
    ->Unit(benchmark::kMillisecond)
    ;

BENCHMARK(Hard)
    ->UseRealTime()
    ->Args({300, 1000})
    ->Args({2000, 6000})
    ->Unit(benchmark::kMillisecond)
    ;

BENCHMARK(HardChaos)
    ->UseRealTime()
    ->Args({300, 1000})
    ->Args({2000, 6000})
    ->Unit(benchmark::kMillisecond)
    ;

BENCHMARK(HardWithoutFilter)
    ->UseRealTime()
    ->Args({300, 1000})
    ->Args({2000, 6000})
    ->Unit(benchmark::kMillisecond)
    ;

BENCHMARK(HardScan)
    ->UseRealTime()
    ->Args({300, 1000, 20, 100})
    ->Args({2000, 6000, 100, 20})
    ->Unit(benchmark::kMillisecond)
    ;

BENCHMARK(HardScanWithoutFilter)
    ->UseRealTime()
    ->Args({300, 1000, 20, 100})
    ->Args({2000, 6000, 100, 20})
    ->Unit(benchmark::kMillisecond)
    ;

BENCHMARK(BigTables)
    ->UseRealTime()
    ->Args({300, 1000})
    ->Args({1000, 3000})
    ->Unit(benchmark::kMillisecond)
    ;
BENCHMARK(BigTablesChaos)
    ->UseRealTime()
    ->Args({300, 1000})
    ->Args({1000, 3000})
    ->Unit(benchmark::kMillisecond)
    ;

BENCHMARK(BigTablesWithoutFilter)
    ->UseRealTime()
    ->Args({300, 1000})
    ->Args({1000, 3000})
    ->Unit(benchmark::kMillisecond)
    ;

// const auto kBigTableGeneration = GenerateLSM(
//     {
//         30, 40, 200'000, 400'000,
//         1000, 3000
//     },
//     GranularLsmOptions()
// );

// BENCHMARK(BigTablesScan)
//     ->UseRealTime()
//     ->Args({reinterpret_cast<long>(&kBigTableGeneration), 20, 5})
//     ->Args({reinterpret_cast<long>(&kBigTableGeneration), 100, 5})
//     ->Args({reinterpret_cast<long>(&kBigTableGeneration), 1000, 5})
//     ->Unit(benchmark::kMillisecond)
//     ;

BENCHMARK_MAIN();

} //namespace lsm
