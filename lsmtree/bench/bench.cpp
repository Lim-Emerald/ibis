#include </home/lim/HSE/Projects/IBIS/contrib/benchmark/include/benchmark/benchmark.h>

#include <lsm/lsm.h>
#include <lsm/utils/lsm_utils.h>
#include <cstdint>

namespace lsm {

struct RandomKeyOptions {
    uint32_t min_key_len;
    uint32_t max_key_len;
    uint32_t min_value_len;
    uint32_t max_value_len;
};

void WriteHard(RandomKeyOptions rk_options, int operations) {
    lsm::GranularLsmOptions options;

    std::shared_ptr<ISSTableSerializer> sstable_factory = MakeSSTableFileFactory();
    auto files_provider = std::make_shared<TestVectorLevelsProvider>();
    std::shared_ptr<ILSM> lsm = MakeGranularLsm(options, files_provider, sstable_factory);

    std::vector<UserKey> keys;
    std::vector<Value> values;

    std::mt19937 rng(42);

    const int k_keys_count = 1'000;
    for (int i = 0; i < k_keys_count; ++i) {
        keys.push_back(GenerateRandomKey(rng, rk_options.min_key_len, rk_options.max_key_len));
        values.push_back(GenerateRandomKey(rng, rk_options.min_value_len, rk_options.max_value_len));
    }

    for (int i = 0; i < operations; ++i) {
        int operation = rng() % 10;
        UserKey key = keys[rng() % keys.size()];
        if (operation <= 7) {
            Value value = values[rng() % values.size()];
            lsm->Put(key, value);
        } else {
            lsm->Delete(key);
        }
    }
}

void Write(benchmark::State& state) {
    while (state.KeepRunning()) {
        WriteHard({
            .min_key_len = 27,
            .max_key_len = 30,
            .min_value_len = 300,
            .max_value_len = 600
        }, state.range(0));
    }
}

BENCHMARK(Write)
    ->MeasureProcessCPUTime()
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Args({1000})
    ->Args({6000})
    ->Args({50000})
    ;

BENCHMARK_MAIN();

} //namespace lsm
