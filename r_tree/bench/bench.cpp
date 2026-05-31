#include </home/lim/HSE/Projects/IBIS/contrib/benchmark/include/benchmark/benchmark.h>

#include <impl/r_tree/r_tree.h>
#include <sys/types.h>
#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <vector>

namespace r_tree {

using Clock = std::chrono::steady_clock;

struct Options {
    int dim;
    int b_koef;
    double alpha_koef;
    int n_points;
    int n_requests;
    int min_value = INT_MIN;
    int max_value = INT_MAX;
};

struct Results {
    double insert_time = 0;
    double nn_request_time = 0;
    uint64_t tree_max_level = 0;
};

struct RTreeTest {
    std::vector<std::vector<int64_t>> points;
    std::vector<std::vector<int64_t>> requests;
};

uint64_t GetDist2(uint64_t d,  const std::vector<int64_t>& lhs, const std::vector<int64_t>& rhs) {
    uint64_t dist = 0;
    for (uint64_t dim = 0; dim < d; ++dim) {
        uint64_t dim_dist = std::abs(lhs[dim] - rhs[dim]);
        dist += dim_dist * dim_dist;
    }
    return dist;
}

RTreeTest GenTest(uint64_t dim, uint64_t point_count, uint64_t request_count, int64_t left = INT_MIN, int64_t right = INT_MAX) {
    int64_t dl = INT_MIN, dr = INT_MAX;
    for (uint64_t del = 1; del < dim; ++del) {
        dl /= 2ll;
        dr /= 2ll;
    }
    left = std::max(left, dl);
    right = std::min(right, dr);
    std::random_device rd;
    std::mt19937 gen(rd());
    RTreeTest test;
    std::uniform_int_distribution<> point_distrib(left, right);
    for (uint64_t i = 0; i < point_count; ++i) {
        std::vector<int64_t> point(dim);
        for (auto & c : point) {
            c = point_distrib(gen);
        }
        test.points.push_back(point);
    }
    for (uint64_t i = 0; i < request_count; ++i) {
        std::vector<int64_t> point(dim);
        for (auto & c : point) {
            c = point_distrib(gen);
        }
        test.requests.push_back(point);
    }
    return test;
}

void SetCounters(benchmark::State& state, Options options, Results results) {
    double i_per_op = results.insert_time / options.n_points;
    state.counters["i(ns/op)"] = benchmark::Counter(i_per_op * 1e9);

    double nn_per_op = results.nn_request_time / options.n_requests;
    state.counters["nn(ms/op)"] = benchmark::Counter(nn_per_op * 1e3);

    double avg_max_lvl = results.tree_max_level;
    state.counters["lvl"] = benchmark::Counter(avg_max_lvl);
}

Results Testing(const Options& options) {
    auto test = GenTest(options.dim, options.n_points, options.n_requests, options.min_value, options.max_value);
    RTreeConfig config = {
        static_cast<uint64_t>(options.dim),
        static_cast<uint64_t>(options.b_koef),
        options.alpha_koef
    };
    Results results;
    auto r_tree = RTree(config);
    for (auto & point : test.points) {
        auto t0 = Clock::now();
        r_tree.Insert(point);
        auto t1 = Clock::now();
        results.insert_time += std::chrono::duration<double>(t1 - t0).count();
    }
    results.tree_max_level = r_tree.GetLvl();
    for (auto & request : test.requests) {
        auto t0 = Clock::now();
        r_tree.BestFirst(request);
        auto t1 = Clock::now();
        results.nn_request_time += std::chrono::duration<double>(t1 - t0).count();
    }
    return results;
}

void BenchD(benchmark::State& state) {
    Options options = {
        static_cast<int>(state.range(0)),
        static_cast<int>(state.range(1)),
        0.4,
        static_cast<int>(state.range(2)),
        static_cast<int>(state.range(3))
    };
    while (state.KeepRunning()) {
        auto results = Testing(options);
        SetCounters(state, options, results);
    }
}

BENCHMARK(BenchD)
    ->UseRealTime()
    ->Args({1, 3, 100, 100})
    ->Args({1, 3, 100, 1000})
    ->Args({1, 3, 1000, 1000})
    ->Args({2, 3, 1000, 1000})
    ->Args({3, 3, 1000, 1000})
    ->Args({4, 3, 1000, 1000})
    ->Args({5, 3, 1000, 1000})
    ->Args({6, 3, 1000, 1000})
    ->Args({7, 3, 1000, 1000})
    ->Args({8, 3, 1000, 1000})
    ->Args({16, 3, 1000, 1000})
    ->Args({32, 3, 1000, 1000})
    ->Args({64, 3, 1000, 1000})
    ->Args({128, 3, 1000, 1000})
    ->Args({256, 3, 1000, 1000})
    ->Unit(benchmark::kMillisecond)
    ;

BENCHMARK_MAIN();

} //namespace r_tree
