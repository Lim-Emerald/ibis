#include </home/lim/HSE/Projects/IBIS/contrib/gtest/gtest.h>

#include <impl/r_tree/r_tree.h>

#include <cassert>
#include <climits>
#include <cstdint>
#include <random>
#include <ratio>
#include <set>
#include <string>
#include <vector>

namespace r_tree {

namespace {

struct RTreeTest {
    std::vector<std::vector<int64_t>> points;
    std::vector<std::vector<int64_t>> requests;
    std::vector<std::set<std::vector<int64_t>>> answers;
};

TEST(RTree_1D, SmallTests) {
    RTreeConfig config;
    config.d = 1;
    std::vector<RTreeTest> tests = {
        {
            {{-10}, {-6}, {0}, {4}, {7}},
            {{-18}, {-2}, {1}, {5}, {6}, {8}},
            {{{-10}}, {{0}}, {{0}}, {{4}}, {{7}}, {{7}}}
        }, {
            {{-100}, {-61}, {12}, {42}, {73}},
            {{-180}, {-22}, {13}, {51}, {73}, {80}},
            {{{-100}}, {{12}}, {{12}}, {{42}}, {{73}}, {{73}}}
        }
    };

    for (auto & test : tests) {
        assert(test.requests.size() == test.answers.size());
        auto r_tree = RTree(config);
        for (auto & point : test.points) {
            r_tree.Insert(point);
        }
        for (size_t ind = 0; ind < test.requests.size(); ++ind) {
            auto result = r_tree.BestFirst(test.requests[ind]);
            ASSERT_TRUE(test.answers[ind].contains(result));
        }
    }
}

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
        auto best_dist = GetDist2(dim, point, test.points[0]);
        std::set<std::vector<int64_t>> answer = {test.points[0]};
        for (uint64_t ind = 1; ind < test.points.size(); ++ind) {
            auto dist = GetDist2(dim, point, test.points[ind]);
            if (best_dist > dist) {
                best_dist = dist;
                answer.clear();
                answer.insert(test.points[ind]);
            } else if (best_dist == dist) {
                answer.insert(test.points[ind]);
            }
        }
        test.answers.push_back(answer);
    }
    return test;
}

TEST(RTree_1D, GenTests) {
    RTreeConfig config;
    config.d = 1;
    std::vector<RTreeTest> tests(1);
    for (auto & test : tests) {
        test = GenTest(config.d, 100, 100);
    }

    for (auto & test : tests) {
        assert(test.requests.size() == test.answers.size());
        auto r_tree = RTree(config);
        for (auto & point : test.points) {
            r_tree.Insert(point);
        }
        for (size_t ind = 0; ind < test.requests.size(); ++ind) {
            auto result = r_tree.BestFirst(test.requests[ind]);
            ASSERT_TRUE(test.answers[ind].contains(result));
        }
    }
}

TEST(RTree_2D, GenTests) {
    RTreeConfig config;
    config.d = 2;
    std::vector<RTreeTest> tests(10);
    for (auto & test : tests) {
        test = GenTest(config.d, 1000, 1000);
    }

    for (auto & test : tests) {
        assert(test.requests.size() == test.answers.size());
        auto r_tree = RTree(config);
        for (auto & point : test.points) {
            r_tree.Insert(point);
        }
        for (size_t ind = 0; ind < test.requests.size(); ++ind) {
            auto result = r_tree.BestFirst(test.requests[ind]);
            ASSERT_TRUE(test.answers[ind].contains(result));
        }
    }
}

TEST(RTree_3D, GenTests) {
    RTreeConfig config;
    config.d = 3;
    std::vector<RTreeTest> tests(10);
    for (auto & test : tests) {
        test = GenTest(config.d, 1000, 1000);
    }

    for (auto & test : tests) {
        assert(test.requests.size() == test.answers.size());
        auto r_tree = RTree(config);
        for (auto & point : test.points) {
            r_tree.Insert(point);
        }
        for (size_t ind = 0; ind < test.requests.size(); ++ind) {
            auto result = r_tree.BestFirst(test.requests[ind]);
            ASSERT_TRUE(test.answers[ind].contains(result));
        }
    }
}

TEST(RTree_4D, GenTests) {
    RTreeConfig config;
    config.d = 3;
    std::vector<RTreeTest> tests(10);
    for (auto & test : tests) {
        test = GenTest(config.d, 1000, 1000);
    }

    for (auto & test : tests) {
        assert(test.requests.size() == test.answers.size());
        auto r_tree = RTree(config);
        for (auto & point : test.points) {
            r_tree.Insert(point);
        }
        for (size_t ind = 0; ind < test.requests.size(); ++ind) {
            auto result = r_tree.BestFirst(test.requests[ind]);
            ASSERT_TRUE(test.answers[ind].contains(result));
        }
    }
}

}  // namespace
}  // namespace r_tree
