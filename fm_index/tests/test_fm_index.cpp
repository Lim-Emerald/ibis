#include </home/lim/HSE/Projects/IBIS/contrib/gtest/gtest.h>

#include <impl/index/index.h>
#include <impl/ukkonen/ukkonen.h>

#include <algorithm>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace fm_index {

namespace ukkonen {

TEST(Ukkonen, TestUkkonen) {
    auto res = MakeSuffixArray("abracadabra#");
    std::vector<size_t> exp = {11, 10, 7, 0, 3, 5, 8, 1, 4, 6, 9, 2};
    ASSERT_EQ(res, exp);
}

TEST(Ukkonen, SmartTestUkkonen) {
    auto res = MakeSuffixArray("CGATACGTGTGTCCCCTACT#");
    std::vector<size_t> exp = {20, 4, 17, 2, 12, 13, 14, 0, 5, 18, 15, 1, 10, 8, 6, 19, 3, 16, 11, 9, 7};
    ASSERT_EQ(res, exp);
}

}

namespace {

struct FmIndexTest {
    std::string request;
    uint64_t count;
    std::vector<uint64_t> locate;
};

TEST(FmIndex, SmallRequests) {
    std::vector<std::string> texts = {
        "abracadabra"
    };
    std::string merged_str;
    for (auto & s : texts) {
        merged_str += s + "#";
    }
    auto fm_index = FMIndex(merged_str, 64);

    std::vector<FmIndexTest> tests = {
        {
            .request = "bra",
            .count = 2,
            .locate = {1, 8},
        }, {
            .request = "abra",
            .count = 2,
            .locate = {0, 7},
        }, {
            .request = "cabra",
            .count = 0,
            .locate = {},
        }, {
            .request = "cada",
            .count = 1,
            .locate = {4},
        }
    };

    for (auto & test : tests) {
        ASSERT_EQ(fm_index.Count(test.request), test.count);
        auto locate = fm_index.Locate(test.request);
        sort(locate.begin(), locate.end());
        sort(test.locate.begin(), test.locate.end());
        ASSERT_EQ(locate, test.locate);
    }
}

TEST(FmIndex, RepeatedBlocks) {
    std::string genstr;
    for (char c = 'd'; c < 'd' + 11; ++c) {
        genstr += "abc";
        genstr.push_back(c);
    }
    std::vector<std::string> texts = {
        genstr
    };
    std::string merged_str;
    for (auto & s : texts) {
        merged_str += s + "#";
    }
    auto fm_index = FMIndex(merged_str, 64);

    std::vector<FmIndexTest> tests = {
        {
            .request = "abc",
            .count = 11,
            .locate = {0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40},
        }, {
            .request = "ab",
            .count = 11,
            .locate = {0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40},
        }, {
            .request = "abcd",
            .count = 1,
            .locate = {0},
        }, {
            .request = "cfab",
            .count = 1,
            .locate = {10},
        }, {
            .request = "bc",
            .count = 11,
            .locate = {1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41},
        }
    };

    for (auto & test : tests) {
        ASSERT_EQ(fm_index.Count(test.request), test.count);
        auto locate = fm_index.Locate(test.request);
        sort(locate.begin(), locate.end());
        sort(test.locate.begin(), test.locate.end());
        ASSERT_EQ(locate, test.locate);
    }
}

std::string GenDNA(uint64_t len) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(1, 4);
    std::string dna;
    while (dna.size() < len) {
        char sym;
        switch (distrib(gen)) {
            case 1:
                sym = 'A';
                break;
            case 2:
                sym = 'T';
                break;
            case 3:
                sym = 'G';
                break;
            case 4:
                sym = 'C';
                break;
        }
        dna.push_back(sym);
    }
    return dna;
}

TEST(FmIndex, RandomDNA) {
    uint64_t text_count = 100, dna_len = 10000;
    std::vector<std::string> texts;
    while (text_count--) {
        texts.push_back(GenDNA(dna_len));
    }
    std::string merged_str;
    for (auto & s : texts) {
        merged_str += s + "#";
    }
    merged_str.back() = '!';
    auto fm_index = FMIndex(merged_str, 64);

    std::vector<FmIndexTest> tests;

    uint64_t test_count = 50, test_len = 30;
    while (--test_count) {
        auto test_dna = GenDNA(test_len);
        FmIndexTest test = {test_dna, 0, {}};
        for (size_t pos = 0; pos + test_len <= merged_str.size(); ++pos) {
            bool eq = true;
            for (size_t shift = 0; shift < test_dna.size(); ++shift) {
                if (test_dna[shift] != merged_str[pos + shift]) {
                    eq = false;
                    break;
                }
            }
            if (eq) {
                ++test.count;
                test.locate.push_back(pos);
            }
        }
        tests.push_back(test);
    }

    for (auto & test : tests) {
        ASSERT_EQ(fm_index.Count(test.request), test.count);
        auto locate = fm_index.Locate(test.request);
        sort(locate.begin(), locate.end());
        sort(test.locate.begin(), test.locate.end());
        ASSERT_EQ(locate, test.locate);
    }
}

}  // namespace
}  // namespace invindex
