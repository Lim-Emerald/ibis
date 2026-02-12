#include </home/lim/HSE/Projects/IBIS/contrib/gtest/gtest.h>
#include <lsm/common/types.h>
#include <lsm/memtable.h>

#include <cstdint>
#include <optional>
#include <vector>

namespace lsm {
namespace {

TEST(MemTable, PutGetDelete) {
    auto mt = MakeMemTable(20);
    UserKey k{1, 2, 3};

    Value out;
    EXPECT_EQ(mt->Get(k, &out), IMemTable::GetKind::kNotFound);

    Value v1{9};
    mt->Add(1, k, v1);
    EXPECT_EQ(mt->Get(k, &out), IMemTable::GetKind::kFound);
    EXPECT_EQ(out, v1);

    Value v2{9, 9};
    mt->Add(2, k, v2);
    EXPECT_EQ(mt->Get(k, &out), IMemTable::GetKind::kFound);
    EXPECT_EQ(out, v2);

    mt->Delete(3, k);
    EXPECT_EQ(mt->Get(k, &out), IMemTable::GetKind::kDeletion);
}

TEST(MemTable, Delete) {
    auto mt = MakeMemTable(20);
    UserKey k{1, 2, 3};

    Value out;
    EXPECT_EQ(mt->Get(k, &out), IMemTable::GetKind::kNotFound);

    mt->Delete(1, k);
    EXPECT_EQ(mt->Get(k, &out), IMemTable::GetKind::kDeletion);
}

#ifdef EXTRA_TESTS
TEST(MemTable, GetNonExistentKeyBetweenExisting) {
    auto mt = MakeMemTable(20);

    UserKey k1{1};
    UserKey k2{2};
    UserKey k3{3};
    Value v1{1};
    Value v3{3};

    mt->Add(1, k1, v1);
    mt->Add(2, k3, v3);

    Value out;
    EXPECT_EQ(mt->Get(k2, &out), IMemTable::GetKind::kNotFound);
    EXPECT_EQ(out, Value{});
}
#endif

TEST(MemTable, Scan) {
    auto mt = MakeMemTable(20);
    UserKey a{'a'};
    UserKey b{'b'};
    Value v1{1};
    Value v2{2};
    Value v3{3};

    mt->Add(1, a, v1);
    mt->Delete(2, a);
    mt->Add(4, b, v2);
    mt->Add(5, a, v3);
    mt->Delete(6, b);

    auto it = mt->MakeScan();
    std::vector<std::pair<InternalKey, Value>> all;
    while (true) {
        auto next = it->Next();
        if (!next.has_value()) {
            break;
        }
        all.push_back(*next);
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
}

TEST(MemTable, ApproximateMemoryUsageMonotonic) {
    auto mt = MakeMemTable(20);
    UserKey k1{1};
    UserKey k2{2, 2};
    Value v1{5};
    Value v2{6, 6, 6};

    uint64_t m0 = mt->ApproximateMemoryUsage();

    mt->Add(1, k1, v1);

    uint64_t m1 = mt->ApproximateMemoryUsage();
    EXPECT_GT(m1, m0);

    mt->Add(2, k2, v2);

    uint64_t m2 = mt->ApproximateMemoryUsage();
    EXPECT_GT(m2, m1);

    mt->Delete(3, k2);

    uint64_t m3 = mt->ApproximateMemoryUsage();
    EXPECT_GT(m3, m2);
}

TEST(MemTable, GetWithSequenceNumber) {
    auto mt = MakeMemTable(20);
    UserKey k{1, 2, 3};
    Value v1{10};
    Value v2{20};
    Value v3{30};
    Value out;

    mt->Add(1, k, v1);
    mt->Add(3, k, v2);
    mt->Delete(5, k);
    mt->Add(7, k, v3);

    EXPECT_EQ(mt->Get(k, &out, 0), IMemTable::GetKind::kNotFound);
    EXPECT_EQ(out, Value{});

    EXPECT_EQ(mt->Get(k, &out, 1), IMemTable::GetKind::kFound);
    EXPECT_EQ(out, v1);

    EXPECT_EQ(mt->Get(k, &out, 2), IMemTable::GetKind::kFound);
    EXPECT_EQ(out, v1);

    EXPECT_EQ(mt->Get(k, &out, 3), IMemTable::GetKind::kFound);
    EXPECT_EQ(out, v2);

    EXPECT_EQ(mt->Get(k, &out, 4), IMemTable::GetKind::kFound);
    EXPECT_EQ(out, v2);

    EXPECT_EQ(mt->Get(k, &out, 5), IMemTable::GetKind::kDeletion);
    EXPECT_EQ(out, Value{});

    EXPECT_EQ(mt->Get(k, &out, 6), IMemTable::GetKind::kDeletion);
    EXPECT_EQ(out, Value{});

    EXPECT_EQ(mt->Get(k, &out, 7), IMemTable::GetKind::kFound);
    EXPECT_EQ(out, v3);

    EXPECT_EQ(mt->Get(k, &out), IMemTable::GetKind::kFound);
    EXPECT_EQ(out, v3);
}

}  // namespace
}  // namespace lsm
