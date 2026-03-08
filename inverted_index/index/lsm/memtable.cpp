#include "index/lsm/memtable.h"

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "index/common/types.h"

namespace invindex::lsm {

namespace {

class DefaultMemTableImpl : public IMemTable<DefaultValue> {
   public:
    DefaultMemTableImpl(uint64_t max_level) : max_level_(max_level) {
        head_ = std::make_shared<Node>();
        head_->links.resize(max_level_, nullptr);
        std::random_device device;
        random_generator_.seed(device());
    }

    void Add(const UserKey& user_key, const DefaultValue& value) override {
        auto node = std::make_shared<Node>();
        node->key = {user_key};
        node->value = value;
        InsertNode(node);
    }

    GetKind Get(const UserKey& user_key, DefaultValue& out_value) const override {
        out_value = {};
        InternalKey key = {user_key};
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0] && cur->links[0]->key.user_key == user_key) {
            out_value = cur->links[0]->value;
            return GetKind::kFound;
        } else {
            return GetKind::kNotFound;
        }
    }

    std::optional<UserKey> LowerBound(const UserKey& user_key, DefaultValue& out_value) const override {
        out_value = {};
        InternalKey key = {user_key};
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0]) {
            out_value = cur->links[0]->value;
            return cur->links[0]->key.user_key;
        } else {
            return std::nullopt;
        }
    }

    std::optional<UserKey> UpperBound(const UserKey& user_key, DefaultValue& out_value) const override {
        out_value = {};
        InternalKey key = {user_key};
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key <= key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0]) {
            out_value = cur->links[0]->value;
            return cur->links[0]->key.user_key;
        } else {
            return std::nullopt;
        }
    }

    std::shared_ptr<IStream<std::pair<InternalKey, DefaultValue>>> MakeScan() const override { return std::make_shared<MemTableStream>(head_); }

    uint64_t ApproximateMemoryUsage() const override { return amu_; }

    virtual ~DefaultMemTableImpl() = default;

   private:
    struct Node {
        InternalKey key;
        DefaultValue value;
        std::vector<std::shared_ptr<Node>> links;
    };

    class MemTableStream : public IStream<std::pair<InternalKey, DefaultValue>> {
       public:
        MemTableStream(std::shared_ptr<Node> head) : cur_(head->links[0]) {}

        std::optional<std::pair<InternalKey, DefaultValue>> Next() {
            if (!cur_) {
                return std::nullopt;
            }
            auto result = std::make_pair(cur_->key, cur_->value);
            cur_ = cur_->links[0];
            return result;
        }

       private:
        std::shared_ptr<Node> cur_;
    };

    void InsertNode(const std::shared_ptr<Node>& node) {
        std::uniform_int_distribution<int> hit(0, 1);
        do {
            node->links.push_back(nullptr);
        } while (node->links.size() < max_level_ && hit(random_generator_));
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < node->key) {
                cur = cur->links[level - 1];
            } else {
                --level;
                if (level < node->links.size()) {
                    node->links[level] = cur->links[level];
                    cur->links[level] = node;
                }
            }
        }
        amu_ += node->key.user_key.size() + node->value.size() + node->links.size() * sizeof(Node*);
    }

   private:
    uint64_t amu_ = 0;
    uint64_t max_level_;
    std::shared_ptr<Node> head_;
    std::mt19937 random_generator_;
};

class IndexMemTableImpl : public IMemTable<IndexValue> {
   public:
    IndexMemTableImpl(uint64_t max_level) : max_level_(max_level) {
        head_ = std::make_shared<Node>();
        head_->links.resize(max_level_, nullptr);
        std::random_device device;
        random_generator_.seed(device());
    }

    void Add(const UserKey& user_key, const IndexValue& value) override {
        InternalKey key = {user_key};
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0] && cur->links[0]->key.user_key == user_key) {
            amu_ -= cur->links[0]->value.getSizeInBytes();
            cur->links[0]->value |= value;
            cur->links[0]->value.runOptimize();
            amu_ += cur->links[0]->value.getSizeInBytes();
            return;
        }
        auto node = std::make_shared<Node>();
        node->key = {user_key};
        node->value = value;
        InsertNode(node);
    }

    GetKind Get(const UserKey& user_key, IndexValue& out_value) const override {
        out_value = {};
        InternalKey key = {user_key};
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0] && cur->links[0]->key.user_key == user_key) {
            out_value = cur->links[0]->value;
            return GetKind::kFound;
        } else {
            return GetKind::kNotFound;
        }
    }

    std::optional<UserKey> LowerBound(const UserKey& user_key, IndexValue& out_value) const override {
        out_value = {};
        InternalKey key = {user_key};
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0]) {
            out_value = cur->links[0]->value;
            return cur->links[0]->key.user_key;
        } else {
            return std::nullopt;
        }
    }

    std::optional<UserKey> UpperBound(const UserKey& user_key, IndexValue& out_value) const override {
        out_value = {};
        InternalKey key = {user_key};
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key <= key) {
                cur = cur->links[level - 1];
            } else {
                --level;
            }
        }
        if (cur->links[0]) {
            out_value = cur->links[0]->value;
            return cur->links[0]->key.user_key;
        } else {
            return std::nullopt;
        }
    }

    std::shared_ptr<IStream<std::pair<InternalKey, IndexValue>>> MakeScan() const override { return std::make_shared<MemTableStream>(head_); }

    uint64_t ApproximateMemoryUsage() const override { return amu_; }

    virtual ~IndexMemTableImpl() = default;

   private:
    struct Node {
        InternalKey key;
        IndexValue value;
        std::vector<std::shared_ptr<Node>> links;
    };

    class MemTableStream : public IStream<std::pair<InternalKey, IndexValue>> {
       public:
        MemTableStream(std::shared_ptr<Node> head) : cur_(head->links[0]) {}

        std::optional<std::pair<InternalKey, IndexValue>> Next() {
            if (!cur_) {
                return std::nullopt;
            }
            auto result = std::make_pair(cur_->key, cur_->value);
            cur_ = cur_->links[0];
            return result;
        }

       private:
        std::shared_ptr<Node> cur_;
    };

    void InsertNode(const std::shared_ptr<Node>& node) {
        std::uniform_int_distribution<int> hit(0, 1);
        do {
            node->links.push_back(nullptr);
        } while (node->links.size() < max_level_ && hit(random_generator_));
        uint64_t level = max_level_;
        std::shared_ptr<Node> cur = head_;
        while (level) {
            if (cur->links[level - 1] && cur->links[level - 1]->key < node->key) {
                cur = cur->links[level - 1];
            } else {
                --level;
                if (level < node->links.size()) {
                    node->links[level] = cur->links[level];
                    cur->links[level] = node;
                }
            }
        }
        amu_ += node->key.user_key.size() + node->value.getSizeInBytes() + node->links.size() * sizeof(Node*);
    }

   private:
    uint64_t amu_ = 0;
    uint64_t max_level_;
    std::shared_ptr<Node> head_;
    std::mt19937 random_generator_;
};

}  // namespace

std::shared_ptr<IMemTable<DefaultValue>> MakeDefaultMemTable(uint32_t max_level) { return std::make_shared<DefaultMemTableImpl>(max_level); }

std::shared_ptr<IMemTable<IndexValue>> MakeIndexMemTable(uint32_t max_level) { return std::make_shared<IndexMemTableImpl>(max_level); }

}  // namespace invindex::lsm
