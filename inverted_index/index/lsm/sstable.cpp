#include "index/lsm/sstable.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <vector>

#include "index/common/types.h"
#include "index/common/utils.h"
#include "index/roaring/roaring_utils.h"

namespace invindex::lsm {

namespace {

class DefaultSSTableBuilder final : public ISSTableBuilder<DefaultValue> {
   public:
    explicit DefaultSSTableBuilder(std::shared_ptr<storage::IFile> file) : file_(file) {}

    void Add(const InternalKey& k, const DefaultValue& v) override { objects_.push_back({k, v}); }

    void Finish() override {
        uint64_t mem = (2 * objects_.size() + 1) * sizeof(uint64_t);
        for (auto& object : objects_) {
            mem += object.first.user_key.size() + object.second.size();
        }
        std::vector<char> buffer_file(mem);

        std::vector<char> header((2 * objects_.size() + 1) * sizeof(uint64_t) / sizeof(uint8_t));
        size_t object_count = objects_.size();
        std::memcpy(&header[0], &object_count, sizeof(object_count));
        uint64_t shift = 0;
        for (size_t ind = 0; ind < object_count; ++ind) {
            auto object = objects_[ind];

            shift += object.second.size();
            std::memcpy(&header[(2 * ind + 2) * sizeof(uint64_t)], &shift, sizeof(shift));
            if (!object.second.empty()) {
                std::memcpy(buffer_file.data() + buffer_file.size() - shift, object.second.data(), object.second.size());
            }

            shift += object.first.user_key.size();
            std::memcpy(buffer_file.data() + buffer_file.size() - shift, object.first.user_key.data(), object.first.user_key.size());

            std::memcpy(&header[(2 * ind + 1) * sizeof(uint64_t)], &shift, sizeof(shift));
        }
        std::memcpy(buffer_file.data(), header.data(), header.size());
        file_->Write(buffer_file.data(), buffer_file.size());
    }

   private:
    std::vector<std::pair<InternalKey, DefaultValue>> objects_;
    std::shared_ptr<storage::IFile> file_;
};

class DefaultSSTableReader final : public ISSTableReader<DefaultValue> {
   public:
    explicit DefaultSSTableReader(std::shared_ptr<const storage::IFile> file) : page_(std::make_shared<const SSTableViewer>(std::move(file))) {}

    std::shared_ptr<IStream<std::pair<InternalKey, DefaultValue>>> MakeScan() const override { return std::make_shared<SSTableStream>(page_); }

    GetKind Get(const UserKey& user_key, DefaultValue& out_value) const override {
        out_value = {};
        InternalKey internal_key = {user_key};
        size_t l = 0, r = page_->GetObjectCount() + 1;
        while (r - l > 1) {
            size_t m = (l + r) / 2;
            if (page_->GetObject(m - 1).first < internal_key) {
                l = m;
            } else {
                r = m;
            }
        }

        if (r == page_->GetObjectCount() + 1 || page_->GetObject(r - 1).first.user_key != user_key) {
            return GetKind::kNotFound;
        } else {
            out_value = page_->GetObject(r - 1).second;
            return GetKind::kFound;
        }
    }

    std::optional<UserKey> LowerBound(const UserKey& user_key, DefaultValue& out_value) const override {
        out_value = {};
        InternalKey internal_key = {user_key};
        size_t l = 0, r = page_->GetObjectCount() + 1;
        while (r - l > 1) {
            size_t m = (l + r) / 2;
            if (page_->GetObject(m - 1).first < internal_key) {
                l = m;
            } else {
                r = m;
            }
        }

        if (r == page_->GetObjectCount() + 1) {
            return std::nullopt;
        } else {
            out_value = page_->GetObject(r - 1).second;
            return page_->GetObject(r - 1).first.user_key;
        }
    }

    std::optional<UserKey> UpperBound(const UserKey& user_key, DefaultValue& out_value) const override {
        out_value = {};
        InternalKey internal_key = {user_key};
        size_t l = 0, r = page_->GetObjectCount() + 1;
        while (r - l > 1) {
            size_t m = (l + r) / 2;
            if (page_->GetObject(m - 1).first <= internal_key) {
                l = m;
            } else {
                r = m;
            }
        }

        if (r == page_->GetObjectCount() + 1) {
            return std::nullopt;
        } else {
            out_value = page_->GetObject(r - 1).second;
            return page_->GetObject(r - 1).first.user_key;
        }
    }

   private:
    class SSTableViewer {
       public:
        explicit SSTableViewer(std::shared_ptr<const storage::IFile> file) : file_(std::move(file)) { std::memcpy(&object_count_, file_->Read(0, sizeof(uint64_t)).data(), sizeof(uint64_t)); }

        std::pair<InternalKey, DefaultValue> GetObject(size_t ind) const {
            if (ind >= object_count_) {
                throw "SSTablePage: out of bounds";
            }
            std::pair<uint64_t, uint64_t> offsets;
            std::memcpy(&offsets.first, file_->Read((2 * ind + 1) * sizeof(uint64_t), sizeof(uint64_t)).data(), sizeof(uint64_t));
            std::memcpy(&offsets.second, file_->Read((2 * ind + 2) * sizeof(uint64_t), sizeof(uint64_t)).data(), sizeof(uint64_t));

            std::pair<InternalKey, DefaultValue> object;
            object.first.user_key = DeserializeVectorUint8(file_->Read(file_->Size() - offsets.first, offsets.first - offsets.second));

            uint64_t bytes_value = offsets.second;
            if (ind) {
                uint64_t ofs;
                std::memcpy(&ofs, file_->Read((2 * ind - 1) * sizeof(uint64_t), sizeof(uint64_t)).data(), sizeof(uint64_t));
                bytes_value -= ofs;
            }
            object.second = DeserializeVectorUint8(file_->Read(file_->Size() - offsets.second, bytes_value));
            return object;
        }

        size_t GetObjectCount() const { return object_count_; }

       private:
        uint64_t object_count_;
        std::shared_ptr<const storage::IFile> file_;
    };

    class SSTableStream : public IStream<std::pair<InternalKey, DefaultValue>> {
       public:
        SSTableStream(std::shared_ptr<const SSTableViewer> page) : page_(page) {}

        std::optional<std::pair<InternalKey, DefaultValue>> Next() {
            if (ind_ == page_->GetObjectCount()) {
                return std::nullopt;
            }
            return page_->GetObject(ind_++);
        }

       private:
        size_t ind_ = 0;
        std::shared_ptr<const SSTableViewer> page_;
    };

   private:
    std::shared_ptr<const SSTableViewer> page_;
};

class DefaultSSTableFactory final : public ISSTableSerializer<DefaultValue> {
   public:
    std::unique_ptr<ISSTableBuilder<DefaultValue>> NewFileBuilder(const std::shared_ptr<storage::IFile>& file) const override { return std::make_unique<DefaultSSTableBuilder>(file); }
    std::shared_ptr<ISSTableReader<DefaultValue>> FromFile(const std::shared_ptr<const storage::IFile>& file) const override { return std::make_shared<DefaultSSTableReader>(file); }
};

class IndexSSTableBuilder final : public ISSTableBuilder<IndexValue> {
   public:
    explicit IndexSSTableBuilder(std::shared_ptr<storage::IFile> file) : file_(file) {}

    void Add(const InternalKey& k, const IndexValue& v) override { objects_.push_back({k, v}); }

    void Finish() override {
        uint64_t mem = (2 * objects_.size() + 1) * sizeof(uint64_t);
        for (auto& object : objects_) {
            mem += object.first.Size() + object.second.getSizeInBytes();
        }
        std::vector<char> buffer_file(mem);

        std::vector<char> header((2 * objects_.size() + 1) * sizeof(uint64_t));
        size_t object_count = objects_.size();
        std::memcpy(&header[0], &object_count, sizeof(object_count));
        uint64_t shift = 0;
        for (size_t ind = 0; ind < object_count; ++ind) {
            auto object = objects_[ind];

            shift += object.second.getSizeInBytes();
            std::memcpy(&header[(2 * ind + 2) * sizeof(uint64_t)], &shift, sizeof(shift));
            if (!object.second.isEmpty()) {
                auto buffer = roaring::Serialize(object.second);
                std::memcpy(buffer_file.data() + buffer_file.size() - shift, buffer.data(), buffer.size());
            }

            shift += object.first.user_key.size();
            std::memcpy(buffer_file.data() + buffer_file.size() - shift, object.first.user_key.data(), object.first.user_key.size());

            std::memcpy(&header[(2 * ind + 1) * sizeof(uint64_t)], &shift, sizeof(shift));
        }
        std::memcpy(buffer_file.data(), header.data(), header.size());
        file_->Write(buffer_file.data(), buffer_file.size());
    }

   private:
    std::vector<std::pair<InternalKey, IndexValue>> objects_;
    std::shared_ptr<storage::IFile> file_;
};

class IndexSSTableReader final : public ISSTableReader<IndexValue> {
   public:
    explicit IndexSSTableReader(std::shared_ptr<const storage::IFile> file) : page_(std::make_shared<const SSTableViewer>(std::move(file))) {}

    std::shared_ptr<IStream<std::pair<InternalKey, IndexValue>>> MakeScan() const override { return std::make_shared<SSTableStream>(page_); }

    GetKind Get(const UserKey& user_key, IndexValue& out_value) const override {
        out_value = {};
        InternalKey internal_key = {user_key};
        size_t l = 0, r = page_->GetObjectCount() + 1;
        while (r - l > 1) {
            size_t m = (l + r) / 2;
            if (page_->GetObject(m - 1).first < internal_key) {
                l = m;
            } else {
                r = m;
            }
        }

        if (r == page_->GetObjectCount() + 1 || page_->GetObject(r - 1).first.user_key != user_key) {
            return GetKind::kNotFound;
        } else {
            out_value = page_->GetObject(r - 1).second;
            return GetKind::kFound;
        }
    }

    std::optional<UserKey> LowerBound(const UserKey& user_key, IndexValue& out_value) const override {
        out_value = {};
        InternalKey internal_key = {user_key};
        size_t l = 0, r = page_->GetObjectCount() + 1;
        while (r - l > 1) {
            size_t m = (l + r) / 2;
            if (page_->GetObject(m - 1).first < internal_key) {
                l = m;
            } else {
                r = m;
            }
        }

        if (r == page_->GetObjectCount() + 1) {
            return std::nullopt;
        } else {
            out_value = page_->GetObject(r - 1).second;
            return page_->GetObject(r - 1).first.user_key;
        }
    }

    std::optional<UserKey> UpperBound(const UserKey& user_key, IndexValue& out_value) const override {
        out_value = {};
        InternalKey internal_key = {user_key};
        size_t l = 0, r = page_->GetObjectCount() + 1;
        while (r - l > 1) {
            size_t m = (l + r) / 2;
            if (page_->GetObject(m - 1).first <= internal_key) {
                l = m;
            } else {
                r = m;
            }
        }

        if (r == page_->GetObjectCount() + 1) {
            return std::nullopt;
        } else {
            out_value = page_->GetObject(r - 1).second;
            return page_->GetObject(r - 1).first.user_key;
        }
    }

   private:
    class SSTableViewer {
       public:
        explicit SSTableViewer(std::shared_ptr<const storage::IFile> file) : file_(std::move(file)) { std::memcpy(&object_count_, file_->Read(0, sizeof(uint64_t)).data(), sizeof(uint64_t)); }

        std::pair<InternalKey, IndexValue> GetObject(size_t ind) const {
            if (ind >= object_count_) {
                throw "SSTablePage: out of bounds";
            }
            std::pair<uint64_t, uint64_t> offsets;
            std::memcpy(&offsets.first, file_->Read((2 * ind + 1) * sizeof(uint64_t), sizeof(uint64_t)).data(), sizeof(uint64_t));
            std::memcpy(&offsets.second, file_->Read((2 * ind + 2) * sizeof(uint64_t), sizeof(uint64_t)).data(), sizeof(uint64_t));

            std::pair<InternalKey, IndexValue> object;

            object.first.user_key = DeserializeVectorUint8(file_->Read(file_->Size() - offsets.first, offsets.first - offsets.second));

            uint64_t bytes_value = offsets.second;
            if (ind) {
                uint64_t ofs;
                std::memcpy(&ofs, file_->Read((2 * ind - 1) * sizeof(uint64_t), sizeof(uint64_t)).data(), sizeof(uint64_t));
                bytes_value -= ofs;
            }
            object.second = roaring::Deserialize(file_->Read(file_->Size() - offsets.second, bytes_value));
            return object;
        }

        size_t GetObjectCount() const { return object_count_; }

       private:
        uint64_t object_count_;
        std::shared_ptr<const storage::IFile> file_;
    };

    class SSTableStream : public IStream<std::pair<InternalKey, IndexValue>> {
       public:
        SSTableStream(std::shared_ptr<const SSTableViewer> page) : page_(page) {}

        std::optional<std::pair<InternalKey, IndexValue>> Next() {
            if (ind_ == page_->GetObjectCount()) {
                return std::nullopt;
            }
            return page_->GetObject(ind_++);
        }

       private:
        size_t ind_ = 0;
        std::shared_ptr<const SSTableViewer> page_;
    };

   private:
    std::shared_ptr<const SSTableViewer> page_;
};

class IndexSSTableFactory final : public ISSTableSerializer<IndexValue> {
   public:
    std::unique_ptr<ISSTableBuilder<IndexValue>> NewFileBuilder(const std::shared_ptr<storage::IFile>& file) const override { return std::make_unique<IndexSSTableBuilder>(file); }
    std::shared_ptr<ISSTableReader<IndexValue>> FromFile(const std::shared_ptr<const storage::IFile>& file) const override { return std::make_shared<IndexSSTableReader>(file); }
};

}  // namespace

std::shared_ptr<ISSTableSerializer<DefaultValue>> MakeDefaultSSTableFileFactory() { return std::make_shared<DefaultSSTableFactory>(); }

std::shared_ptr<ISSTableSerializer<IndexValue>> MakeIndexSSTableFileFactory() { return std::make_shared<IndexSSTableFactory>(); }

}  // namespace invindex::lsm
