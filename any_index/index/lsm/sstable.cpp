#include "index/common/types.h"
#include "index/lsm/sstable.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

namespace invindex::lsm {

namespace {

class IndexSSTableBuilder final : public ISSTableBuilder {
   public:
    explicit IndexSSTableBuilder(std::shared_ptr<storage::IFile> file) : file_(file) {}

    void Add(const UserKey& k, const Value& v) override { objects_.push_back({k, v}); }

    void Finish() override {
        uint64_t mem = (2 * objects_.size() + 1) * sizeof(uint64_t);
        for (auto& object : objects_) {
            mem += object.first->GetSerializedSize() + object.second->GetSerializedSize();
        }
        std::vector<char> buffer_file(mem);

        std::vector<char> header((2 * objects_.size() + 1) * sizeof(uint64_t));
        size_t object_count = objects_.size();
        std::memcpy(&header[0], &object_count, sizeof(object_count));
        uint64_t shift = 0;
        for (size_t ind = 0; ind < object_count; ++ind) {
            auto object = objects_[ind];

            shift += object.second->GetSerializedSize();
            std::memcpy(&header[(2 * ind + 2) * sizeof(uint64_t)], &shift, sizeof(shift));
            {
                auto buffer = object.second->Serialize();
                std::memcpy(buffer_file.data() + buffer_file.size() - shift, buffer.data(), buffer.size());
            }

            shift += object.first->GetSerializedSize();
            {
                auto buffer = object.first->Serialize();
                std::memcpy(buffer_file.data() + buffer_file.size() - shift, buffer.data(), buffer.size());
            }

            std::memcpy(&header[(2 * ind + 1) * sizeof(uint64_t)], &shift, sizeof(shift));
        }
        std::memcpy(buffer_file.data(), header.data(), header.size());
        file_->Write(buffer_file.data(), buffer_file.size());
    }

   private:
    std::vector<std::pair<UserKey, Value>> objects_;
    std::shared_ptr<storage::IFile> file_;
};

class IndexSSTableReader final : public ISSTableReader {
   public:
    explicit IndexSSTableReader(std::shared_ptr<const storage::IFile> file, std::pair<UserKey, Value> example) : example_(example), page_(std::make_shared<const SSTableViewer>(std::move(file))) {}

    std::shared_ptr<IStream<std::pair<UserKey, Value>>> MakeScan() const override { return std::make_shared<SSTableStream>(page_, example_); }

    GetKind Get(const UserKey& user_key, Value& out_value) const override {
        std::pair<UserKey, Value> temp = example_;
        size_t l = 0, r = page_->GetObjectCount() + 1;
        while (r - l > 1) {
            size_t m = (l + r) / 2;
            // std::cout << "GetFromBin" << std::endl;
            page_->GetObject(m - 1, temp);
            // std::cout << "GetFromBin End" << std::endl;
            if (*temp.first < user_key) {
                l = m;
            } else {
                r = m;
            }
        }

        if (r == page_->GetObjectCount() + 1) {
            return GetKind::kNotFound;
        } else {
            // std::cout << "GetFromGet" << std::endl;
            page_->GetObject(r - 1, temp);
            // std::cout << "GetFromGet End" << std::endl;
            if (*temp.first != user_key) {
                // std::cout << "Get End" << std::endl;
                return GetKind::kNotFound;
            }
            out_value = temp.second;
                // std::cout << "Get End 2" << std::endl;
            return GetKind::kFound;
        }
    }

   private:
    class SSTableViewer {
       public:
        explicit SSTableViewer(std::shared_ptr<const storage::IFile> file) : file_(std::move(file)) { std::memcpy(&object_count_, file_->Read(0, sizeof(uint64_t)).data(), sizeof(uint64_t)); }

        void GetObject(size_t ind, std::pair<UserKey, Value>& result) const {
            if (ind >= object_count_) {
                throw "SSTablePage: out of bounds";
            }
            std::pair<uint64_t, uint64_t> offsets;
            std::memcpy(&offsets.first, file_->Read((2 * ind + 1) * sizeof(uint64_t), sizeof(uint64_t)).data(), sizeof(uint64_t));
            std::memcpy(&offsets.second, file_->Read((2 * ind + 2) * sizeof(uint64_t), sizeof(uint64_t)).data(), sizeof(uint64_t));

            // std::cout << "GetObject" << std::endl;
            result.first->Deserialize(file_->Read(file_->Size() - offsets.first, offsets.first - offsets.second));

            uint64_t bytes_value = offsets.second;
            if (ind) {
                uint64_t ofs;
                std::memcpy(&ofs, file_->Read((2 * ind - 1) * sizeof(uint64_t), sizeof(uint64_t)).data(), sizeof(uint64_t));
                bytes_value -= ofs;
            }
            result.second->Deserialize(file_->Read(file_->Size() - offsets.second, bytes_value));
            // std::cout << "GetObject fin" << std::endl;
        }

        size_t GetObjectCount() const { return object_count_; }

       private:
        uint64_t object_count_;
        std::shared_ptr<const storage::IFile> file_;
    };

    class SSTableStream : public IStream<std::pair<UserKey, Value>> {
       public:
        SSTableStream(std::shared_ptr<const SSTableViewer> page, std::pair<UserKey, Value> example) : page_(page), example_(example) {}

        std::optional<std::pair<UserKey, Value>> Next() {
            if (ind_ == page_->GetObjectCount()) {
                return std::nullopt;
            }
            // std::cout << "GetFromStream" << std::endl;
            page_->GetObject(ind_++, example_);
            // std::cout << "GetFromStream End" << std::endl;
            return example_;
        }

       private:
        size_t ind_ = 0;
        std::shared_ptr<const SSTableViewer> page_;
        std::pair<UserKey, Value> example_;
    };

   private:
    std::pair<UserKey, Value> example_;
    std::shared_ptr<const SSTableViewer> page_;
};

class IndexSSTableFactory final : public ISSTableSerializer {
   public:
    IndexSSTableFactory(std::pair<UserKey, Value> example) : example_(example) {}

    std::unique_ptr<ISSTableBuilder> NewFileBuilder(const std::shared_ptr<storage::IFile>& file) const override { return std::make_unique<IndexSSTableBuilder>(file); }
    std::shared_ptr<ISSTableReader> FromFile(const std::shared_ptr<const storage::IFile>& file) const override { return std::make_shared<IndexSSTableReader>(file, example_); }

private:
    std::pair<UserKey, Value> example_;
};

}  // namespace

std::shared_ptr<ISSTableSerializer> MakeIndexSSTableFileFactory(std::pair<UserKey, Value> example) { return std::make_shared<IndexSSTableFactory>(example); }

}  // namespace invindex::lsm
