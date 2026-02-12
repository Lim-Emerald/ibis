#pragma once

#include <lsm/storage/buffer_pool.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace lsm::storage {

class IFile {
   public:
    virtual std::vector<uint8_t> Read(uint64_t offset, uint64_t bytes) const = 0;
    virtual void Write(const uint8_t* data, uint64_t size) = 0;
    virtual uint64_t Size() const = 0;

    virtual ~IFile() = default;
};

class BufferedMemoryFile : public IFile {
   public:
    BufferedMemoryFile(const std::string& dir, uint64_t table_id, const std::shared_ptr<IReadBufferPool>& buffer_pool) : table_id_(table_id), buffer_pool_(buffer_pool), dir_(dir) {}

    std::vector<uint8_t> Read(uint64_t offset, uint64_t bytes) const override {
        if (offset + bytes > Size()) {
            throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": failed to read " + std::to_string(bytes) + " bytes from offset " + std::to_string(offset) + " (file size is " +
                                     std::to_string(size_) + ")");
        }
        std::vector<uint8_t> result(bytes);
        uint64_t l = offset / kFrameSize, r = (offset + bytes - 1) / kFrameSize;
        auto frames = buffer_pool_->GetFrames(table_id_, l, r);
        if (l == r) {
            std::memcpy(result.data(), frames[0]->Data() + offset % kFrameSize, bytes);
        } else {
            uint64_t ost = offset % kFrameSize;
            uint64_t iost = kFrameSize - ost;
            std::memcpy(result.data(), frames[0]->Data() + ost, iost);
            for (uint64_t ind = l + 1; ind < r; ++ind) {
                std::memcpy(result.data() + iost + (ind - l - 1) * kFrameSize, frames[ind - l]->Data(), kFrameSize);
            }
            std::memcpy(result.data() + iost + (r - l - 1) * kFrameSize, frames[r - l]->Data(), bytes - iost - (r - l - 1) * kFrameSize);
        }
        return result;
    }

    void Write(const uint8_t* data, uint64_t size) override {
        std::ofstream file(dir_ + "/sstable_" + std::to_string(table_id_), std::ios::trunc | std::ios::binary);
        file.write(reinterpret_cast<const char*>(data), size);
        file.close();
        size_ = size;
    }

    uint64_t Size() const override { return size_; }

    ~BufferedMemoryFile() { std::filesystem::remove(dir_ + "/sstable_" + std::to_string(table_id_)); }

   private:
    uint64_t size_ = 0;
    uint64_t table_id_;
    std::shared_ptr<IReadBufferPool> buffer_pool_;
    std::string dir_;
};

class MemoryFile : public IFile {
   public:
    MemoryFile(const std::string& path) : path_(path) {}

    std::vector<uint8_t> Read(uint64_t offset, uint64_t bytes) const override {
        if (offset + bytes > Size()) {
            throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": failed to read " + std::to_string(bytes) + " bytes from offset " + std::to_string(offset) + " (file size is " +
                                     std::to_string(size_) + ")");
        }
        std::vector<uint8_t> result(bytes);
        std::ifstream file(path_, std::ios::binary);
        file.seekg(offset);
        file.read(reinterpret_cast<char*>(result.data()), result.size());
        file.close();
        return result;
    }

    void Write(const uint8_t* data, uint64_t size) override {
        std::ofstream file(path_, std::ios::trunc | std::ios::binary);
        file.write(reinterpret_cast<const char*>(data), size);
        file.close();
        size_ = size;
    }

    uint64_t Size() const override { return size_; }

    ~MemoryFile() { std::filesystem::remove(path_); }

   private:
    uint64_t size_ = 0;
    std::string path_;
};

class TestMemoryFile : public IFile {
   public:
    std::vector<uint8_t> Read(uint64_t offset, uint64_t bytes) const override {
        if (offset + bytes > storage_.size()) {
            throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": failed to read " + std::to_string(bytes) + " bytes from offset " + std::to_string(offset) + " (file size is " +
                                     std::to_string(storage_.size()) + ")");
        }
        std::vector<uint8_t> result(bytes);
        std::memcpy(result.data(), storage_.data() + offset, bytes);
        return result;
    }

    void Write(const uint8_t* data, uint64_t size) override {
        if (storage_.size() < size) {
            storage_.resize(size);
        }
        std::memcpy(storage_.data(), data, size);
    }

    uint64_t Size() const override { return storage_.size(); }

   private:
    std::vector<uint8_t> storage_;
};

}  // namespace lsm::storage
