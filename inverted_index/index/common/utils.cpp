#include "index/common/utils.h"

#include <cstdint>
#include <cstring>

std::vector<uint8_t> SerializeUint32(uint32_t value) {
    std::vector<uint8_t> buffer(sizeof(uint32_t));
    std::memcpy(buffer.data(), &value, sizeof(uint32_t));
    return buffer;
}

uint32_t DeserializeUint32(std::vector<uint8_t> value) {
    uint32_t buffer;
    std::memcpy(&buffer, value.data(), sizeof(uint32_t));
    return buffer;
}

std::vector<uint8_t> SerializeUint64(uint64_t value) {
    std::vector<uint8_t> buffer(sizeof(uint64_t));
    std::memcpy(buffer.data(), &value, sizeof(uint64_t));
    return buffer;
}

uint64_t DeserializeUint64(std::vector<uint8_t> value) {
    uint64_t buffer;
    std::memcpy(&buffer, value.data(), sizeof(uint64_t));
    return buffer;
}

std::vector<char> SerializeVectorUint8(std::vector<uint8_t> value) { return std::vector<char>(value.begin(), value.end()); }

std::vector<uint8_t> DeserializeVectorUint8(std::vector<char> value) { return std::vector<uint8_t>(value.begin(), value.end()); }