#pragma once

#include <cstdint>
#include <string>
#include <vector>

uint32_t GetHash(const std::string& value);

std::vector<uint8_t> SerializeUint32(uint32_t value);

uint32_t DeserializeUint32(std::vector<uint8_t> value);

std::vector<uint8_t> SerializeUint64(uint64_t value);

uint64_t DeserializeUint64(std::vector<uint8_t> value);

std::vector<char> SerializeVectorUint8(std::vector<uint8_t> value);

std::vector<uint8_t> DeserializeVectorUint8(std::vector<char> value);
