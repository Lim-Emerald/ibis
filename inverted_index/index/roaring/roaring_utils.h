#pragma once

#include <vector>

#include "index/roaring/roaring.hh"

namespace roaring {

std::vector<char> Serialize(const Roaring64Map& roaring_map) {
    std::vector<char> buffer(roaring_map.getSizeInBytes());
    roaring_map.write(buffer.data());
    return buffer;
}

Roaring64Map Deserialize(const std::vector<char>& buffer) {
    return Roaring64Map::readSafe(buffer.data(), buffer.size());
}

}  // namespace roaring