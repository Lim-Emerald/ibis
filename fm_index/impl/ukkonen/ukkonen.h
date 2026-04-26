#pragma once

#include <string>
#include <vector>

namespace fm_index::ukkonen {

std::vector<size_t> MakeSuffixArray(const std::string& s);

};  // namespace fm_index::ukkonen
