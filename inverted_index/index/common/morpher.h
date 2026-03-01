#pragma once

#include <memory>

#include <strutext/morpho/alphabets/eng_alphabet.h>
#include <strutext/morpho/alphabets/rus_alphabet.h>
#include <strutext/morpho/morpholib/morpho.h>

namespace invindex {

using RussianMorpher = strutext::morpho::Morphologist<strutext::morpho::RussianAlphabet>;
using EnglishMorpher = strutext::morpho::Morphologist<strutext::morpho::EnglishAlphabet>;

std::shared_ptr<RussianMorpher> MakeRussianMorpher(const std::string& rus_dict);

}  // namespace invindex
