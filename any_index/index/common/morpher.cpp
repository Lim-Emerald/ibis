#include "index/common/morpher.h"

#include <fstream>

namespace invindex {

std::shared_ptr<RussianMorpher> MakeRussianMorpher(const std::string& rus_dict) {
    std::ifstream rus_file(rus_dict);
    auto russian_morpher = std::make_shared<RussianMorpher>();
    russian_morpher->Deserialize(rus_file);
    return russian_morpher;
}

}  // namespace invindex