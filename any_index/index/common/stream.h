#pragma once

#include <optional>

namespace invindex {

template <typename T>
class IStream {
   public:
    virtual std::optional<T> Next() = 0;

    virtual ~IStream() = default;
};

}  // namespace invindex
