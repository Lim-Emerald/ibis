#pragma once

#include <optional>

namespace lsm {

template <typename T>
class IStream {
   public:
    virtual std::optional<T> Next() = 0;

    virtual ~IStream() = default;
};

}  // namespace lsm
