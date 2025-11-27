#pragma once

#include "ui/types.hpp"

namespace sphaira::utils {

struct HashStr {
    char str[0x21];
};

HashStr hexIdToStr(FsRightsId id);
HashStr hexIdToStr(NcmRightsId id);
HashStr hexIdToStr(NcmContentId id);

template<typename T>
constexpr inline T AlignUp(T value, T align) {
    return (value + (align - 1)) &~ (align - 1);
}

template<typename T>
constexpr inline T AlignDown(T value, T align) {
    return value &~ (align - 1);
}

// formats size to 1.23 MB in 1024 base.
// only uses 32 bytes so its SSO optimised, not need to cache.
std::string formatSizeStorage(u64 size);

// formats size to 1.23 MB in 1000 base (used for progress bars).
std::string formatSizeNetwork(u64 size);

} // namespace sphaira::utils
