#pragma once

#include "base.hpp"
#include <switch.h>
#include <span>

namespace sphaira::yati::container {

struct Nsp final : Base {
    using Base::Base;
    Result GetCollections(Collections& out) override;

    // builds nsp meta data and the size of the entier nsp.
    static auto Build(std::span<CollectionEntry> collections, s64& size) -> std::vector<u8>;
};

} // namespace sphaira::yati::container
