#pragma once

#include "yati/source/base.hpp"
#include <vector>
#include <string>
#include <memory>
#include <switch.h>

namespace sphaira::yati::container {

enum class CollectionType {
    CollectionType_NCA,
    CollectionType_NCZ,
    CollectionType_TIK,
    CollectionType_CERT,
};

struct CollectionEntry {
    // collection name within file.
    std::string name{};
    // collection offset within file.
    s64 offset{};
    // collection size within file, may be compressed size.
    s64 size{};
};

using Collections = std::vector<CollectionEntry>;

struct Base {
    using Source = source::Base;

    Base(std::shared_ptr<Source> source) : m_source{source} { }
    virtual ~Base() = default;
    virtual Result GetCollections(Collections& out) = 0;
    auto GetSource() const {
        return m_source;
    }

protected:
    std::shared_ptr<Source> m_source;
};

} // namespace sphaira::yati::container
