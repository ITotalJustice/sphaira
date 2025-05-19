#pragma once

#include <string>
#include <vector>
#include <switch.h>

namespace sphaira::location {

struct Entry {
    std::string name{};
    std::string url{};
    std::string user{};
    std::string pass{};
    std::string bearer{};
    std::string pub_key{};
    std::string priv_key{};
    u16 port{};
};
using Entries = std::vector<Entry>;

auto Load() -> Entries;
void Add(const Entry& e);

} // namespace sphaira::location
