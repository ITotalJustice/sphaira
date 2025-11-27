#pragma once

#include <string>
#include <vector>
#include <switch.h>
// to import FsEntryFlags.
// todo: this should be part of a smaller header, such as filesystem_types.hpp
#include "ui/menus/filebrowser.hpp"

namespace sphaira::location {

using FsEntryFlag = ui::menu::filebrowser::FsEntryFlag;

// helper for hdd devices.
// this doesn't really belong in this header, however
// locations likely will be renamed to something more generic soon.
struct StdioEntry {
    // mount point (ums0:)
    std::string mount{};
    // ums0: (USB Flash Disk)
    std::string name{};
    // FsEntryFlag
    u32 flags{};
    // optional dump path inside the mount point.
    std::string dump_path{};
    // set to hide for filebrowser.
    bool fs_hidden{};
    // set to hide in dump list.
    bool dump_hidden{};
};

using StdioEntries = std::vector<StdioEntry>;

// set write=true to filter out write protected devices.
auto GetStdio(bool write) -> StdioEntries;

} // namespace sphaira::location
