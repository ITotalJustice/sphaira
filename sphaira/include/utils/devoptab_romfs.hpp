#pragma once

#include "yati/source/base.hpp"
#include <memory>
#include <span>
#include <vector>
#include <string_view>
#include <sys/stat.h>

namespace sphaira::devoptab::romfs {

struct RomfsCollection {
    romfs_header header;
    std::vector<u8> dir_table;
    std::vector<u8> file_table;
    u64 offset;
};

struct FileEntry {
    const romfs_file* romfs;
    u64 offset;
    u64 size;
};

struct DirEntry {
    const RomfsCollection* romfs_collection;
    const romfs_dir* romfs_root; // start of the dir.
    u32 romfs_childDir;
    u32 romfs_childFile;
};

bool find_file(const RomfsCollection& romfs, std::string_view path, FileEntry& out);
bool find_dir(const RomfsCollection& romfs, std::string_view path, DirEntry& out);

// helper
void dirreset(DirEntry& entry);
bool dirnext(DirEntry& entry, char* filename, struct stat *filestat);

Result LoadRomfsCollection(yati::source::Base* source, u64 offset, RomfsCollection& out);

} // namespace sphaira::devoptab::romfs
