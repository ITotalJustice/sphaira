#include "utils/devoptab_romfs.hpp"
#include "defines.hpp"
#include "log.hpp"

#include <cstring>
#include <algorithm>
#include <sys/stat.h>
#include <sys/syslimits.h>

namespace sphaira::devoptab::romfs {
namespace {

auto find_romfs_relative_dir(const RomfsCollection& romfs, std::string_view path) -> const romfs_dir* {
    if (path.starts_with('/')) {
        path = path.substr(1);
    }

    const auto root = (const romfs_dir*)romfs.dir_table.data();
    const auto rel_index = path.find_last_of('/');
    path = path.substr(0, rel_index);

    if (rel_index == path.npos) {
        return root;
    }

    u32 childDir = root->childDir;
    while (path.length() && (childDir != ~0)) {
        const auto sub = path.substr(0, path.find_first_of('/'));
        const auto dir = (const romfs_dir*)(romfs.dir_table.data() + childDir);

        if (dir->nameLen == sub.length() && !std::memcmp(sub.data(), dir->name, dir->nameLen)) {
            if (path == sub) {
                return dir; // bingo
            } else {
                childDir = dir->childDir;
                path = path.substr(sub.length() + 1);
            }
        } else {
            childDir = dir->sibling;
        }
    }

    return nullptr;
}

auto find_romfs_dir(const romfs_dir* parent, const RomfsCollection& romfs, std::string_view path) -> const romfs_dir* {
    if (path.starts_with('/')) {
        path = path.substr(1);
    }

    if (!path.length()) {
        return parent;
    }

    if (auto idx = path.find_last_of('/'); idx != path.npos) {
        path = path.substr(idx + 1);
    }

    u32 childDir = parent->childDir;
    while (path.length() && (childDir != ~0)) {
        const auto dir = (const romfs_dir*)(romfs.dir_table.data() + childDir);

        if (dir->nameLen == path.length() && !std::memcmp(path.data(), dir->name, dir->nameLen)) {
            return dir; // bingo
        } else {
            childDir = dir->sibling;
        }
    }

    return nullptr;
}

auto find_romfs_file(const romfs_dir* parent, const RomfsCollection& romfs, std::string_view path) -> const romfs_file* {
    if (path.starts_with('/')) {
        path = path.substr(1);
    }

    if (auto idx = path.find_last_of('/'); idx != path.npos) {
        path = path.substr(idx + 1);
    }

    u32 childFile = parent->childFile;
    while (path.length() && (childFile != ~0)) {
        const auto file = (const romfs_file*)(romfs.file_table.data() + childFile);

        if (file->nameLen == path.length() && !std::memcmp(path.data(), file->name, file->nameLen)) {
            return file; // bingo
        } else {
            childFile = file->sibling;
        }
    }

    return nullptr;
}

} // namespace

bool find_file(const RomfsCollection& romfs, std::string_view path, FileEntry& out) {
    const auto parent = find_romfs_relative_dir(romfs, path);
    if (!parent) {
        return false;
    }

    out.romfs = find_romfs_file(parent, romfs, path);
    if (!out.romfs) {
        return false;
    }

    out.offset = romfs.offset + romfs.header.fileDataOff + out.romfs->dataOff;
    out.size = out.romfs->dataSize;
    return true;
}

bool find_dir(const RomfsCollection& romfs, std::string_view path, DirEntry& out) {
    const auto parent = find_romfs_relative_dir(romfs, path);
    if (!parent) {
        return false;
    }

    out.romfs_root = find_romfs_dir(parent, romfs, path);
    if (!out.romfs_root) {
        return false;
    }

    out.romfs_collection = &romfs;
    out.romfs_childDir = out.romfs_root->childDir;
    out.romfs_childFile = out.romfs_root->childFile;
    return true;
}

void dirreset(DirEntry& entry) {
    entry.romfs_childDir = entry.romfs_root->childDir;
    entry.romfs_childFile = entry.romfs_root->childFile;
}

bool dirnext(DirEntry& entry, char* filename, struct stat *filestat) {
    if (entry.romfs_childDir != ~0) {
        auto d = (const romfs_dir*)(entry.romfs_collection->dir_table.data() + entry.romfs_childDir);
        entry.romfs_childDir = d->sibling;

        filestat->st_nlink = 1;
        filestat->st_mode = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH;
        const auto name_len = std::min<u32>(d->nameLen + 1, NAME_MAX);
        std::snprintf(filename, name_len, "%s", (const char*)d->name);
        return true;
    } else if (entry.romfs_childFile != ~0) {
        auto d = (const romfs_file*)(entry.romfs_collection->file_table.data() + entry.romfs_childFile);
        entry.romfs_childFile = d->sibling;

        filestat->st_nlink = 1;
        filestat->st_size = d->dataSize;
        filestat->st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        const auto name_len = std::min<u32>(d->nameLen + 1, NAME_MAX);
        std::snprintf(filename, name_len, "%s", (const char*)d->name);
        return true;
    }

    return false;
}

Result LoadRomfsCollection(yati::source::Base* source, u64 offset, RomfsCollection& out) {
    out.offset = offset;

    // log_write("is valid romfs maybe: %zu\n", hash_data.info_level_hash.levels[5].logical_offset);
    R_TRY(source->Read2(&out.header, out.offset, sizeof(out.header)));

    log_write("[RomFS] headerSize: %zu\n", out.header.headerSize);
    log_write("[RomFS] dirHashTableOff: %zu\n", out.header.dirHashTableOff);
    log_write("[RomFS] dirHashTableSize: %zu\n", out.header.dirHashTableSize);
    log_write("[RomFS] dirTableOff: %zu\n", out.header.dirTableOff);
    log_write("[RomFS] dirTableSize: %zu\n", out.header.dirTableSize);
    log_write("[RomFS] fileHashTableOff: %zu\n", out.header.fileHashTableOff);
    log_write("[RomFS] fileHashTableSize: %zu\n", out.header.fileHashTableSize);
    log_write("[RomFS] fileTableOff: %zu\n", out.header.fileTableOff);
    log_write("[RomFS] fileTableSize: %zu\n", out.header.fileTableSize);
    log_write("[RomFS] fileDataOff: %zu\n", out.header.fileDataOff);
    R_UNLESS(out.header.headerSize == sizeof(out.header), 0x8);

    out.dir_table.resize(out.header.dirTableSize);
    R_TRY(source->Read2(out.dir_table.data(), out.offset + out.header.dirTableOff, out.dir_table.size()));

    log_write("romfs dir\n");

    out.file_table.resize(out.header.fileTableSize);
    R_TRY(source->Read2(out.file_table.data(), out.offset + out.header.fileTableOff, out.file_table.size()));

    log_write("read romfs file\n");

    R_SUCCEED();
}

} // sphaira::devoptab::romfs
