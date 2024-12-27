#include "nro.hpp"
#include "defines.hpp"
#include "evman.hpp"
#include "app.hpp"
#include "log.hpp"

#include <switch.h>
#include <vector>
#include <cstring>
#include <string_view>
#include <minIni.h>

namespace sphaira {
namespace {

enum {
    Module_Nro = 421,
};

enum NroError {
    NroError_BadMagic = MAKERESULT(Module_Nro, 1),
    NroError_BadSize = MAKERESULT(Module_Nro, 2),
};

struct NroData {
    NroStart start;
    NroHeader header;
};

auto nro_parse_internal(fs::FsNative& fs, const fs::FsPath& path, NroEntry& entry) -> Result {
    entry.path = path;

    // todo: special sorting for fw 2.0.0 to make it not look like shit
    if (hosversionAtLeast(3,0,0)) {
        // it doesn't matter if we fail
        entry.timestamp.is_valid = false;
        fs.GetFileTimeStampRaw(entry.path, &entry.timestamp);
        // if (R_FAILED(fsFsGetFileTimeStampRaw(fs, entry.path, &entry.timestamp))) {
        //     // log_write("failed to get timestamp for: %s\n", path);
        // }
    }

    FsFile f;
    R_TRY(fs.OpenFile(entry.path, FsOpenMode_Read, &f));
    ON_SCOPE_EXIT(fsFileClose(&f));

    R_TRY(fsFileGetSize(&f, &entry.size));

    NroData data;
    u64 bytes_read;
    R_TRY(fsFileRead(&f, 0, &data, sizeof(data), FsReadOption_None, &bytes_read));
    R_UNLESS(data.header.magic == NROHEADER_MAGIC, NroError_BadMagic);

    NroAssetHeader asset;
    R_TRY(fsFileRead(&f, data.header.size, &asset, sizeof(asset), FsReadOption_None, &bytes_read));
    // R_UNLESS(asset.magic == NROASSETHEADER_MAGIC, NroError_BadMagic);

    // some .nro (vgedit) have bad nacp, fake the nacp
    if (asset.magic != NROASSETHEADER_MAGIC || asset.nacp.offset == 0 || asset.nacp.size != sizeof(entry.nacp)) {
        std::memset(&entry.nacp, 0, sizeof(entry.nacp));

        // get the name without the .nro
        const auto file_name = std::strrchr(path, '/') + 1;
        const auto file_name_len = std::strlen(file_name);
        for (auto& lang : entry.nacp.lang) {
            std::strncpy(lang.name, file_name, file_name_len - 4);
            std::strcpy(lang.author, "Unknown");
        }
        std::strcpy(entry.nacp.display_version, "Unknown");
        entry.is_nacp_valid = false;
    } else {
        R_TRY(fsFileRead(&f, data.header.size + asset.nacp.offset, &entry.nacp, sizeof(entry.nacp), FsReadOption_None, &bytes_read));
        entry.is_nacp_valid = true;
    }

    // lazy load the icons
    entry.icon_size = asset.icon.size;
    entry.icon_offset = data.header.size + asset.icon.offset;

    R_SUCCEED();
}

// this function is recursive by 1 level deep
// if the nro is in switch/folder/folder2/app.nro it will NOT be found
// switch/folder/app.nro for example will work fine.
auto nro_scan_internal(const fs::FsPath& path, std::vector<NroEntry>& nros, bool hide_sphaira, bool nested, bool scan_all_dir, bool root) -> Result {
    fs::FsNativeSd fs;
    R_TRY(fs.GetFsOpenResult());

    // we don't need to scan for folders if we are not root
    u32 dir_open_type = FsDirOpenMode_ReadFiles | FsDirOpenMode_NoFileSize;
    if (root) {
        dir_open_type |= FsDirOpenMode_ReadDirs;
    }

    FsDir d;
    R_TRY(fs.OpenDirectory(path, dir_open_type, &d));
    ON_SCOPE_EXIT(fs.DirClose(&d));

    s64 count;
    R_TRY(fs.DirGetEntryCount(&d, &count));
    // return early if empty
    R_UNLESS(count > 0, 0x0);

    // we won't run out of memory here
    std::vector<FsDirectoryEntry> entries(count);

    R_TRY(fs.DirRead(&d, &count, entries.size(), entries.data()));

    // size may of changed
    entries.resize(count);

    for (const auto& e : entries) {
        // skip hidden files / folders
        if ('.' == e.name[0]) {
            continue;
        }

        // skip self
        if (hide_sphaira && !strncmp(e.name, "sphaira", strlen("sphaira"))) {
            continue;
        }

        if (e.type == FsDirEntryType_Dir) {
            // assert(!root && "dir should only be scanned on non-root!");
            fs::FsPath fullpath;
            std::snprintf(fullpath, sizeof(fullpath), "%s/%s/%s.nro", path, e.name, e.name);

            // fast path for detecting an nro in a folder
            NroEntry entry;
            if (R_SUCCEEDED(nro_parse_internal(fs, fullpath, entry))) {
                // log_write("NRO: fast path for: %s\n", fullpath);
                nros.emplace_back(entry);
            } else {
                // slow path...
                std::snprintf(fullpath, sizeof(fullpath), "%s/%s", path, e.name);
                nro_scan_internal(fullpath, nros, hide_sphaira, nested, scan_all_dir, false);
            }
        } else if (e.type == FsDirEntryType_File && std::string_view{e.name}.ends_with(".nro")) {
            fs::FsPath fullpath;
            std::snprintf(fullpath, sizeof(fullpath), "%s/%s", path, e.name);

            NroEntry entry;
            if (R_SUCCEEDED(nro_parse_internal(fs, fullpath, entry))) {
                nros.emplace_back(entry);
                if (!root && !scan_all_dir) {
                    // log_write("NRO: slow path for: %s\n", fullpath);
                    R_SUCCEED();
                }
            } else {
                log_write("error when trying to parse %s\n", fullpath);
            }
        }
    }

    R_SUCCEED();
}

auto nro_get_icon_internal(FsFile* f, u64 size, u64 offset) -> std::vector<u8> {
    std::vector<u8> icon;
    u64 bytes_read{};
    icon.resize(size);

    R_TRY_RESULT(fsFileRead(f, offset, icon.data(), icon.size(), FsReadOption_None, &bytes_read), {});
    R_UNLESS(bytes_read == icon.size(), {});

    return icon;
}

auto launch_internal(const std::string& path, const std::string& argv) -> Result {
    R_TRY(envSetNextLoad(path.c_str(), argv.c_str()));

    log_write("set launch with path: %s argv: %s\n", path.c_str(), argv.c_str());

    evman::push(evman::LaunchNroEventData{path, argv});
    R_SUCCEED();
}

} // namespace

/*
    NRO INFO PAGE:
    - icon
    - name
    - author
    - path
    - filesize
    - launch count
    - timestamp created
    - timestamp modified
*/

auto nro_verify(std::span<const u8> data) -> Result {
    NroData nro;
    R_UNLESS(data.size() >= sizeof(nro), NroError_BadSize);

    memcpy(&nro, data.data(), sizeof(nro));
    R_UNLESS(nro.header.magic == NROHEADER_MAGIC, NroError_BadMagic);

    R_SUCCEED();
}

auto nro_parse(const fs::FsPath& path, NroEntry& entry) -> Result {
    fs::FsNativeSd fs;
    R_TRY(fs.GetFsOpenResult());

    return nro_parse_internal(fs, path, entry);
}

auto nro_scan(const fs::FsPath& path, std::vector<NroEntry>& nros, bool hide_sphaira, bool nested, bool scan_all_dir) -> Result {
    return nro_scan_internal(path, nros, hide_sphaira, nested, scan_all_dir, true);
}

auto nro_get_icon(const fs::FsPath& path, u64 size, u64 offset) -> std::vector<u8> {
    fs::FsNativeSd fs;
    FsFile f;

    R_TRY_RESULT(fs.GetFsOpenResult(), {});

    R_TRY_RESULT(fs.OpenFile(path, FsOpenMode_Read, &f), {});
    ON_SCOPE_EXIT(fsFileClose(&f));

    return nro_get_icon_internal(&f, size, offset);
}

auto nro_get_icon(const fs::FsPath& path) -> std::vector<u8> {
    fs::FsNativeSd fs;
    FsFile f;
    NroData data;
    NroAssetHeader asset;
    u64 bytes_read;

    R_TRY_RESULT(fs.GetFsOpenResult(), {});

    R_TRY_RESULT(fs.OpenFile(path, FsOpenMode_Read, &f), {});
    ON_SCOPE_EXIT(fsFileClose(&f));

    R_TRY_RESULT(fsFileRead(&f, 0, &data, sizeof(data), FsReadOption_None, &bytes_read), {});
    R_UNLESS(data.header.magic == NROHEADER_MAGIC, {});
    R_TRY_RESULT(fsFileRead(&f, data.header.size, &asset, sizeof(asset), FsReadOption_None, &bytes_read), {});
    R_UNLESS(asset.magic == NROASSETHEADER_MAGIC, {});

    return nro_get_icon_internal(&f, asset.icon.size, data.header.size + asset.icon.offset);
}

auto nro_get_nacp(const fs::FsPath& path, NacpStruct& nacp) -> Result {
    fs::FsNativeSd fs;
    FsFile f;
    NroData data;
    NroAssetHeader asset;
    u64 bytes_read;

    R_TRY_RESULT(fs.GetFsOpenResult(), {});

    R_TRY(fs.OpenFile(path, FsOpenMode_Read, &f));
    ON_SCOPE_EXIT(fsFileClose(&f));

    R_TRY(fsFileRead(&f, 0, &data, sizeof(data), FsReadOption_None, &bytes_read));
    R_UNLESS(data.header.magic == NROHEADER_MAGIC, NroError_BadMagic);
    R_TRY(fsFileRead(&f, data.header.size, &asset, sizeof(asset), FsReadOption_None, &bytes_read));
    R_UNLESS(asset.magic == NROASSETHEADER_MAGIC, NroError_BadMagic);
    R_TRY(fsFileRead(&f, data.header.size + asset.nacp.offset, &nacp, sizeof(nacp), FsReadOption_None, &bytes_read));

    R_SUCCEED();
}

auto nro_launch(std::string path, std::string args) -> Result {
    if (path.empty()) {
        return 1;
    }

    // keeps compat with hbloader
    // https://github.com/ITotalJustice/Gamecard-Installer-NX/blob/master/source/main.c#L73
    // https://github.com/ITotalJustice/Gamecard-Installer-NX/blob/d549c5f916dea814fa0a7e5dc8c903fa3044ba15/source/main.c#L29
    if (!path.starts_with("sdmc:")) {
        path = "sdmc:" + path;
    }

    if (args.empty()) {
        args = nro_add_arg(path);
    } else {
        args = nro_add_arg(path) + ' ' + args;
    }

    return launch_internal(path, args);
}

auto nro_add_arg(std::string arg) -> std::string {
    if (arg.contains(' ')) {
        return '\"' + arg + '\"';
    }
    return arg;
}

auto nro_add_arg_file(std::string arg) -> std::string {
    if (!arg.starts_with("sdmc:")) {
        arg = "sdmc:" + arg;
    }
    if (arg.contains(' ')) {
        return '\"' + arg + '\"';
    }
    return arg;
}

auto nro_normalise_path(const std::string& p) -> std::string {
    if (p.starts_with("sdmc:")) {
        return p.substr(5);
    }
    return p;
}

auto search_homebrew(const std::vector<NroEntry> m_entries, const char* name, const fs::FsPath path) -> NroEntry {
    for (auto& p : m_entries) {
        if (path == "") {
            if (!std::strcmp(p.nacp.lang[0].name, name)) {
                return p;
            }
        } else {
            if (!std::strcmp(p.path.s, path.s) && !std::strcmp(p.nacp.lang[0].name, name)) {
                return p;
            }
        }
    }
    return {};
}

} // namespace sphaira
