#pragma once

#include "fs.hpp"
#include <optional>
#include <span>
#include <vector>
#include <memory>
#include <functional>
#include <switch.h>

namespace sphaira::title {

constexpr u32 ContentMetaTypeToContentFlag(u8 meta_type) {
    if (meta_type & 0x80) {
        return 1 << (meta_type - 0x80);
    }

    return 0;
}

enum ContentFlag {
    ContentFlag_Application = ContentMetaTypeToContentFlag(NcmContentMetaType_Application),
    ContentFlag_Patch = ContentMetaTypeToContentFlag(NcmContentMetaType_Patch),
    ContentFlag_AddOnContent = ContentMetaTypeToContentFlag(NcmContentMetaType_AddOnContent),
    ContentFlag_DataPatch = ContentMetaTypeToContentFlag(NcmContentMetaType_DataPatch),

    // nca locations where a control.nacp can exist.
    ContentFlag_Nacp = ContentFlag_Application | ContentFlag_Patch,
    // all of the above.
    ContentFlag_All = ContentFlag_Application | ContentFlag_Patch | ContentFlag_AddOnContent | ContentFlag_DataPatch,
};

enum class NacpLoadStatus {
    // not yet attempted to be loaded.
    None,
    // started loading.
    Progress,
    // loaded, ready to parse.
    Loaded,
    // failed to load, do not attempt to load again!
    Error,
};

struct ThreadResultData {
    u64 id{};
    std::vector<u8> icon;
    NacpLanguageEntry lang{};
    NacpLoadStatus status{NacpLoadStatus::None};
};

using MetaEntries = std::vector<NsApplicationContentMetaStatus>;

// starts background thread (ref counted).
Result Init();
// closes the background thread.
void Exit();
// clears cache and empties the result array.
void Clear();

// adds new entry to queue.
void PushAsync(u64 app_id);
void PushAsync(const std::span<const NsApplicationRecord> app_ids);
// gets entry without removing it from the queue.
auto GetAsync(u64 app_id) -> ThreadResultData*;
// single threaded title info fetch.
auto Get(u64 app_id, bool* cached = nullptr) -> ThreadResultData*;

auto GetNcmCs(u8 storage_id) -> NcmContentStorage&;
auto GetNcmDb(u8 storage_id) -> NcmContentMetaDatabase&;

// gets all meta entries for an id.
Result GetMetaEntries(u64 id, MetaEntries& out, u32 flags = ContentFlag_All);

// returns the nca path of a control nca.
Result GetControlPathFromStatus(const NsApplicationContentMetaStatus& status, u64* out_program_id, fs::FsPath* out_path);

// taken from nxdumptool.
void utilsReplaceIllegalCharacters(char *str, bool ascii_only);

// /atmosphere/contents/xxx
auto GetContentsPath(u64 app_id) -> fs::FsPath;

} // namespace sphaira::title
