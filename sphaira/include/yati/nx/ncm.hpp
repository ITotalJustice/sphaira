#pragma once

#include <switch.h>

namespace sphaira::ncm {

struct PackagedContentMeta {
    u64 title_id;
    u32 title_version;
    u8 meta_type; // NcmContentMetaType
    u8 content_meta_platform; // [17.0.0+]
    NcmContentMetaHeader meta_header;
    u8 install_type; // NcmContentInstallType
    u8 _0x17;
    u32 required_sys_version;
    u8 _0x1C[0x4];
};
static_assert(sizeof(PackagedContentMeta) == 0x20);

struct ContentStorageRecord {
    NcmContentMetaKey key;
    u8 storage_id; // NcmStorageId
    u8 padding[0x7];
};

union ExtendedHeader {
    NcmApplicationMetaExtendedHeader application;
    NcmPatchMetaExtendedHeader patch;
    NcmAddOnContentMetaExtendedHeader addon;
    NcmLegacyAddOnContentMetaExtendedHeader addon_legacy;
    NcmDataPatchMetaExtendedHeader data_patch;
};

auto GetAppId(u8 meta_type, u64 id) -> u64;
auto GetAppId(const NcmContentMetaKey& key) -> u64;
auto GetAppId(const PackagedContentMeta& meta) -> u64;

Result Delete(NcmContentStorage* cs, const NcmContentId *content_id);
Result Register(NcmContentStorage* cs, const NcmContentId *content_id, const NcmPlaceHolderId *placeholder_id);

} // namespace sphaira::ncm
