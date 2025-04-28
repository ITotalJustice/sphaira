#include "yati/nx/ncm.hpp"
#include "defines.hpp"
#include <memory>

namespace sphaira::ncm {
namespace {

} // namespace

auto GetAppId(u8 meta_type, u64 id) -> u64 {
    if (meta_type == NcmContentMetaType_Patch) {
        return id ^ 0x800;
    } else if (meta_type == NcmContentMetaType_AddOnContent) {
        return (id ^ 0x1000) & ~0xFFF;
    } else {
        return id;
    }
}

auto GetAppId(const NcmContentMetaKey& key) -> u64 {
    return GetAppId(key.type, key.id);
}

auto GetAppId(const PackagedContentMeta& meta) -> u64 {
    return GetAppId(meta.meta_type, meta.title_id);
}

Result Delete(NcmContentStorage* cs, const NcmContentId *content_id) {
    bool has;
    R_TRY(ncmContentStorageHas(cs, std::addressof(has), content_id));
    if (has) {
        R_TRY(ncmContentStorageDelete(cs, content_id));
    }
    R_SUCCEED();
}

Result Register(NcmContentStorage* cs, const NcmContentId *content_id, const NcmPlaceHolderId *placeholder_id) {
    R_TRY(Delete(cs, content_id));
    return ncmContentStorageRegister(cs, content_id, placeholder_id);
}

} // namespace sphaira::ncm
