#include "yati/nx/ns.hpp"
#include "yati/nx/service_guard.h"
#include "defines.hpp"

namespace sphaira::ns {
namespace {

Service g_nsAppSrv;

NX_GENERATE_SERVICE_GUARD(nsEx);

Result _nsExInitialize() {
    R_TRY(nsInitialize());

    if (hosversionAtLeast(3,0,0)) {
        R_TRY(nsGetApplicationManagerInterface(&g_nsAppSrv));
    } else {
        g_nsAppSrv = *nsGetServiceSession_ApplicationManagerInterface();
    }

    R_SUCCEED();
}

void _nsExCleanup() {
    serviceClose(&g_nsAppSrv);
    nsExit();
}

} // namespace

Result Initialize() {
    return nsExInitialize();
}

void Exit() {
    nsExExit();
}

Result PushApplicationRecord(u64 tid, const ncm::ContentStorageRecord* records, u32 count) {
    const struct {
        u8 last_modified_event;
        u8 padding[0x7];
        u64 tid;
    } in = { ApplicationRecordType_Installed, {0}, tid };

    return serviceDispatchIn(&g_nsAppSrv, 16, in,
        .buffer_attrs = { SfBufferAttr_HipcMapAlias | SfBufferAttr_In },
        .buffers = { { records, sizeof(*records) * count } });
}

Result ListApplicationRecordContentMeta(u64 offset, u64 tid, ncm::ContentStorageRecord* out_records, u32 count, s32* entries_read) {
    struct {
        u64 offset;
        u64 tid;
    } in = { offset, tid };

    return serviceDispatchInOut(&g_nsAppSrv, 17, in, *entries_read,
        .buffer_attrs = { SfBufferAttr_HipcMapAlias | SfBufferAttr_Out },
        .buffers = { { out_records, sizeof(*out_records) * count } });
}

Result DeleteApplicationRecord(u64 tid) {
    return serviceDispatchIn(&g_nsAppSrv, 27, tid);
}

Result InvalidateApplicationControlCache(u64 tid) {
    return serviceDispatchIn(&g_nsAppSrv, 404, tid);
}

Result GetApplicationRecords(u64 id, std::vector<ncm::ContentStorageRecord>& out) {
    s32 count;
    R_TRY(nsCountApplicationContentMeta(id, &count));

    s32 records_read;
    out.resize(count);
    R_TRY(ns::ListApplicationRecordContentMeta(0, id, out.data(), out.size(), &records_read));
    out.resize(records_read);

    R_SUCCEED();
}

Result SetLowestLaunchVersion(u64 id) {
    std::vector<ncm::ContentStorageRecord> records;
    R_TRY(GetApplicationRecords(id, records));

    return SetLowestLaunchVersion(id, records);
}

Result SetLowestLaunchVersion(u64 id, std::span<const ncm::ContentStorageRecord> records) {
    R_TRY(avmInitialize());
    ON_SCOPE_EXIT(avmExit());

    u32 new_version = 0;
    for (const auto& record : records) {
        new_version = std::max(new_version, record.key.version);
    }

    return avmPushLaunchVersion(id, new_version);
}

} // namespace sphaira::ns
