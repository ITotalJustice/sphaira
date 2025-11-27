#include "usbdvd.hpp"
#include "defines.hpp"

#include "utils/thread.hpp"

#include <switch.h>
#include <usbdvd.h>
#include <memory>

namespace sphaira::usbdvd {
namespace {

Thread g_thread;
Mutex g_mutex;
std::unique_ptr<CUSBDVD> g_dvd;

void thread_func(void* arg) {
    SCOPED_MUTEX(&g_mutex);
    g_dvd = std::make_unique<CUSBDVD>();
}

} // namespace

Result MountAll() {
    SCOPED_MUTEX(&g_mutex);

    // check if we are already mounted.
    if (g_dvd) {
        R_SUCCEED();
    }

    // load usbdvd async.
    R_TRY(utils::CreateThread(&g_thread, thread_func, nullptr, 1024*64));
    R_TRY(threadStart(&g_thread));

    R_SUCCEED();
}

void UnmountAll() {
    SCOPED_MUTEX(&g_mutex);
    threadWaitForExit(&g_thread);
    threadClose(&g_thread);
    g_dvd.reset();
}

bool GetMountPoint(location::StdioEntry& out) {
    SCOPED_MUTEX(&g_mutex);

    // ensure that we have loaded.
    if (!g_dvd) {
        return false;
    }

    const auto& ctx = g_dvd->usbdvd_drive_ctx;
    const auto& fs = ctx.fs;

    // check we have a valid cd mounted.
    if (!fs.mounted) {
        return false;
    }

    // todo: make the display name better (show size etc).
    char display_name[0x100];
    std::snprintf(display_name, sizeof(display_name), "%s - %s", ctx.disc_type, ctx.fs.disc_fstype);

    out.mount = fs.mountpoint;
    out.name = display_name;
    out.flags = location::FsEntryFlag::FsEntryFlag_ReadOnly;

    return true;
}

} // namespace sphaira::usbdvd
