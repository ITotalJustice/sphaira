#include "log.hpp"
#include <cstdio>
#include <cstdarg>
#include <unistd.h>
#include <mutex>
#include <switch.h>

#if sphaira_USE_LOG
namespace {

constexpr const char* logpath = "/config/sphaira/log.txt";

int nxlink_socket{};
bool g_file_open{};
std::mutex mutex{};

void log_write_arg_internal(const char* s, std::va_list* v) {
    const auto t = std::time(nullptr);
    const auto tm = std::localtime(&t);

    static char buf[512];
    const auto len = std::snprintf(buf, sizeof(buf), "[%02u:%02u:%02u] -> ", tm->tm_hour, tm->tm_min, tm->tm_sec);
    std::vsnprintf(buf + len, sizeof(buf) - len, s, *v);

    if (g_file_open) {
        auto file = std::fopen(logpath, "a");
        if (file) {
            std::fprintf(file, "%s", buf);
            std::fclose(file);
        }
    }
    if (nxlink_socket) {
        std::printf("%s", buf);
    }
}

} // namespace

extern "C" {

auto log_file_init() -> bool {
    std::scoped_lock lock{mutex};
    if (g_file_open) {
        return false;
    }

    auto file = std::fopen(logpath, "w");
    if (file) {
        g_file_open = true;
        std::fclose(file);
        return true;
    }

    return false;
}

auto log_nxlink_init() -> bool {
    std::scoped_lock lock{mutex};
    if (nxlink_socket) {
        return false;
    }

    nxlink_socket = nxlinkConnectToHost(true, false);
    return nxlink_socket != 0;
}

void log_file_exit() {
    std::scoped_lock lock{mutex};
    if (g_file_open) {
        g_file_open = false;
    }
}

void log_nxlink_exit() {
    std::scoped_lock lock{mutex};
    if (nxlink_socket) {
        close(nxlink_socket);
        nxlink_socket = 0;
    }
}

bool log_is_init() {
    std::scoped_lock lock{mutex};
    return g_file_open || nxlink_socket;
}

void log_write(const char* s, ...) {
    if (!log_is_init()) {
        return;
    }

    std::scoped_lock lock{mutex};
    std::va_list v{};
    va_start(v, s);
    log_write_arg_internal(s, &v);
    va_end(v);
}

void log_write_arg(const char* s, va_list* v) {
    if (!log_is_init()) {
        return;
    }

    std::scoped_lock lock{mutex};
    log_write_arg_internal(s, v);
}

} // extern "C"

#endif
