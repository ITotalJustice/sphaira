#pragma once

#include "defines.hpp"
#include <functional>
#include <atomic>

namespace sphaira::utils {

static inline Result CreateThread(Thread *t, ThreadFunc entry, void *arg, size_t stack_sz = 1024*128, int prio = 0x3B) {
    u64 core_mask = 0;
    R_TRY(svcGetInfo(&core_mask, InfoType_CoreMask, CUR_PROCESS_HANDLE, 0));
    R_TRY(threadCreate(t, entry, arg, nullptr, stack_sz, prio, -2));
    R_TRY(svcSetThreadCoreMask(t->handle, -1, core_mask));
    R_SUCCEED();
}

struct Async final {
    using Callback = std::function<void(void)>;

    // core0=main, core1=audio, core2=servers (ftp,mtp,nxlink)
    Async(Callback&& callback) : m_callback{std::forward<Callback>(callback)} {
        m_running = true;

        if (R_FAILED(CreateThread(&m_thread, thread_func, &m_callback))) {
            m_running = false;
            return;
        }

        if (R_FAILED(threadStart(&m_thread))) {
            threadClose(&m_thread);
            m_running = false;
        }
    }

    ~Async() {
        WaitForExit();
    }

    void WaitForExit() {
        if (m_running) {
            threadWaitForExit(&m_thread);
            threadClose(&m_thread);
            m_running = false;
        }
    }

private:
    static void thread_func(void* arg) {
        (*static_cast<Callback*>(arg))();
    }

private:
    Callback m_callback;
    Thread m_thread{};
    std::atomic_bool m_running{};
};

} // namespace sphaira::utils
