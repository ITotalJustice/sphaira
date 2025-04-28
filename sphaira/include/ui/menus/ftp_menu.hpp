#pragma once

#include "ui/menus/menu_base.hpp"
#include "yati/source/stream.hpp"

namespace sphaira::ui::menu::ftp {

enum class State {
    // not connected.
    None,
    // just connected, starts the transfer.
    Connected,
    // set whilst transfer is in progress.
    Progress,
    // set when the transfer is finished.
    Done,
    // failed to connect.
    Failed,
};

struct StreamFtp final : yati::source::Stream {
    StreamFtp(const fs::FsPath& path, std::stop_token token);

    Result ReadChunk(void* buf, s64 size, u64* bytes_read) override;
    bool Push(const void* buf, s64 size);
    void Disable();

// private:
    fs::FsPath m_path{};
    std::stop_token m_token{};
    std::vector<u8> m_buffer{};
    Mutex m_mutex{};
    bool m_active{};
    // bool m_push_exit{};
};

struct Menu final : MenuBase {
    Menu();
    ~Menu();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;
    void OnFocusGained() override;

// this should be private
// private:
    std::shared_ptr<StreamFtp> m_source;
    Thread m_thread{};
    Mutex m_mutex{};
    // the below are shared across threads, lock with the above mutex!
    State m_state{State::None};

    const char* m_user{};
    const char* m_pass{};
    unsigned m_port{};
    bool m_anon{};
};

} // namespace sphaira::ui::menu::ftp
