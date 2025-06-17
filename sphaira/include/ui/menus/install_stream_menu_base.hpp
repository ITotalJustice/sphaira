#pragma once

#include "ui/menus/menu_base.hpp"
#include "yati/source/stream.hpp"

namespace sphaira::ui::menu::stream {

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

using OnInstallStart = std::function<bool(const char* path)>;
using OnInstallWrite = std::function<bool(const void* buf, size_t size)>;
using OnInstallClose = std::function<void()>;

struct Stream final : yati::source::Stream {
    Stream(const fs::FsPath& path, std::stop_token token);

    Result ReadChunk(void* buf, s64 size, u64* bytes_read) override;
    bool Push(const void* buf, s64 size);
    void Disable();
    auto& GetPath() const { return m_path; }

private:
    fs::FsPath m_path{};
    std::stop_token m_token{};
    std::vector<u8> m_buffer{};
    CondVar m_can_read{};

public:
    Mutex m_mutex{};
    bool m_active{};
};

struct Menu : MenuBase {
    Menu(const std::string& title, u32 flags);
    virtual ~Menu();

    virtual void Update(Controller* controller, TouchInfo* touch);
    virtual void Draw(NVGcontext* vg, Theme* theme);
    virtual void OnDisableInstallMode() = 0;

protected:
    bool OnInstallStart(const char* path);
    bool OnInstallWrite(const void* buf, size_t size);
    void OnInstallClose();

private:
    std::shared_ptr<Stream> m_source{};
    Thread m_thread{};
    Mutex m_mutex{};
    State m_state{State::None};
};

} // namespace sphaira::ui::menu::stream
