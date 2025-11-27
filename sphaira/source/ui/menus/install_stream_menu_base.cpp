#include "ui/menus/install_stream_menu_base.hpp"
#include "yati/yati.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"
#include <cstring>

namespace sphaira::ui::menu::stream {
namespace {

enum class InstallState {
    None,
    Progress,
    Finished,
};

constexpr u64 MAX_BUFFER_SIZE = 1024ULL*1024ULL*1ULL;
std::atomic<InstallState> INSTALL_STATE{InstallState::None};

} // namespace

Stream::Stream(const fs::FsPath& path, std::stop_token token) {
    m_path = path;
    m_token = token;
    m_active = true;
    m_buffer.reserve(MAX_BUFFER_SIZE);

    mutexInit(&m_mutex);
    condvarInit(&m_can_read);
    condvarInit(&m_can_write);
}

Result Stream::ReadChunk(void* _buf, s64 size, u64* bytes_read) {
    auto buf = static_cast<u8*>(_buf);
    *bytes_read = 0;

    log_write("[Stream::ReadChunk] inside\n");
    ON_SCOPE_EXIT(
        log_write("[Stream::ReadChunk] exiting\n");
    );

    while (!m_token.stop_requested()) {
        SCOPED_MUTEX(&m_mutex);
        if (m_active && m_buffer.empty()) {
            R_TRY(condvarWait(std::addressof(m_can_read), std::addressof(m_mutex)));
        }

        if ((!m_active && m_buffer.empty()) || m_token.stop_requested()) {
            break;
        }

        const auto rsize = std::min<s64>(size, m_buffer.size());
        std::memcpy(buf, m_buffer.data(), rsize);
        m_buffer.erase(m_buffer.begin(), m_buffer.begin() + rsize);
        condvarWakeOne(&m_can_write);

        size -= rsize;
        buf += rsize;
        *bytes_read += rsize;

        if (!size) {
            R_SUCCEED();
        }
    }

    log_write("[Stream::ReadChunk] failed to read\n");
    R_THROW(Result_TransferCancelled);
}

bool Stream::Push(const void* _buf, s64 size) {
    auto buf = static_cast<const u8*>(_buf);
    if (!size) {
        return true;
    }

    log_write("[Stream::Push] inside\n");
    ON_SCOPE_EXIT(
        log_write("[Stream::Push] exiting\n");
    );

    while (!m_token.stop_requested()) {
        if (INSTALL_STATE == InstallState::Finished) {
            log_write("[Stream::Push] install has finished\n");
            return true;
        }

        SCOPED_MUTEX(&m_mutex);
        if (m_active && m_buffer.size() >= MAX_BUFFER_SIZE) {
            R_TRY(condvarWait(std::addressof(m_can_write), std::addressof(m_mutex)));
        }

        if (!m_active) {
            log_write("[Stream::Push] file not active\n");
            break;
        }

        const auto wsize = std::min<s64>(size, MAX_BUFFER_SIZE - m_buffer.size());
        const auto offset = m_buffer.size();
        m_buffer.resize(offset + wsize);

        std::memcpy(m_buffer.data() + offset, buf, wsize);
        condvarWakeOne(&m_can_read);

        size -= wsize;
        buf += wsize;
        if (!size) {
            return true;
        }
    }

    log_write("[Stream::Push] failed to push\n");
    return false;
}

void Stream::Disable() {
    log_write("[Stream::Disable] disabling file\n");

    SCOPED_MUTEX(&m_mutex);
    m_active = false;
    condvarWakeOne(&m_can_read);
    condvarWakeOne(&m_can_write);
}

Menu::Menu(const std::string& title, u32 flags) : MenuBase{title, flags} {
    SetAction(Button::B, Action{"Back"_i18n, [this](){
        SetPop();
    }});

    SetAction(Button::X, Action{"Options"_i18n, [this](){
        App::DisplayInstallOptions(false);
    }});

    App::SetAutoSleepDisabled(true);
    mutexInit(&m_mutex);

    INSTALL_STATE = InstallState::None;
}

Menu::~Menu() {
    // signal for thread to exit and wait.
    m_stop_source.request_stop();

    if (m_source) {
        m_source->Disable();
    }

    App::SetAutoSleepDisabled(false);
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);

    SCOPED_MUTEX(&m_mutex);

    if (m_state == State::Connected) {
        m_state = State::Progress;
        App::Push<ui::ProgressBox>(0, "Installing "_i18n, m_source->GetPath(), [this](auto pbox) -> Result {
            INSTALL_STATE = InstallState::Progress;
            const auto rc = yati::InstallFromSource(pbox, m_source.get(), m_source->GetPath());
            INSTALL_STATE = InstallState::Finished;

            if (R_FAILED(rc)) {
                m_source->Disable();
                R_THROW(rc);
            }

            R_SUCCEED();
        }, [this](Result rc){
            App::PushErrorBox(rc, "Install failed!"_i18n);

            SCOPED_MUTEX(&m_mutex);

            if (R_SUCCEEDED(rc)) {
                App::Notify("Install success!"_i18n);
                m_state = State::Done;
            } else {
                m_state = State::Failed;
                OnDisableInstallMode();
            }
        });
    }
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    SCOPED_MUTEX(&m_mutex);

    switch (m_state) {
        case State::None:
        case State::Done:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Drag'n'Drop (NSP, XCI, NSZ, XCZ) to the install folder"_i18n.c_str());
            break;

        case State::Connected:
        case State::Progress:
            break;

        case State::Failed:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Failed to install, press B to exit..."_i18n.c_str());
            break;
    }
}

bool Menu::OnInstallStart(const char* path) {
    log_write("[Menu::OnInstallStart] inside\n");

    for (;;) {
        {
            SCOPED_MUTEX(&m_mutex);

            if (m_state != State::Progress) {
                break;
            }

            if (GetToken().stop_requested()) {
                return false;
            }
        }

        svcSleepThread(1e+6);
    }

    log_write("[Menu::OnInstallStart] got state: %u\n", (u8)m_state);

    if (m_source) {
        log_write("[Menu::OnInstallStart] we have source\n");
        for (;;) {
            {
                SCOPED_MUTEX(&m_source->m_mutex);

                if (!m_source->m_active && INSTALL_STATE != InstallState::Progress) {
                    break;
                }

                if (GetToken().stop_requested()) {
                    return false;
                }
            }

            svcSleepThread(1e+6);
        }

        log_write("[Menu::OnInstallStart] stopped polling source\n");
    }

    SCOPED_MUTEX(&m_mutex);

    m_source = std::make_unique<Stream>(path, GetToken());
    INSTALL_STATE = InstallState::None;
    m_state = State::Connected;
    log_write("[Menu::OnInstallStart] exiting\n");

    return true;
}

bool Menu::OnInstallWrite(const void* buf, size_t size) {
    log_write("[Menu::OnInstallWrite] inside\n");
    return m_source->Push(buf, size);
}

void Menu::OnInstallClose() {
    log_write("[Menu::OnInstallClose] inside\n");

    m_source->Disable();

    // wait until the install has finished before returning.
    while (INSTALL_STATE == InstallState::Progress) {
        svcSleepThread(1e+7);
    }
}

} // namespace sphaira::ui::menu::stream
