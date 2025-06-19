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

constexpr u64 MAX_BUFFER_SIZE = 1024ULL*1024ULL*8ULL;
constexpr u64 MAX_BUFFER_RESERVE_SIZE = 1024ULL*1024ULL*32ULL;
volatile InstallState INSTALL_STATE{InstallState::None};

} // namespace

Stream::Stream(const fs::FsPath& path, std::stop_token token) {
    m_path = path;
    m_token = token;
    m_active = true;
    m_buffer.reserve(MAX_BUFFER_RESERVE_SIZE);

    mutexInit(&m_mutex);
    condvarInit(&m_can_read);
}

Result Stream::ReadChunk(void* buf, s64 size, u64* bytes_read) {
    log_write("[Stream::ReadChunk] inside\n");
    ON_SCOPE_EXIT(
        log_write("[Stream::ReadChunk] exiting\n");
    );

    while (!m_token.stop_requested()) {
        SCOPED_MUTEX(&m_mutex);
        if (m_active && m_buffer.empty()) {
            condvarWait(&m_can_read, &m_mutex);
        }

        if ((!m_active && m_buffer.empty()) || m_token.stop_requested()) {
            break;
        }

        size = std::min<s64>(size, m_buffer.size());
        std::memcpy(buf, m_buffer.data(), size);
        m_buffer.erase(m_buffer.begin(), m_buffer.begin() + size);
        *bytes_read = size;
        R_SUCCEED();
    }

    log_write("[Stream::ReadChunk] failed to read\n");
    R_THROW(Result_TransferCancelled);
}

bool Stream::Push(const void* buf, s64 size) {
    log_write("[Stream::Push] inside\n");
    ON_SCOPE_EXIT(
        log_write("[Stream::Push] exiting\n");
    );

    while (!m_token.stop_requested()) {
        if (INSTALL_STATE == InstallState::Finished) {
            log_write("[Stream::Push] install has finished\n");
            return true;
        }

        // don't use condivar here as windows mtp is very broken.
        // stalling for too longer (3s+) and having too varied transfer speeds
        // results in windows stalling the transfer for 1m until it kills it via timeout.
        // the workaround is to always accept new data, but stall for 1s.
        SCOPED_MUTEX(&m_mutex);
        if (m_active && m_buffer.size() >= MAX_BUFFER_SIZE) {
            // unlock the mutex and wait for 1s to bring transfer speed down to 1MiB/s.
            log_write("[Stream::Push] buffer is full, delaying\n");
            mutexUnlock(&m_mutex);
            ON_SCOPE_EXIT(mutexLock(&m_mutex));

            svcSleepThread(1e+9);
        }

        if (!m_active) {
            log_write("[Stream::Push] file not active\n");
            break;
        }

        const auto offset = m_buffer.size();
        m_buffer.resize(offset + size);
        std::memcpy(m_buffer.data() + offset, buf, size);
        condvarWakeOne(&m_can_read);
        return true;
    }

    log_write("[Stream::Push] failed to push\n");
    return false;
}

void Stream::Disable() {
    log_write("[Stream::Disable] disabling file\n");

    SCOPED_MUTEX(&m_mutex);
    m_active = false;
    condvarWakeOne(&m_can_read);
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
        App::Push(std::make_shared<ui::ProgressBox>(0, "Installing "_i18n, m_source->GetPath(), [this](auto pbox) -> Result {
            INSTALL_STATE = InstallState::Progress;
            const auto rc = yati::InstallFromSource(pbox, m_source, m_source->GetPath());
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
        }));
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

    m_source = std::make_shared<Stream>(path, GetToken());
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
