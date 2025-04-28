#include "ui/menus/usb_menu.hpp"
#include "yati/yati.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"
#include <cstring>

namespace sphaira::ui::menu::usb {
namespace {

constexpr u64 CONNECTION_TIMEOUT = 1e+9 * 3;
constexpr u64 TRANSFER_TIMEOUT = 1e+9 * 5;

void thread_func(void* user) {
    auto app = static_cast<Menu*>(user);

    for (;;) {
        if (app->GetToken().stop_requested()) {
            break;
        }

        const auto rc = app->m_usb_source->WaitForConnection(CONNECTION_TIMEOUT, app->m_usb_speed, app->m_usb_count);
        mutexLock(&app->m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&app->m_mutex));

        if (R_SUCCEEDED(rc)) {
            app->m_state = State::Connected;
            break;
        } else if (R_FAILED(rc) && R_VALUE(rc) != 0xEA01) {
            log_write("got: 0x%X value: 0x%X\n", rc, R_VALUE(rc));
            app->m_state = State::Failed;
            break;
        }
    }
}

} // namespace

Menu::Menu() : MenuBase{"USB"_i18n} {
    SetAction(Button::B, Action{"Back"_i18n, [this](){
        SetPop();
    }});

    SetAction(Button::X, Action{"Options"_i18n, [this](){
        App::DisplayInstallOptions(false);
    }});

    // if mtp is enabled, disable it for now.
    m_was_mtp_enabled = App::GetMtpEnable();
    if (m_was_mtp_enabled) {
        App::Notify("Disable MTP for usb install"_i18n);
        App::SetMtpEnable(false);
    }

    // 3 second timeout for transfers.
    m_usb_source = std::make_shared<yati::source::Usb>(TRANSFER_TIMEOUT);
    if (R_FAILED(m_usb_source->GetOpenResult())) {
        log_write("usb init open\n");
        m_state = State::Failed;
    } else {
        if (R_FAILED(m_usb_source->Init())) {
            log_write("usb init failed\n");
            m_state = State::Failed;
        }
    }

    mutexInit(&m_mutex);
    if (m_state != State::Failed) {
        threadCreate(&m_thread, thread_func, this, nullptr, 1024*32, 0x2C, 1);
        threadStart(&m_thread);
    }
}

Menu::~Menu() {
    // signal for thread to exit and wait.
    m_stop_source.request_stop();
    threadWaitForExit(&m_thread);
    threadClose(&m_thread);

    // free usb source before re-enabling mtp.
    log_write("closing data!!!!\n");
    m_usb_source.reset();

    if (m_was_mtp_enabled) {
        App::Notify("Re-enabled MTP"_i18n);
        App::SetMtpEnable(true);
    }
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);

    mutexLock(&m_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

    switch (m_state) {
        case State::None:
            break;

        case State::Connected:
            log_write("set to progress\n");
            m_state = State::Progress;
            log_write("got connection\n");
            App::Push(std::make_shared<ui::ProgressBox>(0, "Installing "_i18n, "", [this](auto pbox) mutable -> bool {
                log_write("inside progress box\n");
                for (u32 i = 0; i < m_usb_count; i++) {
                    std::string file_name;
                    u64 file_size;
                    if (R_FAILED(m_usb_source->GetFileInfo(file_name, file_size))) {
                        return false;
                    }

                    log_write("got file name: %s size: %lX\n", file_name.c_str(), file_size);

                    const auto rc = yati::InstallFromSource(pbox, m_usb_source, file_name);
                    if (R_FAILED(rc)) {
                        return false;
                    }

                    App::Notify("Installed via usb"_i18n);
                    m_usb_source->Finished();
                }

                return true;
            }, [this](bool result){
                if (result) {
                    App::Notify("Usb install success!"_i18n);
                    m_state = State::Done;
                } else {
                    App::Notify("Usb install failed!"_i18n);
                    m_state = State::Failed;
                }
            }));
            break;

        case State::Progress:
            break;

        case State::Done:
            break;

        case State::Failed:
            break;
    }
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    mutexLock(&m_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

    switch (m_state) {
        case State::None:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Waiting for connection..."_i18n.c_str());
            break;

        case State::Connected:
            break;

        case State::Progress:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Transferring data..."_i18n.c_str());
            break;

        case State::Done:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Press B to exit..."_i18n.c_str());
            break;

        case State::Failed:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Failed to init usb, press B to exit..."_i18n.c_str());
            break;
    }
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();
}

} // namespace sphaira::ui::menu::usb
