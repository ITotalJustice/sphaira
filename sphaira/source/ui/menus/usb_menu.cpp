#include "ui/menus/usb_menu.hpp"
#include "yati/yati.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"

#include "utils/thread.hpp"

#include <cstring>

namespace sphaira::ui::menu::usb {
namespace {

constexpr u64 CONNECTION_TIMEOUT = 3e+9;
constexpr u64 TRANSFER_TIMEOUT = 3e+9;
constexpr u64 FINISHED_TIMEOUT = 3e+9; // 3 seconds.

void thread_func(void* user) {
    auto app = static_cast<Menu*>(user);
    app->ThreadFunction();
}

} // namespace

Menu::Menu(u32 flags) : MenuBase{"USB"_i18n, flags} {
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
    m_usb_source = std::make_unique<yati::source::Usb>(TRANSFER_TIMEOUT);
    if (R_FAILED(m_usb_source->GetOpenResult())) {
        log_write("usb init open\n");
        m_state = State::Failed;
    }

    if (m_state != State::Failed) {
        utils::CreateThread(&m_thread, thread_func, this, 1024*32);
        threadStart(&m_thread);
    }
}

Menu::~Menu() {
    // signal for thread to exit and wait.
    if (R_FAILED(waitSingleHandle(m_thread.handle, 0))) {
        m_usb_source->SignalCancel();
        m_stop_source.request_stop();
    }

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

    static TimeStamp poll_ts;
    if (poll_ts.GetSeconds() >= 1) {
        poll_ts.Update();

        UsbState state{UsbState_Detached};
        usbDsGetState(&state);

        UsbDeviceSpeed speed{(UsbDeviceSpeed)UsbDeviceSpeed_None};
        usbDsGetSpeed(&speed);

        char buf[128];
        std::snprintf(buf, sizeof(buf), "State: %s | Speed: %s"_i18n.c_str(), i18n::get(GetUsbDsStateStr(state)).c_str(), i18n::get(GetUsbDsSpeedStr(speed)).c_str());
        SetSubHeading(buf);
    }

    if (m_state == State::Connected_StartingTransfer) {
        log_write("set to progress\n");
        m_state = State::Progress;
        log_write("got connection\n");
        App::Push<ui::ProgressBox>(0, "Installing "_i18n, "", [this](auto pbox) -> Result {
            pbox->AddCancelEvent(m_usb_source->GetCancelEvent());
            ON_SCOPE_EXIT(pbox->RemoveCancelEvent(m_usb_source->GetCancelEvent()));

            log_write("inside progress box\n");
            for (u32 i = 0; i < std::size(m_names); i++) {
                const auto& file_name = m_names[i];

                s64 file_size;
                R_TRY(m_usb_source->OpenFile(i, file_size));
                ON_SCOPE_EXIT(m_usb_source->CloseFile());

                // if we are doing s2s install, skip verifying the nca contents.
                yati::ConfigOverride config_override{};
                if (m_usb_source->IsStream()) {
                    config_override.skip_nca_hash_verify = true;
                    config_override.skip_rsa_header_fixed_key_verify = true;
                    config_override.skip_rsa_npdm_fixed_key_verify = true;
                }

                pbox->SetTitle(file_name);
                const auto rc = yati::InstallFromSource(pbox, m_usb_source.get(), file_name, config_override);
                if (R_FAILED(rc)) {
                    m_usb_source->SignalCancel();
                    log_write("exiting usb install\n");
                    R_THROW(rc);
                }

                App::Notify("Installed via usb"_i18n);
            }

            R_SUCCEED();
        }, [this](Result rc){
            App::PushErrorBox(rc, "USB install failed!"_i18n);

            if (R_SUCCEEDED(rc)) {
                App::Notify("Usb install success!"_i18n);
                m_state = State::Done;
                SetPop();
            } else {
                m_state = State::Failed;
            }
        });
    }
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    switch (m_state) {
        case State::None:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Waiting for connection..."_i18n.c_str());
            break;

        case State::Connected_WaitForFileList:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Connected, waiting for file list..."_i18n.c_str());
            break;

        case State::Connected_StartingTransfer:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Connected, starting transfer..."_i18n.c_str());
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

void Menu::ThreadFunction() {
    for (;;) {
        if (GetToken().stop_requested()) {
            break;
        }

        const auto rc = m_usb_source->IsUsbConnected(CONNECTION_TIMEOUT);
        if (rc == Result_UsbCancelled) {
            break;
        }

        // set connected status
        if (R_SUCCEEDED(rc)) {
            m_state = State::Connected_WaitForFileList;
        } else {
            m_state = State::None;
        }

        if (R_SUCCEEDED(rc)) {
            std::vector<std::string> names;
            if (R_SUCCEEDED(m_usb_source->WaitForConnection(CONNECTION_TIMEOUT, names))) {
                m_names = names;
                m_state = State::Connected_StartingTransfer;
                break;
            }
        }
    }
}

} // namespace sphaira::ui::menu::usb
