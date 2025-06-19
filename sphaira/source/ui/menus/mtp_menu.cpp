#include "ui/menus/mtp_menu.hpp"
#include "usb/usbds.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"
#include "haze_helper.hpp"

namespace sphaira::ui::menu::mtp {
namespace {

auto GetUsbStateStr(UsbState state) -> const char* {
    switch (state) {
        case UsbState_Detached: return "Detached";
        case UsbState_Attached: return "Attached";
        case UsbState_Powered: return "Powered";
        case UsbState_Default: return "Default";
        case UsbState_Address: return "Address";
        case UsbState_Configured: return "Configured";
        case UsbState_Suspended: return "Suspended";
    }

    return "Unknown";
}

auto GetUsbSpeedStr(UsbDeviceSpeed speed) -> const char* {
    // todo: remove this cast when libnx pr is merged.
    switch ((u32)speed) {
        case UsbDeviceSpeed_None: return "None";
        case UsbDeviceSpeed_Low: return "USB 1.0 Low Speed";
        case UsbDeviceSpeed_Full: return "USB 1.1 Full Speed";
        case UsbDeviceSpeed_High: return "USB 2.0 High Speed";
        case UsbDeviceSpeed_Super: return "USB 3.0 Super Speed";
    }

    return "Unknown";
}

} // namespace

Menu::Menu(u32 flags) : stream::Menu{"MTP Install"_i18n, flags} {
    m_was_mtp_enabled = App::GetMtpEnable();
    if (!m_was_mtp_enabled) {
        log_write("[MTP] wasn't enabled, forcefully enabling\n");
        App::SetMtpEnable(true);
    }

    haze::InitInstallMode(
        [this](const char* path){ return OnInstallStart(path); },
        [this](const void *buf, size_t size){ return OnInstallWrite(buf, size); },
        [this](){ return OnInstallClose(); }
    );
}

Menu::~Menu() {
    // signal for thread to exit and wait.
    haze::DisableInstallMode();

    if (!m_was_mtp_enabled) {
        log_write("[MTP] disabling on exit\n");
        App::SetMtpEnable(false);
    }
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    stream::Menu::Update(controller, touch);

    static TimeStamp poll_ts;
    if (poll_ts.GetSeconds() >= 1) {
        poll_ts.Update();

        UsbState state{UsbState_Detached};
        usbDsGetState(&state);

        UsbDeviceSpeed speed{(UsbDeviceSpeed)UsbDeviceSpeed_None};
        usbDsGetSpeed(&speed);

        char buf[128];
        std::snprintf(buf, sizeof(buf), "State: %s | Speed: %s", i18n::get(GetUsbStateStr(state)).c_str(), i18n::get(GetUsbSpeedStr(speed)).c_str());
        SetSubHeading(buf);
    }
}

void Menu::OnDisableInstallMode() {
    haze::DisableInstallMode();
}

} // namespace sphaira::ui::menu::mtp
