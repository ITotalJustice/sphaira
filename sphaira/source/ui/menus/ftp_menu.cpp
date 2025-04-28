#include "ui/menus/ftp_menu.hpp"
#include "yati/yati.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"
#include "ftpsrv_helper.hpp"
#include <cstring>

namespace sphaira::ui::menu::ftp {
namespace {

constexpr u64 MAX_BUFFER_SIZE = 1024*1024*32;
constexpr u64 SLEEPNS = 1000;
volatile bool IN_PUSH_THREAD{};

bool OnInstallStart(void* user, const char* path) {
    auto menu = (Menu*)user;
    log_write("[INSTALL] inside OnInstallStart()\n");

    for (;;) {
        mutexLock(&menu->m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&menu->m_mutex));

        if (menu->m_state != State::Progress) {
            break;
        }

        if (menu->GetToken().stop_requested()) {
            return false;
        }

        svcSleepThread(1e+6);
    }

    log_write("[INSTALL] OnInstallStart() got state: %u\n", (u8)menu->m_state);

    if (menu->m_source) {
        log_write("[INSTALL] OnInstallStart() we have source\n");
        for (;;) {
            mutexLock(&menu->m_source->m_mutex);
            ON_SCOPE_EXIT(mutexUnlock(&menu->m_source->m_mutex));

            if (!IN_PUSH_THREAD) {
                break;
            }

            if (menu->GetToken().stop_requested()) {
                return false;
            }

            svcSleepThread(1e+6);
        }

        log_write("[INSTALL] OnInstallStart() stopped polling source\n");
    }

    log_write("[INSTALL] OnInstallStart() doing make_shared\n");
    menu->m_source = std::make_shared<StreamFtp>(path, menu->GetToken());

    mutexLock(&menu->m_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&menu->m_mutex));
    menu->m_state = State::Connected;
    log_write("[INSTALL] OnInstallStart() done make shared\n");

    return true;
}

bool OnInstallWrite(void* user, const void* buf, size_t size) {
    auto menu = (Menu*)user;

    return menu->m_source->Push(buf, size);
}

void OnInstallClose(void* user) {
    auto menu = (Menu*)user;
    menu->m_source->Disable();
}

} // namespace

StreamFtp::StreamFtp(const fs::FsPath& path, std::stop_token token) {
    m_path = path;
    m_token = token;
    m_buffer.reserve(MAX_BUFFER_SIZE);
    m_active = true;
}

Result StreamFtp::ReadChunk(void* buf, s64 size, u64* bytes_read) {
    while (!m_token.stop_requested()) {
        mutexLock(&m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

        if (m_buffer.empty()) {
            if (!m_active) {
                break;
            }

            svcSleepThread(SLEEPNS);
        } else {
            size = std::min<s64>(size, m_buffer.size());
            std::memcpy(buf, m_buffer.data(), size);
            m_buffer.erase(m_buffer.begin(), m_buffer.begin() + size);
            *bytes_read = size;
            R_SUCCEED();
        }
    }

    return 0x1;
}

bool StreamFtp::Push(const void* buf, s64 size) {
    IN_PUSH_THREAD = true;
    ON_SCOPE_EXIT(IN_PUSH_THREAD = false);

    while (!m_token.stop_requested()) {
        mutexLock(&m_mutex);
        ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

        if (!m_active) {
            break;
        }

        if (m_buffer.size() + size >= MAX_BUFFER_SIZE) {
            svcSleepThread(SLEEPNS);
        } else {
            const auto offset = m_buffer.size();
            m_buffer.resize(offset + size);
            std::memcpy(m_buffer.data() + offset, buf, size);
            return true;
        }
    }

    return false;
}

void StreamFtp::Disable() {
    mutexLock(&m_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&m_mutex));
    m_active = false;
}

Menu::Menu() : MenuBase{"FTP Install (EXPERIMENTAL)"_i18n} {
    SetAction(Button::B, Action{"Back"_i18n, [this](){
        SetPop();
    }});

    SetAction(Button::X, Action{"Options"_i18n, [this](){
        App::DisplayInstallOptions(false);
    }});

    mutexInit(&m_mutex);
    ftpsrv::InitInstallMode(this, OnInstallStart, OnInstallWrite, OnInstallClose);

    m_port = ftpsrv::GetPort();
    m_anon = ftpsrv::IsAnon();
    if (!m_anon) {
        m_user = ftpsrv::GetUser();
        m_pass = ftpsrv::GetPass();
    }
}

Menu::~Menu() {
    // signal for thread to exit and wait.
    ftpsrv::DisableInstallMode();
    m_stop_source.request_stop();

    if (m_source) {
        m_source->Disable();
    }

    log_write("closing data!!!!\n");
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
                const auto rc = yati::InstallFromSource(pbox, m_source, m_source->m_path);
                if (R_FAILED(rc)) {
                    m_source->Disable();
                    return false;
                }

                log_write("progress box is done\n");
                return true;
            }, [this](bool result){
                mutexLock(&m_mutex);
                ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

                if (result) {
                    App::Notify("Ftp install success!"_i18n);
                    m_state = State::Done;
                } else {
                    App::Notify("Ftp install failed!"_i18n);
                    m_state = State::Failed;
                }
            }));
            break;

        case State::Progress:
        case State::Done:
        case State::Failed:
            break;
    }
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    mutexLock(&m_mutex);
    ON_SCOPE_EXIT(mutexUnlock(&m_mutex));

    if (m_ip) {
        if (m_type == NifmInternetConnectionType_WiFi) {
            SetSubHeading("Connection Type: WiFi | Strength: "_i18n + std::to_string(m_strength));
        } else {
            SetSubHeading("Connection Type: Ethernet"_i18n);
        }
    } else {
        SetSubHeading("Connection Type: None"_i18n);
    }

    const float start_x = 80;
    const float font_size = 22;
    const float spacing = 33;
    float start_y = 125;
    float bounds[4];

    nvgFontSize(vg, font_size);

    // note: textbounds strips spaces...todo: use nvgTextGlyphPositions() instead.
    #define draw(key, ...) \
        gfx::textBounds(vg, start_x, start_y, bounds, key.c_str()); \
        gfx::drawTextArgs(vg, start_x, start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT), key.c_str()); \
        gfx::drawTextArgs(vg, bounds[2], start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_SELECTED), __VA_ARGS__); \
        start_y += spacing;

    if (m_ip) {
        draw("Host:"_i18n, " %u.%u.%u.%u", m_ip&0xFF, (m_ip>>8)&0xFF, (m_ip>>16)&0xFF, (m_ip>>24)&0xFF);
        draw("Port:"_i18n, " %u", m_port);
        if (!m_anon) {
            draw("Username:"_i18n, " %s", m_user);
            draw("Password:"_i18n, " %s", m_pass);
        }

        if (m_type == NifmInternetConnectionType_WiFi) {
            NifmNetworkProfileData profile{};
            if (R_SUCCEEDED(nifmGetCurrentNetworkProfile(&profile))) {
                const auto& settings = profile.wireless_setting_data;
                std::string passphrase;
                std::transform(std::cbegin(settings.passphrase), std::cend(settings.passphrase), passphrase.begin(), toascii);
                draw("SSID:"_i18n, " %.*s", settings.ssid_len, settings.ssid);
                draw("Passphrase:"_i18n, " %s", passphrase.c_str());
            }
        }
    }

    #undef draw

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
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Failed to install via FTP, press B to exit..."_i18n.c_str());
            break;
    }
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();
}

} // namespace sphaira::ui::menu::ftp
