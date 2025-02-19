#include "app.hpp"
#include "log.hpp"
#include "ui/menus/menu_base.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"

namespace sphaira::ui::menu {

MenuBase::MenuBase(std::string title) : m_title{title} {
    // this->SetParent(this);
    this->SetPos(30, 87, 1220 - 30, 646 - 87);
    SetAction(Button::START, Action{App::Exit});
    UpdateVars();
}

MenuBase::~MenuBase() {
}

void MenuBase::Update(Controller* controller, TouchInfo* touch) {
    Widget::Update(controller, touch);

    // update every second.
    if (m_poll_timestamp.GetSeconds() >= 1) {
        UpdateVars();
    }
}

void MenuBase::Draw(NVGcontext* vg, Theme* theme) {
    DrawElement(0, 0, SCREEN_WIDTH, SCREEN_HEIGHT, ThemeEntryID_BACKGROUND);
    Widget::Draw(vg, theme);

    const float start_y = 70;
    const float font_size = 22;
    const float spacing = 30;

    float start_x = 1220;
    float bounds[4];

    nvgFontSize(vg, font_size);

    #define draw(colour, ...) \
        gfx::textBounds(vg, 0, 0, bounds, __VA_ARGS__); \
        start_x -= bounds[2] - bounds[0]; \
        gfx::drawTextArgs(vg, start_x, start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_BOTTOM, theme->GetColour(colour), __VA_ARGS__); \
        start_x -= spacing;

    // draw("version %s", APP_VERSION);
    draw(ThemeEntryID_TEXT, "%u\uFE6A", m_battery_percetange);

    if (App::Get12HourTimeEnable()) {
        draw(ThemeEntryID_TEXT, "%02u:%02u:%02u %s", (m_tm.tm_hour == 0 || m_tm.tm_hour == 12) ? 12 : m_tm.tm_hour % 12, m_tm.tm_min, m_tm.tm_sec, (m_tm.tm_hour < 12) ? "AM" : "PM");
    } else {
        draw(ThemeEntryID_TEXT, "%02u:%02u:%02u", m_tm.tm_hour, m_tm.tm_min, m_tm.tm_sec);
    }

    if (m_ip) {
        draw(ThemeEntryID_TEXT, "%u.%u.%u.%u", m_ip&0xFF, (m_ip>>8)&0xFF, (m_ip>>16)&0xFF, (m_ip>>24)&0xFF);
    } else {
        draw(ThemeEntryID_TEXT, ("No Internet"_i18n).c_str());
    }
    if (!App::IsApplication()) {
        draw(ThemeEntryID_ERROR, ("[Applet Mode]"_i18n).c_str());
    }

    #undef draw

    gfx::drawRect(vg, 30.f, 86.f, 1220.f, 1.f, theme->GetColour(ThemeEntryID_LINE));
    gfx::drawRect(vg, 30.f, 646.0f, 1220.f, 1.f, theme->GetColour(ThemeEntryID_LINE));

    nvgFontSize(vg, 28);
    gfx::textBounds(vg, 0, 0, bounds, m_title.c_str());
    gfx::drawTextArgs(vg, 80, start_y, 28.f, NVG_ALIGN_LEFT | NVG_ALIGN_BOTTOM, theme->GetColour(ThemeEntryID_TEXT), m_title.c_str());
    gfx::drawTextArgs(vg, 80 + (bounds[2] - bounds[0]) + 10, start_y, 16, NVG_ALIGN_LEFT | NVG_ALIGN_BOTTOM, theme->GetColour(ThemeEntryID_TEXT_INFO), m_title_sub_heading.c_str());

    gfx::drawTextArgs(vg, 80, 685.f, 18, NVG_ALIGN_LEFT, theme->GetColour(ThemeEntryID_TEXT), "%s", m_sub_heading.c_str());
}

void MenuBase::SetTitle(std::string title) {
    m_title = title;
}

void MenuBase::SetTitleSubHeading(std::string sub_heading) {
    m_title_sub_heading = sub_heading;
}

void MenuBase::SetSubHeading(std::string sub_heading) {
    m_sub_heading = sub_heading;
}

void MenuBase::UpdateVars() {
    m_tm = {};
    m_poll_timestamp = {};
    m_battery_percetange = {};
    m_charger_type = {};
    m_type = {};
    m_status = {};
    m_strength = {};
    m_ip = {};

    const auto t = time(NULL);
    localtime_r(&t, &m_tm);
    psmGetBatteryChargePercentage(&m_battery_percetange);
    psmGetChargerType(&m_charger_type);
    nifmGetInternetConnectionStatus(&m_type, &m_strength, &m_status);
    nifmGetCurrentIpAddress(&m_ip);

    m_poll_timestamp.Update();
}

} // namespace sphaira::ui::menu
