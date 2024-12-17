#include "app.hpp"
#include "log.hpp"
#include "ui/menus/menu_base.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"

namespace sphaira::ui::menu {

MenuBase::MenuBase(std::string title) : m_title{title} {
    // this->SetParent(this);
    this->SetPos(30, 87, 1220 - 30, 646 - 87);
    m_applet_type = appletGetAppletType();
    SetAction(Button::START, Action{App::Exit});
}

MenuBase::~MenuBase() {
}

void MenuBase::Update(Controller* controller, TouchInfo* touch) {
    Widget::Update(controller, touch);
}

void MenuBase::Draw(NVGcontext* vg, Theme* theme) {
    DrawElement(0, 0, SCREEN_WIDTH, SCREEN_HEIGHT, ThemeEntryID_BACKGROUND);
    Widget::Draw(vg, theme);

    u32 battery_percetange{};

    PsmChargerType charger_type{};
    NifmInternetConnectionType type{};
    NifmInternetConnectionStatus status{};
    u32 strength{};
    u32 ip{};

    const auto _time = time(NULL);
    struct tm tm{};
    const auto gmt = gmtime(&_time);
    if (gmt) {
        tm = *gmt;
    }

    // todo: app thread poll every 1s and this query the result
    psmGetBatteryChargePercentage(&battery_percetange);
    psmGetChargerType(&charger_type);
    nifmGetInternetConnectionStatus(&type, &strength, &status);
    nifmGetCurrentIpAddress(&ip);

    const float start_y = 70;
    const float font_size = 22;
    const float spacing = 30;

    float start_x = 1220;
    float bounds[4];

    nvgFontSize(vg, font_size);

    #define draw(...) \
        gfx::textBounds(vg, 0, 0, bounds, __VA_ARGS__); \
        start_x -= bounds[2] - bounds[0]; \
        gfx::drawTextArgs(vg, start_x, start_y, font_size, NVG_ALIGN_LEFT | NVG_ALIGN_BOTTOM, theme->elements[ThemeEntryID_TEXT].colour, __VA_ARGS__); \
        start_x -= spacing;

    // draw("version %s", APP_VERSION);
    draw("%u\uFE6A", battery_percetange);
    draw("%02u:%02u:%02u", tm.tm_hour, tm.tm_min, tm.tm_sec);
    if (ip) {
        draw("%u.%u.%u.%u", ip&0xFF, (ip>>8)&0xFF, (ip>>16)&0xFF, (ip>>24)&0xFF);
    } else {
        draw(("No Internet"_i18n).c_str());
    }
    if (m_applet_type == AppletType_LibraryApplet || m_applet_type == AppletType_SystemApplet) {
        draw(("[Applet Mode]"_i18n).c_str());
    }

    #undef draw

    gfx::drawRect(vg, 30.f, 86.f, 1220.f, 1.f, theme->elements[ThemeEntryID_TEXT].colour);
    gfx::drawRect(vg, 30.f, 646.0f, 1220.f, 1.f, theme->elements[ThemeEntryID_TEXT].colour);

    nvgFontSize(vg, 28);
    gfx::textBounds(vg, 0, 0, bounds, m_title.c_str());
    gfx::drawTextArgs(vg, 80, start_y, 28.f, NVG_ALIGN_LEFT | NVG_ALIGN_BOTTOM, theme->elements[ThemeEntryID_TEXT].colour, m_title.c_str());
    gfx::drawTextArgs(vg, 80 + (bounds[2] - bounds[0]) + 10, start_y, 16, NVG_ALIGN_LEFT | NVG_ALIGN_BOTTOM, theme->elements[ThemeEntryID_TEXT].colour, m_title_sub_heading.c_str());

    // gfx::drawTextArgs(vg, 80, 65, 28.f, NVG_ALIGN_LEFT, theme->elements[ThemeEntryID_TEXT].colour, m_title.c_str());
    // gfx::drawTextArgs(vg, 80, 680.f, 18, NVG_ALIGN_LEFT, theme->elements[ThemeEntryID_TEXT].colour, "%s", m_sub_heading.c_str());
    gfx::drawTextArgs(vg, 80, 685.f, 18, NVG_ALIGN_LEFT, theme->elements[ThemeEntryID_TEXT].colour, "%s", m_sub_heading.c_str());
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

} // namespace sphaira::ui::menu
