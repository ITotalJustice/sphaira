#include "ui/error_box.hpp"
#include "ui/nvg_util.hpp"
#include "app.hpp"
#include "i18n.hpp"

namespace sphaira::ui {

ErrorBox::ErrorBox(const std::string& message) : m_message{message} {

    m_pos.w = 770.f;
    m_pos.h = 430.f;
    m_pos.x = 255;
    m_pos.y = 145;

    SetAction(Button::A, Action{[this](){
        SetPop();
    }});

    App::PlaySoundEffect(SoundEffect::SoundEffect_Error);
}

ErrorBox::ErrorBox(Result code, const std::string& message) : ErrorBox{message} {
    m_code = code;
}

auto ErrorBox::Update(Controller* controller, TouchInfo* touch) -> void {
    Widget::Update(controller, touch);
}

auto ErrorBox::Draw(NVGcontext* vg, Theme* theme) -> void {
    gfx::dimBackground(vg);
    gfx::drawRect(vg, m_pos, theme->GetColour(ThemeEntryID_POPUP));

    const Vec4 box = { 455, 470, 365, 65 };
    const auto center_x = m_pos.x + m_pos.w/2;

    gfx::drawTextArgs(vg, center_x, 180, 63, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_ERROR), "\uE140");
    if (m_code.has_value()) {
        const auto code = m_code.value();
        gfx::drawTextArgs(vg, center_x, 270, 25, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "Code: 0x%X Module: %u Description: 0x%X", R_VALUE(code), R_MODULE(code), R_DESCRIPTION(code));
    } else {
        gfx::drawTextArgs(vg, center_x, 270, 25, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "An error occurred"_i18n.c_str());
    }
    gfx::drawTextArgs(vg, center_x, 325, 23, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "%s", m_message.c_str());
    gfx::drawTextArgs(vg, center_x, 380, 20, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT_INFO), "If this message appears repeatedly, please open an issue."_i18n.c_str());
    gfx::drawTextArgs(vg, center_x, 415, 20, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT_INFO), "https://github.com/ITotalJustice/sphaira/issues");
    gfx::drawRectOutline(vg, theme, 4.f, box);
    gfx::drawTextArgs(vg, center_x, box.y + box.h/2, 23, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_SELECTED), "OK"_i18n.c_str());
}

} // namespace sphaira::ui
