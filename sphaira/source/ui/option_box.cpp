#include "ui/option_box.hpp"
#include "ui/nvg_util.hpp"
#include "app.hpp"

namespace sphaira::ui {

OptionBoxEntry::OptionBoxEntry(const std::string& text, Vec4 pos)
: m_text{text} {
    m_pos = pos;
    m_text_pos = Vec2{m_pos.x + (m_pos.w / 2.f), m_pos.y + (m_pos.h / 2.f)};
}

auto OptionBoxEntry::Draw(NVGcontext* vg, Theme* theme) -> void {
    if (m_selected) {
        gfx::drawRectOutline(vg, 4.f, theme->elements[ThemeEntryID_SELECTED_OVERLAY].colour, m_pos, theme->elements[ThemeEntryID_SELECTED].colour);
        gfx::drawText(vg, m_text_pos, 26.f, theme->elements[ThemeEntryID_TEXT_SELECTED].colour, m_text.c_str(), NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE);
    } else {
        gfx::drawText(vg, m_text_pos, 26.f, theme->elements[ThemeEntryID_TEXT].colour, m_text.c_str(), NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE);
    }
}

auto OptionBoxEntry::Selected(bool enable) -> void {
    m_selected = enable;
}

OptionBox::OptionBox(const std::string& message, const Option& a, Callback cb)
: m_message{message}
, m_callback{cb} {

    m_pos.w = 770.f;
    m_pos.h = 295.f;
    m_pos.x = (1280.f / 2.f) - (m_pos.w / 2.f);
    m_pos.y = (720.f / 2.f) - (m_pos.h / 2.f);

    auto box = m_pos;
    box.y += 220.f;
    box.h -= 220.f;
    m_entries.emplace_back(a, box);

    Setup(0);
}

OptionBox::OptionBox(const std::string& message, const Option& a, const Option& b, Callback cb)
: OptionBox{message, a, b, 0, cb} {

}

OptionBox::OptionBox(const std::string& message, const Option& a, const Option& b, std::size_t index, Callback cb)
: m_message{message}
, m_callback{cb} {

    m_pos.w = 770.f;
    m_pos.h = 295.f;
    m_pos.x = (1280.f / 2.f) - (m_pos.w / 2.f);
    m_pos.y = (720.f / 2.f) - (m_pos.h / 2.f);

    auto box = m_pos;
    box.w /= 2.f;
    box.y += 220.f;
    box.h -= 220.f;
    m_entries.emplace_back(a, box);
    box.x += box.w;
    m_entries.emplace_back(b, box);

    Setup(index);
}

OptionBox::OptionBox(const std::string& message, const Option& a, const Option& b, const Option& c, Callback cb)
: OptionBox{message, a, b, c, 0, cb} {

}

OptionBox::OptionBox(const std::string& message, const Option& a, const Option& b, const Option& c, std::size_t index, Callback cb)
: m_message{message}
, m_callback{cb} {

}

auto OptionBox::Update(Controller* controller, TouchInfo* touch) -> void {
    Widget::Update(controller, touch);

    // if (!controller->GotDown(Button::ANY_HORIZONTAL)) {
    //     return;
    // }

    // const auto old_index = m_index;

    // if (controller->GotDown(Button::LEFT) && m_index) {
    //     m_index--;
    // } else if (controller->GotDown(Button::RIGHT) && m_index < (m_entries.size() - 1)) {
    //     m_index++;
    // }

    // if (old_index != m_index) {
    //     m_entries[old_index].Selected(false);
    //     m_entries[m_index].Selected(true);
    // }
}

auto OptionBox::OnLayoutChange() -> void {

}

auto OptionBox::Draw(NVGcontext* vg, Theme* theme) -> void {
    gfx::dimBackground(vg);
    gfx::drawRect(vg, m_pos, theme->elements[ThemeEntryID_SELECTED].colour);
    gfx::drawText(vg, {m_pos.x + (m_pos.w / 2.f), m_pos.y + 110.f}, 26.f, theme->elements[ThemeEntryID_TEXT].colour, m_message.c_str(), NVG_ALIGN_CENTER | NVG_ALIGN_TOP);
    gfx::drawRect(vg, m_spacer_line, theme->elements[ThemeEntryID_TEXT].colour);

    for (auto&p: m_entries) {
        p.Draw(vg, theme);
    }
}

auto OptionBox::Setup(std::size_t index) -> void {
    m_index = std::min(m_entries.size() - 1, index);
    m_entries[m_index].Selected(true);
    m_spacer_line = Vec4{m_pos.x, m_pos.y + 220.f - 2.f, m_pos.w, 2.f};

    SetActions(
        std::make_pair(Button::LEFT, Action{[this](){
            if (m_index) {
                m_entries[m_index].Selected(false);
                m_index--;
                m_entries[m_index].Selected(true);
                App::PlaySoundEffect(SoundEffect_Focus);
            }
        }}),
        std::make_pair(Button::RIGHT, Action{[this](){
            if (m_index < (m_entries.size() - 1)) {
                m_entries[m_index].Selected(false);
                m_index++;
                m_entries[m_index].Selected(true);
                App::PlaySoundEffect(SoundEffect_Focus);
            }
        }}),
        std::make_pair(Button::A, Action{[this](){
            m_callback(m_index);
            SetPop();
        }}),
        std::make_pair(Button::B, Action{[this](){
            m_callback({});
            SetPop();
        }})
    );
}

} // namespace sphaira::ui
