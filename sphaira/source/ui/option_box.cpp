#include "ui/option_box.hpp"
#include "ui/nvg_util.hpp"
#include "app.hpp"

namespace sphaira::ui {

OptionBoxEntry::OptionBoxEntry(const std::string& text, const Vec4& pos)
: m_text{text} {
    m_pos = pos;
    m_text_pos = Vec2{m_pos.x + (m_pos.w / 2.f), m_pos.y + (m_pos.h / 2.f)};
}

auto OptionBoxEntry::Draw(NVGcontext* vg, Theme* theme) -> void {
    if (m_selected) {
        gfx::drawRectOutline(vg, theme, 4.f, m_pos);
        gfx::drawText(vg, m_text_pos, 26.f, theme->GetColour(ThemeEntryID_TEXT_SELECTED), m_text.c_str(), NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE);
    } else {
        gfx::drawText(vg, m_text_pos, 26.f, theme->GetColour(ThemeEntryID_TEXT), m_text.c_str(), NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE);
    }
}

auto OptionBoxEntry::Selected(bool enable) -> void {
    m_selected = enable;
}

OptionBox::OptionBox(const std::string& message, const Option& a, const Callback& cb, int image, bool own_image)
: m_message{message}
, m_callback{cb}
, m_image{image}
, m_own_image{own_image} {

    m_pos.w = 770.f;
    m_pos.h = 295.f;
    m_pos.x = (SCREEN_WIDTH / 2.f) - (m_pos.w / 2.f);
    m_pos.y = (SCREEN_HEIGHT / 2.f) - (m_pos.h / 2.f);

    auto box = m_pos;
    box.y += 220.f;
    box.h -= 220.f;
    m_entries.emplace_back(a, box);

    Setup(0);
}

OptionBox::OptionBox(const std::string& message, const Option& a, const Option& b, const Callback& cb, int image, bool own_image)
: OptionBox{message, a, b, 0, cb, image, own_image} {

}

OptionBox::OptionBox(const std::string& message, const Option& a, const Option& b, s64 index, const Callback& cb, int image, bool own_image)
: m_message{message}
, m_callback{cb}
, m_image{image}
, m_own_image{own_image} {

    m_pos.w = 770.f;
    m_pos.h = 295.f;
    m_pos.x = (SCREEN_WIDTH / 2.f) - (m_pos.w / 2.f);
    m_pos.y = (SCREEN_HEIGHT / 2.f) - (m_pos.h / 2.f);

    auto box = m_pos;
    box.w /= 2.f;
    box.y += 220.f;
    box.h -= 220.f;
    m_entries.emplace_back(a, box);
    box.x += box.w;
    m_entries.emplace_back(b, box);

    Setup(index);
}

OptionBox::~OptionBox() {
    if (m_image && m_own_image) {
        nvgDeleteImage(App::GetVg(), m_image);
    }
}

auto OptionBox::Update(Controller* controller, TouchInfo* touch) -> void {
    Widget::Update(controller, touch);

    if (touch->is_clicked) {
        for (s64 i = 0; i < m_entries.size(); i++) {
            auto& e = m_entries[i];
            if (touch->in_range(e.GetPos())) {
                SetIndex(i);
                FireAction(Button::A);
                break;
            }
        }
    }
}

auto OptionBox::Draw(NVGcontext* vg, Theme* theme) -> void {
    gfx::dimBackground(vg);
    gfx::drawRect(vg, m_pos, theme->GetColour(ThemeEntryID_POPUP), 5);

    nvgSave(vg);
    nvgTextLineHeight(vg, 1.5);
    if (m_image) {
        Vec4 image{m_pos};
        image.x += 40;
        image.y += 40;
        image.w = 150;
        image.h = 150;

        const float padding = 40;
        gfx::drawImage(vg, image, m_image, 5);
        gfx::drawTextBox(vg, image.x + image.w + padding, m_pos.y + 110.f, 22.f, m_pos.w - (image.x - m_pos.x) - image.w - padding*2, theme->GetColour(ThemeEntryID_TEXT), m_message.c_str(), NVG_ALIGN_LEFT | NVG_ALIGN_BASELINE);
    } else {
        const float padding = 30;
        gfx::drawTextBox(vg, m_pos.x + padding, m_pos.y + 110.f, 24.f, m_pos.w - padding*2, theme->GetColour(ThemeEntryID_TEXT), m_message.c_str(), NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE);
    }
    nvgRestore(vg);

    gfx::drawRect(vg, m_spacer_line, theme->GetColour(ThemeEntryID_LINE_SEPARATOR));

    for (auto&p: m_entries) {
        p.Draw(vg, theme);
    }
}

auto OptionBox::OnFocusGained() noexcept -> void {
    Widget::OnFocusGained();
    SetHidden(false);
}

auto OptionBox::OnFocusLost() noexcept -> void {
    Widget::OnFocusLost();
    SetHidden(true);
}

auto OptionBox::Setup(s64 index) -> void {
    m_index = std::min<s64>(m_entries.size() - 1, index);
    m_entries[m_index].Selected(true);
    m_spacer_line = Vec4{m_pos.x, m_pos.y + 220.f - 2.f, m_pos.w, 2.f};

    SetActions(
        std::make_pair(Button::LEFT, Action{[this](){
            if (m_index) {
                SetIndex(m_index - 1);
            }
        }}),
        std::make_pair(Button::RIGHT, Action{[this](){
            if (m_index < (m_entries.size() - 1)) {
                SetIndex(m_index + 1);
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

void OptionBox::SetIndex(s64 index) {
    if (m_index != index) {
        m_entries[m_index].Selected(false);
        m_index = index;
        m_entries[m_index].Selected(true);
    }
}

} // namespace sphaira::ui
