#include "ui/scrollable_text.hpp"
#include "app.hpp"
#include "ui/nvg_util.hpp"
#include "log.hpp"
#include <cstring>

namespace sphaira::ui {

ScrollableText::ScrollableText(const std::string& text, float x, float y, float y_clip, float w, float font_size)
: m_font_size{font_size}
, m_y_off_base{y}
, m_clip_y{y_clip}
, m_end_w{w}
, m_y_off{y}
{
    SetActions(
        std::make_pair(Button::LS_DOWN, Action{[this](){
            const auto bound = m_bounds[3];
            if (bound < m_clip_y) {
                return;
            }
            const auto a = m_y_off_base + m_clip_y;
            const auto norm = m_bounds[3] - m_bounds[1];
            const auto b = m_y_off + norm;
            if (b <= a) {
                return;
            }
            m_y_off -= m_step;
            m_index++;
            App::PlaySoundEffect(SoundEffect::Scroll);
        }}),
        std::make_pair(Button::LS_UP, Action{[this](){
            if (m_y_off == m_y_off_base) {
                return;
            }
            m_y_off += m_step;
            m_index--;
            App::PlaySoundEffect(SoundEffect::Scroll);
        }})
    );

    #if 1
    // converts '\''n' to '\n' without including <regex> because it bloats
    // the binary by over 400 KiB lol
    const auto mini_regex = [](std::string_view str, std::string_view regex, std::string_view replace) -> std::string {
        std::string out;
        out.reserve(str.size());
        u32 i{};

        while (i < str.size()) {
            if ((i + regex.size()) <= str.size() && !std::memcmp(str.data() + i, regex.data(), regex.size())) {
                out.append(replace.data(), replace.size());
                i += regex.size();
            } else {
                out.push_back(str[i]);
                i++;
            }
        }

        return out;
    };

    m_text = mini_regex(text, "\r", "");
    m_text = mini_regex(m_text, "\\n", "\n");
    #else
    m_text = std::regex_replace(text, std::regex("\\\\n"), "\n");
    #endif
    if (m_text.size() > 4096) {
        m_text.resize(4096);
        m_text += "...";
    }

    nvgFontSize(App::GetVg(), m_font_size);
    nvgTextLineHeight(App::GetVg(), 1.7);
    nvgTextBoxBounds(App::GetVg(), 110.0F, m_y_off_base, m_end_w, m_text.c_str(), nullptr, m_bounds);
    // log_write("bounds x: %.2f y: %.2f w: %.2f h: %.2f\n", m_bounds[0], m_bounds[1], m_bounds[2], m_bounds[3]);
}

// void ScrollableText::Update(Controller* controller, TouchInfo* touch) {

// }

void ScrollableText::Draw(NVGcontext* vg, Theme* theme) {
    Widget::Draw(vg, theme);

    const Vec4 line_vec(30, 86, 1220, 646);
    // const Vec4 banner_vec(70, line_vec.y + 20, 848.f, 208.f);
    const Vec4 banner_vec(70, line_vec.y + 20, m_end_w + (110.0F), 208.f);

    const auto max_index = (m_bounds[3] - m_bounds[1]) / m_step;
    gfx::drawScrollbar2(vg, theme, banner_vec.w + 25, m_y_off_base, m_clip_y, m_index, max_index, 1, m_clip_y / m_step - 1);

    nvgSave(vg);
    nvgIntersectScissor(vg, 0, m_y_off_base - m_font_size, 1280, m_clip_y + m_font_size); // clip

    nvgTextLineHeight(App::GetVg(), 1.7);
    gfx::drawTextBox(vg, banner_vec.x + 40, m_y_off, m_font_size, m_bounds[2] - m_bounds[0], theme->GetColour(ThemeEntryID_TEXT), m_text.c_str());
    nvgRestore(vg);
}

} // namespace sphaira::ui
