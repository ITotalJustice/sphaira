#include "ui/scrollable_text.hpp"
#include "app.hpp"
#include "ui/nvg_util.hpp"
#include "log.hpp"
#include <cstring>

namespace sphaira::ui {

ScrollableText::ScrollableText(const std::string& text, float x, float y, float y_clip, float w, float font_size)
: m_font_size{font_size}
, m_y_off_base{y}
, m_y_off{y}
, m_clip_y{y_clip}
, m_end_w{w}
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
            App::PlaySoundEffect(SoundEffect_Scroll);
        }}),
        std::make_pair(Button::LS_UP, Action{[this](){
            if (m_y_off == m_y_off_base) {
                return;
            }
            m_y_off += m_step;
            m_index--;
            App::PlaySoundEffect(SoundEffect_Scroll);
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

    // only draw scrollbar if needed
    if ((m_bounds[3] - m_bounds[1]) > m_clip_y) {
        const auto scrollbar_size = m_clip_y;
        const auto max_index = (m_bounds[3] - m_bounds[1]) / m_step;
        const auto sb_h = 1.f / max_index * scrollbar_size;
        const auto in_clip = m_clip_y / m_step - 1;
        const auto sb_y = m_index;
        // gfx::drawRect(vg, banner_vec.x+banner_vec.w-20, m_y_off_base, 10, scrollbar_size, theme->elements[ThemeEntryID_GRID].colour);
        // gfx::drawRect(vg, banner_vec.x+banner_vec.w-20+2, m_y_off_base + sb_h * sb_y, 10-4, sb_h + (sb_h * in_clip) - 4, theme->elements[ThemeEntryID_TEXT_SELECTED].colour);
        gfx::drawRect(vg, banner_vec.w, m_y_off_base, 10, scrollbar_size, theme->elements[ThemeEntryID_GRID].colour);
        gfx::drawRect(vg, banner_vec.w+2, m_y_off_base + sb_h * sb_y, 10-4, sb_h + (sb_h * in_clip) - 4, theme->elements[ThemeEntryID_TEXT_SELECTED].colour);
    }

    nvgSave(vg);
    nvgScissor(vg, 0, m_y_off_base - m_font_size, 1280, m_clip_y + m_font_size); // clip

    nvgTextLineHeight(App::GetVg(), 1.7);
    gfx::drawTextBox(vg, banner_vec.x + 40, m_y_off, m_font_size, m_bounds[2] - m_bounds[0], theme->elements[ThemeEntryID_TEXT].colour, m_text.c_str());
    nvgRestore(vg);
}

} // namespace sphaira::ui
