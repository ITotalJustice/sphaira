#include "ui/popup_list.hpp"
#include "ui/nvg_util.hpp"
#include "app.hpp"
#include "i18n.hpp"

namespace sphaira::ui {

PopupList::PopupList(std::string title, Items items, std::string& index_str_ref, std::size_t& index_ref)
: PopupList{std::move(title), std::move(items), Callback{}, index_ref}  {

    m_callback = [&index_str_ref, &index_ref, this](auto op_idx) {
        if (op_idx) {
            index_ref = *op_idx;
            index_str_ref = m_items[index_ref];
        }
    };
}

PopupList::PopupList(std::string title, Items items, std::string& index_ref)
: PopupList{std::move(title), std::move(items), Callback{}}  {

    const auto it = std::find(m_items.cbegin(), m_items.cend(), index_ref);
    if (it != m_items.cend()) {
        m_index = std::distance(m_items.cbegin(), it);
        m_selected_y = m_line_top + 1.f + 42.f + (static_cast<float>(m_index) * m_block.h);
    }

    m_callback = [&index_ref, this](auto op_idx) {
        if (op_idx) {
            index_ref = m_items[*op_idx];
        }
    };
}

PopupList::PopupList(std::string title, Items items, std::size_t& index_ref)
: PopupList{std::move(title), std::move(items), Callback{}, index_ref}  {

    m_callback = [&index_ref, this](auto op_idx) {
        if (op_idx) {
            index_ref = *op_idx;
        }
    };
}

PopupList::PopupList(std::string title, Items items, Callback cb, std::string index)
: PopupList{std::move(title), std::move(items), cb, 0} {

    const auto it = std::find(m_items.cbegin(), m_items.cend(), index);
    if (it != m_items.cend()) {
        m_index = std::distance(m_items.cbegin(), it);
        m_selected_y = m_line_top + 1.f + 42.f + (static_cast<float>(m_index) * m_block.h);
    }
}

PopupList::PopupList(std::string title, Items items, Callback cb, std::size_t index)
: m_title{std::move(title)}
, m_items{std::move(items)}
, m_callback{cb}
, m_index{index} {

    m_pos.w = 1280.f;
    const float a = std::min(405.f, (60.f * static_cast<float>(m_items.size())));
    m_pos.h = 80.f + 140.f + a;
    m_pos.y = 720.f - m_pos.h;
    m_line_top = m_pos.y + 70.f;
    m_line_bottom = 720.f - 73.f;
    m_selected_y = m_line_top + 1.f + 42.f + (static_cast<float>(m_index) * m_block.h);

    m_scrollbar.Setup(Vec4{1220.f, m_line_top, 1.f, m_line_bottom - m_line_top}, m_block.h, m_items.size());

    SetActions(
        std::make_pair(Button::A, Action{"Select"_i18n, [this](){
            if (m_callback) {
                m_callback(m_index);
            }
            SetPop();
        }}),
        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            if (m_callback) {
                m_callback(std::nullopt);
            }
            SetPop();
        }})
    );
}

auto PopupList::Update(Controller* controller, TouchInfo* touch) -> void {
    Widget::Update(controller, touch);

    if (!controller->GotDown(Button::ANY_VERTICAL)) {
        return;
    }

    const auto old_index = m_index;

    if (controller->GotDown(Button::DOWN) && m_index < (m_items.size() - 1)) {
        m_index++;
        m_selected_y += m_block.h;
    } else if (controller->GotDown(Button::UP) && m_index != 0) {
        m_index--;
        m_selected_y -= m_block.h;
    }

    if (old_index != m_index) {
        App::PlaySoundEffect(SoundEffect_Scroll);
        OnLayoutChange();
    }
}

auto PopupList::OnLayoutChange() -> void {
    if ((m_selected_y + m_block.h) > m_line_bottom) {
        m_selected_y -= m_block.h;
        m_index_offset++;
        m_scrollbar.Move(ScrollBar::Direction::DOWN);
    } else if (m_selected_y <= m_line_top) {
        m_selected_y += m_block.h;
        m_index_offset--;
        m_scrollbar.Move(ScrollBar::Direction::UP);
    }
    // LOG("sely: %.2f, index_off: %lu\n", m_selected_y, m_index_offset);
}

auto PopupList::Draw(NVGcontext* vg, Theme* theme) -> void {
    gfx::dimBackground(vg);
    gfx::drawRect(vg, m_pos, theme->elements[ThemeEntryID_SELECTED].colour);
    gfx::drawText(vg, m_pos + m_title_pos, 24.f, theme->elements[ThemeEntryID_TEXT].colour, m_title.c_str());
    gfx::drawRect(vg, 30.f, m_line_top, m_line_width, 1.f, theme->elements[ThemeEntryID_TEXT].colour);
    gfx::drawRect(vg, 30.f, m_line_bottom, m_line_width, 1.f, theme->elements[ThemeEntryID_TEXT].colour);

    // todo: cleanup
    const float x = m_block.x;
    float y = m_line_top + 1.f + 42.f;
    const float h = m_block.h;
    const float w = m_block.w;

    nvgSave(vg);
    nvgScissor(vg, 0, m_line_top, 1280.f, m_line_bottom - m_line_top);

    for (std::size_t i = m_index_offset; i < m_items.size(); ++i) {
        if (m_index == i) {
            gfx::drawRect(vg, x - 4.f, y - 4.f, w + 8.f, h + 8.f, theme->elements[ThemeEntryID_SELECTED_OVERLAY].colour);
            gfx::drawRect(vg, x, y, w, h, theme->elements[ThemeEntryID_SELECTED].colour);
            gfx::drawText(vg, x + m_text_xoffset, y + (h / 2.f), 20.f, m_items[i].c_str(), NULL, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->elements[ThemeEntryID_TEXT_SELECTED].colour);
        } else {
            gfx::drawRect(vg, x, y, w, 1.f, theme->elements[ThemeEntryID_TEXT].colour);
            gfx::drawRect(vg, x, y + h, w, 1.f, theme->elements[ThemeEntryID_TEXT].colour);
            gfx::drawText(vg, x + m_text_xoffset, y + (h / 2.f), 20.f, m_items[i].c_str(), NULL, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->elements[ThemeEntryID_TEXT].colour);
        }
        y += h;
        if (y > m_line_bottom) {
            break;
        }
    }
    nvgRestore(vg);

    m_scrollbar.Draw(vg, theme);
    Widget::Draw(vg, theme);
}

} // namespace sphaira::ui
