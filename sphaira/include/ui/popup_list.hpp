#pragma once

#include "ui/widget.hpp"
#include "ui/scrolling_text.hpp"
#include "ui/list.hpp"
#include <optional>

namespace sphaira::ui {

class PopupList final : public Widget {
public:
    using Items = std::vector<std::string>;
    using Callback = std::function<void(std::optional<s64>)>;

public:
    explicit PopupList(std::string title, Items items, Callback cb, s64 index = 0);
    PopupList(std::string title, Items items, Callback cb, std::string index);
    PopupList(std::string title, Items items, std::string& index_str_ref, s64& index);
    PopupList(std::string title, Items items, std::string& index_ref);
    PopupList(std::string title, Items items, s64& index_ref);

    auto Update(Controller* controller, TouchInfo* touch) -> void override;
    auto Draw(NVGcontext* vg, Theme* theme) -> void override;
    auto OnFocusGained() noexcept -> void override;
    auto OnFocusLost() noexcept -> void override;

private:
    void SetIndex(s64 index);

private:
    static constexpr Vec2 m_title_pos{70.f, 28.f};
    static constexpr Vec4 m_block{280.f, 110.f, 720.f, 60.f};
    static constexpr float m_text_xoffset{15.f};
    static constexpr float m_line_width{1220.f};

    std::string m_title{};
    Items m_items{};
    Callback m_callback{};
    s64 m_index{}; // index in list array
    s64 m_starting_index{};

    std::unique_ptr<List> m_list{};
    ScrollingText m_scroll_text{};

    float m_yoff{};
    float m_line_top{};
    float m_line_bottom{};
};

} // namespace sphaira::ui
