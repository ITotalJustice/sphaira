#pragma once

#include "ui/widget.hpp"
#include <optional>

namespace sphaira::ui {

class OptionBoxEntry final : public Widget {
public:

public:
    OptionBoxEntry(const std::string& text, const Vec4& pos);

    auto Update(Controller* controller, TouchInfo* touch) -> void override {}
    auto Draw(NVGcontext* vg, Theme* theme) -> void override;

    auto Selected(bool enable) -> void;
private:

private:
    std::string m_text{};
    Vec2 m_text_pos{};
    bool m_selected{false};
};

// todo: support multiline messages
// todo: support upto 4 options.
class OptionBox final : public Widget {
public:
    using Callback = std::function<void(std::optional<s64> index)>;
    using Option = std::string;
    using Options = std::vector<Option>;

public:
    OptionBox(const std::string& message, const Option& a, const Callback& cb = [](auto){}, int image = 0, bool own_image = false); // confirm
    OptionBox(const std::string& message, const Option& a, const Option& b, const Callback& cb, int image = 0, bool own_image = false); // yesno
    OptionBox(const std::string& message, const Option& a, const Option& b, s64 index, const Callback& cb, int image = 0, bool own_image = false); // yesno
    ~OptionBox();

    auto Update(Controller* controller, TouchInfo* touch) -> void override;
    auto Draw(NVGcontext* vg, Theme* theme) -> void override;
    auto OnFocusGained() noexcept -> void override;
    auto OnFocusLost() noexcept -> void override;

private:
    auto Setup(s64 index) -> void; // common setup values
    void SetIndex(s64 index);

private:
    const std::string m_message;
    const Callback m_callback;
    const int m_image;
    const bool m_own_image;

    Vec4 m_spacer_line{};

    s64 m_index{};
    std::vector<OptionBoxEntry> m_entries{};
};

} // namespace sphaira::ui
