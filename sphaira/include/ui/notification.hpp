#pragma once

#include "object.hpp"

#include <deque>

namespace sphaira::ui {

class NotifEntry final : public Object {
public:
    enum class Side { LEFT, RIGHT };

public:
    NotifEntry(const std::string& text, Side side);

    auto Draw(NVGcontext* vg, Theme* theme, float y) -> bool;
    auto GetSide() const noexcept { return m_side; }
    auto IsDone() const noexcept { return m_count == 0; }

private:
    void Draw(NVGcontext* vg, Theme* theme) override;

private:
    std::string m_text;
    Side m_side;
    std::size_t m_count{180}; // count down to zero
    bool m_bounds_measured{};
};

class NotifMananger final : public Object {
public:
    void Draw(NVGcontext* vg, Theme* theme) override;

    void Push(const NotifEntry& entry);
    void Pop(NotifEntry::Side side);
    void Clear(NotifEntry::Side side);
    void Clear();

private:
    using Entries = std::deque<NotifEntry>;

private:
    void Draw(NVGcontext* vg, Theme* theme, Entries& entries);
    auto GetEntries(NotifEntry::Side side) -> Entries&;

private:
    Entries m_entries_left{};
    Entries m_entries_right{};
    Mutex m_mutex{};
};

} // namespace sphaira::ui
