#pragma once

#include "ui/menus/menu_base.hpp"
#include "yati/container/base.hpp"
#include "yati/source/base.hpp"
#include "ui/list.hpp"
#include <span>
#include <memory>

namespace sphaira::ui::menu::gc {

struct GcCollection : yati::container::CollectionEntry {
    GcCollection(const char* _name, s64 _size, u8 _type, u8 _id_offset) {
        name = _name;
        size = _size;
        type = _type;
        id_offset = _id_offset;
    }

    // NcmContentType
    u8 type{};
    u8 id_offset{};
};

using GcCollections = std::vector<GcCollection>;

struct ApplicationEntry {
    u64 app_id{};
    u32 version{};
    u8 key_gen{};

    std::vector<GcCollections> application{};
    std::vector<GcCollections> patch{};
    std::vector<GcCollections> add_on{};
    std::vector<GcCollections> data_patch{};
    yati::container::Collections tickets{};

    auto GetSize() const -> s64;
    auto GetSize(const std::vector<GcCollections>& entries) const -> s64;
};

struct Menu final : MenuBase {
    Menu();
    ~Menu();

    void Update(Controller* controller, TouchInfo* touch) override;
    void Draw(NVGcontext* vg, Theme* theme) override;

private:
    Result GcMount();
    void GcUnmount();
    Result GcPoll(bool* inserted);
    Result GcOnEvent();
    Result UpdateStorageSize();

    void FreeImage();
    void OnChangeIndex(s64 new_index);

private:
    FsDeviceOperator m_dev_op{};
    FsGameCardHandle m_handle{};
    std::unique_ptr<fs::FsNativeGameCard> m_fs{};
    FsEventNotifier m_event_notifier{};
    Event m_event{};

    std::vector<ApplicationEntry> m_entries{};
    std::unique_ptr<List> m_list{};
    s64 m_entry_index{};
    s64 m_option_index{};

    s64 m_size_free_sd{};
    s64 m_size_total_sd{};
    s64 m_size_free_nand{};
    s64 m_size_total_nand{};
    NacpLanguageEntry m_lang_entry{};
    int m_icon{};
    bool m_mounted{};
};

} // namespace sphaira::ui::menu::gc
