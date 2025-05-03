#include "ui/menus/gc_menu.hpp"
#include "yati/yati.hpp"
#include "yati/nx/nca.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"
#include <cstring>

namespace sphaira::ui::menu::gc {
namespace {

const char *g_option_list[] = {
    "Nand Install",
    "SD Card Install",
    "Exit",
};

struct HashStr {
    char str[0x21];
};

HashStr hexIdToStr(auto id) {
    HashStr str{};
    const auto id_lower = std::byteswap(*(u64*)id.c);
    const auto id_upper = std::byteswap(*(u64*)(id.c + 0x8));
    std::snprintf(str.str, 0x21, "%016lx%016lx", id_lower, id_upper);
    return str;
}

// @Gc is the mount point, S is for secure partion, the remaining is the
// the gamecard handle value in lower-case hex.
auto BuildGcPath(const char* name, const FsGameCardHandle* handle, FsGameCardPartition partiton = FsGameCardPartition_Secure) -> fs::FsPath {
    static const char mount_parition[] = {
        [FsGameCardPartition_Update] = 'U',
        [FsGameCardPartition_Normal] = 'N',
        [FsGameCardPartition_Secure] = 'S',
        [FsGameCardPartition_Logo] = 'L',
    };

    fs::FsPath path;
    std::snprintf(path, sizeof(path), "@Gc%c%08x://%s", mount_parition[partiton], handle->value, name);
    return path;
}

Result fsOpenGameCardDetectionEventNotifier(FsEventNotifier* out) {
    return serviceDispatch(fsGetServiceSession(), 501,
        .out_num_objects = 1,
        .out_objects = &out->s
    );
}

auto InRange(u64 off, u64 offset, u64 size) -> bool {
    return off < offset + size && off >= offset;
}

struct GcSource final : yati::source::Base {
    GcSource(const ApplicationEntry& entry, fs::FsNativeGameCard* fs, bool sd_install);
    ~GcSource();
    Result Read(void* buf, s64 off, s64 size, u64* bytes_read);

    yati::container::Collections m_collections{};
    yati::ConfigOverride m_config{};
    fs::FsNativeGameCard* m_fs{};
    FsFile m_file{};
    s64 m_offset{};
    s64 m_size{};
};

GcSource::GcSource(const ApplicationEntry& entry, fs::FsNativeGameCard* fs, bool sd_install)
: m_fs{fs} {
    m_offset = -1;

    s64 offset{};
    const auto add_collections = [&](const auto& collections) {
        for (auto collection : collections) {
            collection.offset = offset;
            m_collections.emplace_back(collection);
            offset += collection.size;
        }
    };

    const auto add_entries = [&](const auto& entries) {
        for (auto& e : entries) {
            add_collections(e);
        }
    };

    // yati can handle all of this for use, however, yati lacks information
    // for ncas until it installs the cnmt and parses it.
    // as we already have this info, we can only send yati what we want to install.
    if (App::GetApp()->m_ticket_only.Get()) {
        add_collections(entry.tickets);
    } else {
        if (!App::GetApp()->m_skip_base.Get()) {
            add_entries(entry.application);
        }
        if (!App::GetApp()->m_skip_patch.Get()) {
            add_entries(entry.patch);
        }
        if (!App::GetApp()->m_skip_addon.Get()) {
            add_entries(entry.add_on);
        }
        if (!App::GetApp()->m_skip_data_patch.Get()) {
            add_entries(entry.data_patch);
        }
        if (!App::GetApp()->m_skip_ticket.Get()) {
            add_collections(entry.tickets);
        }
    }

    // we don't need to verify the nca's, this speeds up installs.
    m_config.sd_card_install = sd_install;
    m_config.skip_nca_hash_verify = true;
    m_config.skip_rsa_header_fixed_key_verify = true;
    m_config.skip_rsa_npdm_fixed_key_verify = true;
}

GcSource::~GcSource() {
    fsFileClose(&m_file);
}

Result GcSource::Read(void* buf, s64 off, s64 size, u64* bytes_read) {
    // check is we need to open a new file.
    if (!InRange(off, m_offset, m_size)) {
        fsFileClose(&m_file);
        m_file = {};

        // find new file based on the offset.
        bool found = false;
        for (auto& collection : m_collections) {
            if (InRange(off, collection.offset, collection.size)) {
                found = true;
                m_offset = collection.offset;
                m_size = collection.size;
                R_TRY(m_fs->OpenFile(fs::AppendPath("/", collection.name), FsOpenMode_Read, &m_file));
                break;
            }
        }

        // this will never fail, unless i break something in yati.
        R_UNLESS(found, 0x1);
    }

    return fsFileRead(&m_file, off - m_offset, buf, size, 0, bytes_read);
}

} // namespace

auto ApplicationEntry::GetSize(const std::vector<GcCollections>& entries) const -> s64 {
    s64 size{};
    for (auto& e : entries) {
        for (auto& collection : e) {
            size += collection.size;
        }
    }
    return size;
}

auto ApplicationEntry::GetSize() const -> s64 {
    s64 size{};
    size += GetSize(application);
    size += GetSize(patch);
    size += GetSize(add_on);
    size += GetSize(data_patch);
    return size;
}

Menu::Menu() : MenuBase{"GameCard"_i18n} {
    this->SetActions(
        std::make_pair(Button::B, Action{"Back"_i18n, [this](){
            SetPop();
        }}),
        std::make_pair(Button::X, Action{"Options"_i18n, [this](){
            App::DisplayInstallOptions(false);
        }})
    );

    const Vec4 v{485, 275, 720, 70};
    const Vec2 pad{0, 125 - v.h};
    m_list = std::make_unique<List>(1, 3, m_pos, v, pad);

    fsOpenDeviceOperator(std::addressof(m_dev_op));
    fsOpenGameCardDetectionEventNotifier(std::addressof(m_event_notifier));
    fsEventNotifierGetEventHandle(std::addressof(m_event_notifier), std::addressof(m_event), true);
}

Menu::~Menu() {
    GcUnmount();
    eventClose(std::addressof(m_event));
    fsEventNotifierClose(std::addressof(m_event_notifier));
    fsDeviceOperatorClose(std::addressof(m_dev_op));
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    // poll for the gamecard first before handling inputs as the gamecard
    // may have been removed, thus pressing A would fail.
    if (R_SUCCEEDED(eventWait(std::addressof(m_event), 0))) {
        GcOnEvent();
    }

    MenuBase::Update(controller, touch);
    m_list->OnUpdate(controller, touch, m_option_index, std::size(g_option_list), [this](bool touch, auto i) {
        if (touch && m_option_index == i) {
            FireAction(Button::A);
        } else {
            App::PlaySoundEffect(SoundEffect_Focus);
            m_option_index = i;
        }
    });
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    #define STORAGE_BAR_W   325
    #define STORAGE_BAR_H   14

    const auto size_sd_gb = (double)m_size_free_sd / 0x40000000;
    const auto size_nand_gb = (double)m_size_free_nand / 0x40000000;

    gfx::drawTextArgs(vg, 490, 135, 23.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "System memory %.1f GB", size_nand_gb);
    gfx::drawRect(vg, 480, 170, STORAGE_BAR_W, STORAGE_BAR_H, theme->GetColour(ThemeEntryID_TEXT));
    gfx::drawRect(vg, 480 + 1, 170 + 1, STORAGE_BAR_W - 2, STORAGE_BAR_H - 2, theme->GetColour(ThemeEntryID_BACKGROUND));
    gfx::drawRect(vg, 480 + 2, 170 + 2, STORAGE_BAR_W - (((double)m_size_free_nand / (double)m_size_total_nand) * STORAGE_BAR_W) - 4, STORAGE_BAR_H - 4, theme->GetColour(ThemeEntryID_TEXT));

    gfx::drawTextArgs(vg, 870, 135, 23.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "microSD card %.1f GB", size_sd_gb);
    gfx::drawRect(vg, 860, 170, STORAGE_BAR_W, STORAGE_BAR_H, theme->GetColour(ThemeEntryID_TEXT));
    gfx::drawRect(vg, 860 + 1, 170 + 1, STORAGE_BAR_W - 2, STORAGE_BAR_H - 2, theme->GetColour(ThemeEntryID_BACKGROUND));
    gfx::drawRect(vg, 860 + 2, 170 + 2, STORAGE_BAR_W - (((double)m_size_free_sd / (double)m_size_total_sd) * STORAGE_BAR_W) - 4, STORAGE_BAR_H - 4, theme->GetColour(ThemeEntryID_TEXT));

    gfx::drawRect(vg, 30, 90, 375, 555, theme->GetColour(ThemeEntryID_GRID));

    if (!m_entries.empty()) {
        const auto& e = m_entries[m_entry_index];
        const auto size = e.GetSize();
        gfx::drawImage(vg, 90, 130, 256, 256, m_icon ? m_icon : App::GetDefaultImage());

        nvgSave(vg);
            nvgIntersectScissor(vg, 50, 90, 325, 555);
            gfx::drawTextArgs(vg, 50, 415, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "%s", m_lang_entry.name);
            gfx::drawTextArgs(vg, 50, 455, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "%s", m_lang_entry.author);
            gfx::drawTextArgs(vg, 50, 495, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "App-ID: 0%lX", e.app_id);
            gfx::drawTextArgs(vg, 50, 535, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "Key-Gen: %u (%s)", e.key_gen, nca::GetKeyGenStr(e.key_gen));
            gfx::drawTextArgs(vg, 50, 575, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "Size: %.2f GB", (double)size / 0x40000000);
            gfx::drawTextArgs(vg, 50, 615, 18.f, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, theme->GetColour(ThemeEntryID_TEXT), "Base: %zu Patch: %zu Addon: %zu Data: %zu", e.application.size(), e.patch.size(), e.add_on.size(), e.data_patch.size());
        nvgRestore(vg);
    }

    m_list->Draw(vg, theme, std::size(g_option_list), [this](auto* vg, auto* theme, auto v, auto i) {
        const auto& [x, y, w, h] = v;
        const auto text_y = y + (h / 2.f);
        auto colour = ThemeEntryID_TEXT;
        if (i == m_option_index) {
            gfx::drawRectOutline(vg, theme, 4.f, v);
            // g_background.selected_bar = create_shape(Colour_Nintendo_Cyan, 90, 230, 4, 45, true);
            // draw_shape_position(&g_background.selected_bar, 485, g_options[i].text->rect.y - 10);
            gfx::drawRect(vg, 490, text_y - 45.f / 2.f, 2, 45, theme->GetColour(ThemeEntryID_TEXT_SELECTED));
            colour = ThemeEntryID_TEXT_SELECTED;
        }
        if (i != 2 && !m_mounted) {
            colour = ThemeEntryID_TEXT_INFO;
        }

        gfx::drawTextArgs(vg, x + 15, y + (h / 2.f), 23.f, NVG_ALIGN_LEFT | NVG_ALIGN_MIDDLE, theme->GetColour(colour), "%s", g_option_list[i]);
    });
}

void Menu::OnFocusGained() {
    MenuBase::OnFocusGained();

    GcOnEvent();
    UpdateStorageSize();
}

Result Menu::GcMount() {
    GcUnmount();

    R_TRY(fsDeviceOperatorGetGameCardHandle(std::addressof(m_dev_op), std::addressof(m_handle)));

    m_fs = std::make_unique<fs::FsNativeGameCard>(std::addressof(m_handle), FsGameCardPartition_Secure, false);
    R_TRY(m_fs->GetFsOpenResult());

    FsDir dir;
    R_TRY(m_fs->OpenDirectory("/", FsDirOpenMode_ReadFiles, std::addressof(dir)));
    ON_SCOPE_EXIT(fsDirClose(std::addressof(dir)));

    s64 count;
    R_TRY(m_fs->DirGetEntryCount(std::addressof(dir), std::addressof(count)));

    std::vector<FsDirectoryEntry> buf(count);
    s64 total_entries;
    R_TRY(m_fs->DirRead(std::addressof(dir), std::addressof(total_entries), buf.size(), buf.data()));
    R_UNLESS(buf.size() == total_entries, 0x1);

    yati::container::Collections ticket_collections;
    for (const auto& e : buf) {
        if (!std::string_view(e.name).ends_with(".tik") && !std::string_view(e.name).ends_with(".cert")) {
            continue;
        }

        ticket_collections.emplace_back(e.name, 0, e.file_size);
    }

    for (const auto& e : buf) {
        // we could use ncm to handle finding all the ncas for us
        // however, we can parse faster than ncm.
        // not only that, the first few calls trying to mount ncm db for
        // the gamecard will fail as it has not yet been parsed (or it's locked?).
        // we could, of course, just wait until ncm is ready, which is about
        // 32ms, but i already have code for manually parsing cnmt so lets re-use it.
        if (!std::string_view(e.name).ends_with(".cnmt.nca")) {
            continue;
        }

        // we don't yet use the header or extended header.
        ncm::PackagedContentMeta header;
        std::vector<u8> extended_header;
        std::vector<NcmPackagedContentInfo> infos;
        const auto path = BuildGcPath(e.name, &m_handle);
        R_TRY(yati::ParseCnmtNca(path, 0, header, extended_header, infos));

        u8 key_gen;
        FsRightsId rights_id;
        R_TRY(fsGetRightsIdAndKeyGenerationByPath(path, FsContentAttributes_All, &key_gen, &rights_id));

        // always add tickets, yati will ignore them if not needed.
        GcCollections collections;
        // add cnmt file.
        collections.emplace_back(e.name, e.file_size, NcmContentType_Meta, 0);

        for (const auto& packed_info : infos) {
            const auto& info = packed_info.info;
            // these don't exist for gamecards, however i may copy/paste this code
            // somewhere so i'm future proofing against myself.
            if (info.content_type == NcmContentType_DeltaFragment) {
                continue;
            }

            // find the nca file, this will never fail for gamecards, see above comment.
            const auto str = hexIdToStr(info.content_id);
            const auto it = std::find_if(buf.cbegin(), buf.cend(), [str](auto& e){
                return !std::strncmp(str.str, e.name, std::strlen(str.str));
            });

            R_UNLESS(it != buf.cend(), yati::Result_NcaNotFound);
            collections.emplace_back(it->name, it->file_size, info.content_type, info.id_offset);
        }

        const auto app_id = ncm::GetAppId(header);
        ApplicationEntry* app_entry{};
        for (auto& app : m_entries) {
            if (app.app_id == app_id) {
                app_entry = &app;
                break;
            }
        }

        if (!app_entry) {
            app_entry = &m_entries.emplace_back(app_id, header.title_version);
        }

        app_entry->version = std::max(app_entry->version, header.title_version);
        app_entry->key_gen = std::max(app_entry->key_gen, key_gen);

        if (header.meta_type == NcmContentMetaType_Application) {
            app_entry->application.emplace_back(collections);
        } else if (header.meta_type == NcmContentMetaType_Patch) {
            app_entry->patch.emplace_back(collections);
        } else if (header.meta_type == NcmContentMetaType_AddOnContent) {
            app_entry->add_on.emplace_back(collections);
        } else if (header.meta_type == NcmContentMetaType_DataPatch) {
            app_entry->data_patch.emplace_back(collections);
        }
    }

    R_UNLESS(m_entries.size(), 0x1);

    // append tickets to every application, yati will ignore if undeeded.
    for (auto& e : m_entries) {
        e.tickets = ticket_collections;
    }

    SetAction(Button::A, Action{"OK"_i18n, [this](){
        if (m_option_index == 2) {
            SetPop();
        } else {
            if (m_mounted) {
                App::Push(std::make_shared<ui::ProgressBox>(m_icon, "Installing "_i18n, m_lang_entry.name, [this](auto pbox) mutable -> bool {
                    auto source = std::make_shared<GcSource>(m_entries[m_entry_index], m_fs.get(), m_option_index == 1);
                    return R_SUCCEEDED(yati::InstallFromCollections(pbox, source, source->m_collections, source->m_config));
                }, [this](bool result){
                    if (result) {
                        App::Notify("Gc install success!"_i18n);
                    } else {
                        App::Notify("Gc install failed!"_i18n);
                    }
                }));
            }
        }
    }});

    if (m_entries.size() > 1) {
        SetAction(Button::L2, Action{"Prev"_i18n, [this](){
            if (m_entry_index != 0) {
                OnChangeIndex(m_entry_index - 1);
            }
        }});
        SetAction(Button::R2, Action{"Next"_i18n, [this](){
            if (m_entry_index < m_entries.size()) {
                OnChangeIndex(m_entry_index + 1);
            }
        }});
    }

    OnChangeIndex(0);
    m_mounted = true;
    R_SUCCEED();
}

void Menu::GcUnmount() {
    m_fs.reset();
    m_entries.clear();
    m_entry_index = 0;
    m_mounted = false;
    m_lang_entry = {};
    FreeImage();

    RemoveAction(Button::L2);
    RemoveAction(Button::R2);
}

Result Menu::GcPoll(bool* inserted) {
    R_TRY(fsDeviceOperatorIsGameCardInserted(&m_dev_op, inserted));

    // if the handle changed, re-mount the game card.
    if (*inserted && m_mounted) {
        FsGameCardHandle handle;
        R_TRY(fsDeviceOperatorGetGameCardHandle(std::addressof(m_dev_op), std::addressof(handle)));
        if (handle.value != m_handle.value) {
            R_TRY(GcMount());
        }
    }

    R_SUCCEED();
}

Result Menu::GcOnEvent() {
    bool inserted{};
    R_TRY(GcPoll(&inserted));

    if (m_mounted != inserted) {
        log_write("gc state changed\n");
        m_mounted = inserted;
        if (m_mounted) {
            log_write("trying to mount\n");
            m_mounted = R_SUCCEEDED(GcMount());
            if (m_mounted) {
                App::PlaySoundEffect(SoundEffect::SoundEffect_Startup);
            }
        } else {
            log_write("trying to unmount\n");
            GcUnmount();
        }
    }

    R_SUCCEED();
}

Result Menu::UpdateStorageSize() {
    fs::FsNativeContentStorage fs_nand{FsContentStorageId_User};
    fs::FsNativeContentStorage fs_sd{FsContentStorageId_SdCard};

    R_TRY(fs_sd.GetFreeSpace("/", &m_size_free_sd));
    R_TRY(fs_sd.GetTotalSpace("/", &m_size_total_sd));
    R_TRY(fs_nand.GetFreeSpace("/", &m_size_free_nand));
    R_TRY(fs_nand.GetTotalSpace("/", &m_size_total_nand));
    R_SUCCEED();
}

void Menu::FreeImage() {
    if (m_icon) {
        nvgDeleteImage(App::GetVg(), m_icon);
        m_icon = 0;
    }
}

void Menu::OnChangeIndex(s64 new_index) {
    FreeImage();
    m_entry_index = new_index;

    const auto index = m_entries.empty() ? 0 : m_entry_index + 1;
    this->SetSubHeading(std::to_string(index) + " / " + std::to_string(m_entries.size()));

    // nsGetApplicationControlData() will fail if it's the first time
    // mounting a gamecard if the image is not already cached.
    // waiting 1-2s after mount, then calling seems to work.
    // however, we can just manually parse the nca to get the data we need,
    // which always works and *is* faster too ;)
    for (auto& e : m_entries[m_entry_index].application) {
        for (auto& collection : e) {
            if (collection.type == NcmContentType_Control) {
                NacpStruct nacp;
                std::vector<u8> icon;
                const auto path = BuildGcPath(collection.name.c_str(), &m_handle);

                u64 program_id = m_entries[m_entry_index].app_id | collection.id_offset;
                if (hosversionAtLeast(17, 0, 0)) {
                    fsGetProgramId(&program_id, path, FsContentAttributes_All);
                }

                if (R_SUCCEEDED(yati::ParseControlNca(path, program_id, &nacp, sizeof(nacp), &icon))) {
                    log_write("managed to parse control nca %s\n", path.s);
                    NacpLanguageEntry* lang_entry{};
                    nacpGetLanguageEntry(&nacp, &lang_entry);

                    if (lang_entry) {
                        m_lang_entry = *lang_entry;
                    }

                    m_icon = nvgCreateImageMem(App::GetVg(), 0, icon.data(), icon.size());
                    if (m_icon > 0) {
                        return;
                    }
                } else {
                    log_write("\tFAILED to parse control nca %s\n", path.s);
                }
            }
        }
    }
}

} // namespace sphaira::ui::menu::gc
