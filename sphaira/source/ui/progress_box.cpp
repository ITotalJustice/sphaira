#include "ui/progress_box.hpp"
#include "ui/option_box.hpp"
#include "ui/nvg_util.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "i18n.hpp"

namespace sphaira::ui {
namespace {

void threadFunc(void* arg) {
    auto d = static_cast<ProgressBox::ThreadData*>(arg);
    d->result = d->callback(d->pbox);
    d->pbox->RequestExit();
}

} // namespace

ProgressBox::ProgressBox(const std::string& title, ProgressBoxCallback callback, ProgressBoxDoneCallback done, int cpuid, int prio, int stack_size) {
    SetAction(Button::B, Action{"Back"_i18n, [this](){
        App::Push(std::make_shared<OptionBox>("Are you sure you wish to cancel?"_i18n, "No"_i18n, "Yes"_i18n, 1, [this](auto op_index){
            if (op_index && *op_index) {
                RequestExit();
                SetPop();
            }
        }));
    }});

    m_pos.w = 770.f;
    m_pos.h = 430.f;
    m_pos.x = 255;
    m_pos.y = 145;

    m_done = done;
    m_title = title;

    m_thread_data.pbox = this;
    m_thread_data.callback = callback;
    if (R_FAILED(threadCreate(&m_thread, threadFunc, &m_thread_data, nullptr, stack_size, prio, cpuid))) {
        log_write("failed to create thead\n");
    }
    if (R_FAILED(threadStart(&m_thread))) {
        log_write("failed to start thread\n");
    }
}

ProgressBox::~ProgressBox() {
    mutexLock(&m_mutex);
    m_exit_requested = true;
    mutexUnlock(&m_mutex);

    if (R_FAILED(threadWaitForExit(&m_thread))) {
        log_write("failed to join thread\n");
    }
    if (R_FAILED(threadClose(&m_thread))) {
        log_write("failed to close thread\n");
    }

    m_done(m_thread_data.result);
}

auto ProgressBox::Update(Controller* controller, TouchInfo* touch) -> void {
    Widget::Update(controller, touch);

    if (ShouldExit()) {
        SetPop();
    }
}

auto ProgressBox::Draw(NVGcontext* vg, Theme* theme) -> void {
    mutexLock(&m_mutex);
    const auto title = m_title;
    const auto transfer = m_transfer;
    const auto size = m_size;
    const auto offset = m_offset;
    mutexUnlock(&m_mutex);

    gfx::dimBackground(vg);
    gfx::drawRect(vg, m_pos, theme->elements[ThemeEntryID_SELECTED].colour);

    // The pop up shape.
    // const Vec4 box = { 255, 145, 770, 430 };
    const Vec4 prog_bar = { 400, 470, 480, 12 };
    const auto center_x = m_pos.x + m_pos.w/2;

    // shapes.
    if (offset && size) {
        gfx::drawRect(vg, prog_bar, gfx::Colour::SILVER);
        const u32 percentage = ((double)offset / (double)size) * 100.0;
        gfx::drawRect(vg, prog_bar.x, prog_bar.y, ((float)offset / (float)size) * prog_bar.w, prog_bar.h, gfx::Colour::CYAN);
        // gfx::drawTextArgs(vg, prog_bar.x + 85, prog_bar.y + 40, 20, 0, gfx::Colour::WHITE, "%u%%", percentage);
        gfx::drawTextArgs(vg, prog_bar.x + prog_bar.w + 10, prog_bar.y, 20, NVG_ALIGN_LEFT | NVG_ALIGN_TOP, gfx::Colour::WHITE, "%u%%", percentage);
    }

    gfx::drawTextArgs(vg, center_x, 200, 25, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, gfx::Colour::WHITE, title.c_str());
    // gfx::drawTextArgs(vg, center_x, 260, 20, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, gfx::Colour::SILVER, "Please do not remove the gamecard or");
    // gfx::drawTextArgs(vg, center_x, 295, 20, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, gfx::Colour::SILVER, "power off the system whilst installing.");
    // gfx::drawTextArgs(vg, center_x, 360, 20, NVG_ALIGN_CENTER | NVG_ALIGN_TOP, gfx::Colour::WHITE, "%.2f MiB/s", 24.0);
    if (!transfer.empty()) {
        gfx::drawTextArgs(vg, center_x, 420, 20, NVG_ALIGN_CENTER, gfx::Colour::WHITE, "%s", transfer.c_str());
    }
}

auto ProgressBox::NewTransfer(const std::string& transfer)  -> ProgressBox& {
    mutexLock(&m_mutex);
    m_transfer = transfer;
    m_size = 0;
    m_offset = 0;
    mutexUnlock(&m_mutex);
    Yield();
    return *this;
}

auto ProgressBox::UpdateTransfer(u64 offset, u64 size)  -> ProgressBox& {
    mutexLock(&m_mutex);
    m_size = size;
    m_offset = offset;
    mutexUnlock(&m_mutex);
    Yield();
    return *this;
}

void ProgressBox::RequestExit() {
    mutexLock(&m_mutex);
    m_exit_requested = true;
    mutexUnlock(&m_mutex);
}

auto ProgressBox::ShouldExit() -> bool {
    mutexLock(&m_mutex);
    const auto exit_requested = m_exit_requested;
    mutexUnlock(&m_mutex);
    return exit_requested;
}

auto ProgressBox::CopyFile(const fs::FsPath& src_path, const fs::FsPath& dst_path) -> Result {
    fs::FsNativeSd fs;
    R_TRY(fs.GetFsOpenResult());

    FsFile src_file;
    R_TRY(fs.OpenFile(src_path, FsOpenMode_Read, &src_file));
    ON_SCOPE_EXIT(fsFileClose(&src_file));

    s64 src_size;
    R_TRY(fsFileGetSize(&src_file, &src_size));

    // this can fail if it already exists so we ignore the result.
    // if the file actually failed to be created, the result is implicitly
    // handled when we try and open it for writing.
    fs.CreateFile(dst_path, src_size, 0);

    FsFile dst_file;
    R_TRY(fs.OpenFile(dst_path, FsOpenMode_Write, &dst_file));
    ON_SCOPE_EXIT(fsFileClose(&dst_file));

    R_TRY(fsFileSetSize(&dst_file, src_size));

    s64 offset{};
    std::vector<u8> buf(1024*1024*8); // 8MiB

    while (offset < src_size) {
        if (ShouldExit()) {
            R_THROW(0xFFFF);
        }

        u64 bytes_read;
        R_TRY(fsFileRead(&src_file, offset, buf.data(), buf.size(), 0, &bytes_read));
        Yield();

        R_TRY(fsFileWrite(&dst_file, offset, buf.data(), bytes_read, FsWriteOption_None));
        Yield();

        offset += bytes_read;
    }

    R_SUCCEED();
}

void ProgressBox::Yield() {
    svcSleepThread(YieldType_WithoutCoreMigration);
}

} // namespace sphaira::ui
