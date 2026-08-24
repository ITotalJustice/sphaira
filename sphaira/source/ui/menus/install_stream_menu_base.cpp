#include "ui/menus/install_stream_menu_base.hpp"
#include "yati/yati.hpp"
#include "app.hpp"
#include "defines.hpp"
#include "log.hpp"
#include "ui/nvg_util.hpp"
#include "i18n.hpp"
#include <cstring>

namespace sphaira::ui::menu::stream {
namespace {

constexpr u64 MAX_BUFFER_SIZE = 1024ULL*1024ULL*1ULL;

// every wait in here is bounded, a missed wakeup then costs a few ms rather
// than hanging the transport (and with it the whole app) forever.
constexpr u64 WAIT_TIMEOUT = 1e+8; // 100ms.
constexpr u64 POLL_INTERVAL_FAST = 1e+6; // 1ms.
constexpr u64 POLL_INTERVAL_SLOW = 1e+7; // 10ms.

} // namespace

Stream::Stream(const fs::FsPath& path, std::stop_token token) {
    m_path = path;
    m_token = token;
    m_active = true;
    m_buffer.reserve(MAX_BUFFER_SIZE);

    mutexInit(&m_mutex);
    condvarInit(&m_can_read);
    condvarInit(&m_can_write);
}

void Stream::Wait(CondVar* cv) {
    // a timeout is expected and not an error, the caller loops and re-checks.
    condvarWaitTimeout(cv, std::addressof(m_mutex), WAIT_TIMEOUT);
}

Result Stream::ReadChunk(void* _buf, s64 size, u64* bytes_read) {
    auto buf = static_cast<u8*>(_buf);
    *bytes_read = 0;

    while (!m_token.stop_requested()) {
        SCOPED_MUTEX(&m_mutex);

        if (m_buffer.empty()) {
            if (!m_active) {
                break;
            }

            Wait(std::addressof(m_can_read));
            continue;
        }

        const auto rsize = std::min<s64>(size, m_buffer.size());
        std::memcpy(buf, m_buffer.data(), rsize);
        m_buffer.erase(m_buffer.begin(), m_buffer.begin() + rsize);
        condvarWakeAll(std::addressof(m_can_write));

        size -= rsize;
        buf += rsize;
        *bytes_read += rsize;

        if (!size) {
            R_SUCCEED();
        }
    }

    // a short read is still a success, yati treats zero bytes read as a
    // cancelled transfer.
    if (*bytes_read) {
        R_SUCCEED();
    }

    log_write("[Stream::ReadChunk] failed to read\n");
    R_THROW(Result_TransferCancelled);
}

bool Stream::Push(const void* _buf, s64 size) {
    auto buf = static_cast<const u8*>(_buf);
    if (!size) {
        return true;
    }

    while (!m_token.stop_requested()) {
        SCOPED_MUTEX(&m_mutex);

        // the installer already finished (or gave up) reading this file, the
        // remaining data is not needed, pretend it was consumed so that the
        // transport can move on to the next file.
        if (m_install_done) {
            return true;
        }

        if (!m_active) {
            log_write("[Stream::Push] file not active\n");
            break;
        }

        if (m_buffer.size() >= MAX_BUFFER_SIZE) {
            Wait(std::addressof(m_can_write));
            continue;
        }

        const auto wsize = std::min<s64>(size, MAX_BUFFER_SIZE - m_buffer.size());
        const auto offset = m_buffer.size();
        m_buffer.resize(offset + wsize);

        std::memcpy(m_buffer.data() + offset, buf, wsize);
        condvarWakeAll(std::addressof(m_can_read));

        size -= wsize;
        buf += wsize;
        if (!size) {
            return true;
        }
    }

    log_write("[Stream::Push] failed to push\n");
    return false;
}

void Stream::Disable() {
    log_write("[Stream::Disable] disabling file\n");

    SCOPED_MUTEX(&m_mutex);
    m_active = false;
    condvarWakeAll(std::addressof(m_can_read));
    condvarWakeAll(std::addressof(m_can_write));
}

void Stream::SetInstallFinished() {
    SCOPED_MUTEX(&m_mutex);
    m_install_done = true;
    condvarWakeAll(std::addressof(m_can_read));
    condvarWakeAll(std::addressof(m_can_write));
}

Menu::Menu(const std::string& title, u32 flags) : MenuBase{title, flags} {
    SetAction(Button::B, Action{"Back"_i18n, [this](){
        SetPop();
    }});

    SetAction(Button::X, Action{"Options"_i18n, [this](){
        App::DisplayInstallOptions(false);
    }});

    App::SetAutoSleepDisabled(true);
    mutexInit(&m_mutex);
}

Menu::~Menu() {
    // derived destructors already called CancelInstallMode(), this is only a
    // safety net in case one of them forgot to.
    m_stop_source.request_stop();

    std::shared_ptr<Stream> source;
    {
        SCOPED_MUTEX(&m_mutex);
        source = m_source;
    }

    if (source) {
        source->Disable();
        source->SetInstallFinished();
    }

    App::SetAutoSleepDisabled(false);
}

bool Menu::IsCancelled() const {
    // check the cheap, non-virtual condition first, the transport may already
    // be halfway through tearing us down.
    if (GetToken().stop_requested()) {
        return true;
    }

    return !IsInstallModeActive();
}

void Menu::CancelInstallMode() {
    // 1. unblock the transport callbacks that poll this.
    m_stop_source.request_stop();

    // 2. unblock a transfer that is sat waiting on the installer.
    //    the stream is taken out of the lock before use, the transport thread
    //    may be inside Push() with the stream mutex held.
    std::shared_ptr<Stream> source;
    {
        SCOPED_MUTEX(&m_mutex);
        source = m_source;
    }

    if (source) {
        source->Disable();
        source->SetInstallFinished();
    }

    // 3. tell the transport to stop accepting installs, this waits for any
    //    in-flight callback to return, which steps 1 and 2 just guaranteed.
    OnDisableInstallMode();
}

bool Menu::WaitForIdle(u64 poll_ns) {
    for (;;) {
        {
            SCOPED_MUTEX(&m_mutex);
            // Connected means a stream is waiting to be picked up by the ui
            // thread, Progress means the installer still owns it. In both
            // cases the previous transfer is not done with yet.
            if (m_state != State::Connected && m_state != State::Progress) {
                return true;
            }
        }

        if (IsCancelled()) {
            return false;
        }

        svcSleepThread(poll_ns);
    }
}

void Menu::Update(Controller* controller, TouchInfo* touch) {
    MenuBase::Update(controller, touch);

    std::shared_ptr<Stream> source;
    {
        SCOPED_MUTEX(&m_mutex);
        if (m_state != State::Connected) {
            return;
        }

        m_state = State::Progress;
        source = m_source;
    }

    // pushed outside of the lock, creating the progress box spawns a thread
    // whilst the transport thread is polling m_state.
    App::Push<ui::ProgressBox>(0, "Installing "_i18n, source->GetPath(), [source](auto pbox) -> Result {
        // whatever happens, stop the transport from blocking on this stream.
        ON_SCOPE_EXIT(source->SetInstallFinished());

        const auto rc = yati::InstallFromSource(pbox, source.get(), source->GetPath());
        if (R_FAILED(rc)) {
            source->Disable();
            R_THROW(rc);
        }

        R_SUCCEED();
    }, [this](Result rc){
        App::PushErrorBox(rc, "Install failed!"_i18n);

        bool failed;
        {
            SCOPED_MUTEX(&m_mutex);

            if (R_SUCCEEDED(rc)) {
                App::Notify("Install success!"_i18n);
                m_state = State::Done;
                failed = false;
            } else {
                m_state = State::Failed;
                failed = true;
            }
        }

        // must be done *without* m_mutex held. the transport thread can be
        // blocked inside one of our callbacks whilst holding its own lock, and
        // disabling install mode takes that same lock, which would deadlock.
        if (failed) {
            CancelInstallMode();
        }
    }, ui::ProgressBoxOption::ScreenToggle);
}

void Menu::Draw(NVGcontext* vg, Theme* theme) {
    MenuBase::Draw(vg, theme);

    State state;
    {
        SCOPED_MUTEX(&m_mutex);
        state = m_state;
    }

    switch (state) {
        case State::None:
        case State::Done:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Drag'n'Drop (NSP, XCI, NSZ, XCZ, MSP) to the install folder"_i18n.c_str());
            break;

        case State::Connected:
        case State::Progress:
            break;

        case State::Failed:
            gfx::drawTextArgs(vg, SCREEN_WIDTH / 2.f, SCREEN_HEIGHT / 2.f, 36.f, NVG_ALIGN_CENTER | NVG_ALIGN_MIDDLE, theme->GetColour(ThemeEntryID_TEXT_INFO), "Failed to install, press B to exit..."_i18n.c_str());
            break;
    }
}

bool Menu::OnInstallStart(const char* path) {
    log_write("[Menu::OnInstallStart] inside: %s\n", path);

    // wait for the previous transfer to fully complete. the state only leaves
    // Progress once the progress box has been destroyed, and that joins the
    // installer thread first, so by this point nothing can still be using the
    // old stream.
    if (!WaitForIdle(POLL_INTERVAL_FAST)) {
        log_write("[Menu::OnInstallStart] cancelled whilst waiting\n");
        return false;
    }

    SCOPED_MUTEX(&m_mutex);

    // a failed install ends the session, don't accept anything else.
    if (m_state == State::Failed || GetToken().stop_requested()) {
        log_write("[Menu::OnInstallStart] rejecting, state: %u\n", (u8)m_state);
        return false;
    }

    m_source = std::make_shared<Stream>(path, GetToken());
    m_state = State::Connected;
    log_write("[Menu::OnInstallStart] exiting\n");

    return true;
}

bool Menu::OnInstallWrite(const void* buf, size_t size) {
    std::shared_ptr<Stream> source;
    {
        SCOPED_MUTEX(&m_mutex);
        source = m_source;
    }

    if (!source) {
        log_write("[Menu::OnInstallWrite] no source\n");
        return false;
    }

    return source->Push(buf, size);
}

void Menu::OnInstallClose() {
    log_write("[Menu::OnInstallClose] inside\n");

    std::shared_ptr<Stream> source;
    {
        SCOPED_MUTEX(&m_mutex);
        source = m_source;
    }

    if (source) {
        // the file has been fully received, let the installer drain whatever
        // is still buffered.
        source->Disable();
    }

    // wait until the install has finished before returning. without this the
    // transport opens the next file whilst this one is still installing, which
    // with a queue of many (small) files ends up replacing the stream from
    // under the installer.
    WaitForIdle(POLL_INTERVAL_SLOW);
}

} // namespace sphaira::ui::menu::stream
