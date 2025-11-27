#pragma once

#include "base.hpp"
#include "usb/usb_installer.hpp"

#include <string>
#include <vector>
#include <memory>
#include <switch.h>

namespace sphaira::yati::source {

struct Usb final : Base {
    Usb(u64 transfer_timeout) {
        m_usb = std::make_unique<usb::install::Usb>(transfer_timeout);
    }

    void SignalCancel() override {
        m_usb->SignalCancel();
    }

    bool IsStream() const override {
        return m_usb->GetFlags() & usb::api::FLAG_STREAM;
    }

    Result Read(void* buf, s64 off, s64 size, u64* bytes_read) override {
        return m_usb->Read(buf, off, size, bytes_read);
    }

    Result IsUsbConnected(u64 timeout) {
        return m_usb->IsUsbConnected(timeout);
    }

    Result WaitForConnection(u64 timeout, std::vector<std::string>& names) {
        return m_usb->WaitForConnection(timeout, names);
    }

    Result OpenFile(u32 index, s64& file_size) {
        return m_usb->OpenFile(index, file_size);
    }

    Result CloseFile() {
        return m_usb->CloseFile();
    }

    auto GetOpenResult() const {
        return m_usb->GetOpenResult();
    }

    auto GetCancelEvent() {
        return m_usb->GetCancelEvent();
    }

private:
    std::unique_ptr<usb::install::Usb> m_usb{};
};

} // namespace sphaira::yati::source
