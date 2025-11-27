#pragma once

#include "usb/usbds.hpp"
#include "usb/usb_api.hpp"

#include <string>
#include <vector>
#include <memory>
#include <switch.h>

namespace sphaira::usb::install {

struct Usb {
    Usb(u64 transfer_timeout);
    ~Usb();

    Result Read(void* buf, u64 off, u32 size, u64* bytes_read);
    u32 GetFlags() const;
    void SignalCancel();

    // waits for connection and then sends file list.
    Result IsUsbConnected(u64 timeout);
    Result WaitForConnection(u64 timeout, std::vector<std::string>& names);

    Result OpenFile(u32 index, s64& file_size);
    Result CloseFile();

    auto GetOpenResult() const {
        return m_open_result;
    }

    auto GetCancelEvent() {
        return m_usb->GetCancelEvent();
    }

private:
    Result SendAndVerify(const void* data, u32 size, u64 timeout, api::ResultPacket* out = nullptr);
    Result SendAndVerify(const void* data, u32 size, api::ResultPacket* out = nullptr);

private:
    std::unique_ptr<usb::UsbDs> m_usb{};
    Result m_open_result{};
    bool m_was_connected{};
    u32 m_flags{};
};

} // namespace sphaira::usb::install
