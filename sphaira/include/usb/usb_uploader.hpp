#pragma once

#include "usb/usbhs.hpp"

#include <string>
#include <memory>
#include <span>
#include <switch.h>

namespace sphaira::usb::upload {

struct Usb {
    Usb(u64 transfer_timeout);
    virtual ~Usb();

    virtual Result Read(void* buf, u64 off, u32 size, u64* bytes_read) = 0;
    virtual Result Open(u32 index, s64& out_size, u16& out_flags) = 0;

    Result IsUsbConnected(u64 timeout) {
        return m_usb->IsUsbConnected(timeout);
    }

    // waits for connection and then sends file list.
    Result WaitForConnection(u64 timeout, std::span<const std::string> names);

    // polls for command, executes transfer if possible.
    // will return Result_Exit if exit command is recieved.
    Result PollCommands();

    Result file_transfer_loop();

    auto GetOpenResult() const {
        return m_open_result;
    }

    auto GetCancelEvent() {
        return m_usb->GetCancelEvent();
    }

private:
    Result SendResult(u32 result, u32 arg3 = 0, u32 arg4 = 0);

private:
    std::unique_ptr<usb::UsbHs> m_usb{};
    std::vector<u8> m_buf{};
    Result m_open_result{};
    bool m_was_connected{};
};

} // namespace sphaira::usb::upload
