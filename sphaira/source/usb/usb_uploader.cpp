#include "usb/usb_uploader.hpp"
#include "usb/usb_api.hpp"
#include "log.hpp"
#include "defines.hpp"

namespace sphaira::usb::upload {
namespace {

const UsbHsInterfaceFilter FILTER{
    .Flags = UsbHsInterfaceFilterFlags_idVendor |
             UsbHsInterfaceFilterFlags_idProduct |
             UsbHsInterfaceFilterFlags_bcdDevice_Min |
             UsbHsInterfaceFilterFlags_bcdDevice_Max |
             UsbHsInterfaceFilterFlags_bDeviceClass |
             UsbHsInterfaceFilterFlags_bDeviceSubClass |
             UsbHsInterfaceFilterFlags_bDeviceProtocol |
             UsbHsInterfaceFilterFlags_bInterfaceClass |
             UsbHsInterfaceFilterFlags_bInterfaceSubClass |
             UsbHsInterfaceFilterFlags_bInterfaceProtocol,
    .idVendor = 0x057e,
    .idProduct = 0x3000,
    .bcdDevice_Min = 0x0100,
    .bcdDevice_Max = 0x0100,
    .bDeviceClass = 0x00,
    .bDeviceSubClass = 0x00,
    .bDeviceProtocol = 0x00,
    .bInterfaceClass = USB_CLASS_VENDOR_SPEC,
    .bInterfaceSubClass = USB_CLASS_VENDOR_SPEC,
    .bInterfaceProtocol = USB_CLASS_VENDOR_SPEC,
};

constexpr u8 INDEX = 0;

using namespace usb::api;

} // namespace

Usb::Usb(u64 transfer_timeout) {
    m_usb = std::make_unique<usb::UsbHs>(INDEX, FILTER, transfer_timeout);
    m_usb->Init();
}

Usb::~Usb() {
}

Result Usb::WaitForConnection(u64 timeout, std::span<const std::string> names) {
    R_TRY(m_usb->IsUsbConnected(timeout));

    // build name table.
    std::string names_list;
    for (auto& name : names) {
        names_list += name + '\n';
    }

    // send.
    SendPacket send_header;
    R_TRY(m_usb->TransferAll(true, &send_header, sizeof(send_header), timeout));
    R_TRY(send_header.Verify());

    // send table info.
    R_TRY(SendResult(RESULT_OK, names_list.length()));

    // send name table.
    R_TRY(m_usb->TransferAll(false, names_list.data(), names_list.length(), timeout));

    R_SUCCEED();
}

Result Usb::PollCommands() {
    SendPacket send_header;
    R_TRY(m_usb->TransferAll(true, &send_header, sizeof(send_header)));
    R_TRY(send_header.Verify());

    if (send_header.GetCmd() == CMD_QUIT) {
        R_TRY(SendResult(RESULT_OK));
        R_THROW(Result_UsbUploadExit);
    } else if (send_header.GetCmd() == CMD_OPEN) {
        s64 file_size;
        u16 flags;
        R_TRY(Open(send_header.arg3, file_size, flags));

        const auto size_lsb = file_size & 0xFFFFFFFF;
        const auto size_msb = ((file_size >> 32) & 0xFFFF) | (flags << 16);
        return SendResult(RESULT_OK, size_msb, size_lsb);
    } else {
        R_TRY(SendResult(RESULT_ERROR));
        R_THROW(Result_UsbUploadBadCommand);
    }
}

Result Usb::file_transfer_loop() {
    log_write("doing file transfer\n");

    // get offset + size.
    SendDataPacket send_header;
    R_TRY(m_usb->TransferAll(true, &send_header, sizeof(send_header)));

    // check if we should finish now.
    if (send_header.GetOffset() == 0 && send_header.GetSize() == 0) {
        log_write("finished\n");
        R_TRY(SendResult(RESULT_OK));
        return Result_UsbUploadExit;
    }

    // read file and calculate the hash.
    u64 bytes_read;
    m_buf.resize(send_header.GetSize());
    log_write("reading buffer: %zu\n", m_buf.size());

    R_TRY(Read(m_buf.data(), send_header.GetOffset(), m_buf.size(), &bytes_read));
    const auto crc32 = crc32Calculate(m_buf.data(), m_buf.size());

    log_write("read the buffer: %zu\n", bytes_read);
    // respond back with the length of the data and the crc32.
    R_TRY(SendResult(RESULT_OK, m_buf.size(), crc32));

    log_write("sent result with crc\n");

    // send the data.
    R_TRY(m_usb->TransferAll(false, m_buf.data(), m_buf.size()));

    log_write("sent the data\n");

    R_SUCCEED();
}

Result Usb::SendResult(u32 result, u32 arg3, u32 arg4) {
    auto recv_header = api::ResultPacket::Build(result, arg3, arg4);
    return m_usb->TransferAll(false, &recv_header, sizeof(recv_header));
}

} // namespace sphaira::usb::upload
