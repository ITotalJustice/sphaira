#include "usb/usb_dumper.hpp"
#include "usb/usb_api.hpp"
#include "defines.hpp"
#include "log.hpp"

#include <ranges>

namespace sphaira::usb::dump {
namespace {

using namespace usb::api;

} // namespace

Usb::Usb(u64 transfer_timeout) {
    m_usb = std::make_unique<usb::UsbDs>(transfer_timeout);
    m_open_result = m_usb->Init();
}

Usb::~Usb() {
    if (m_was_connected && R_SUCCEEDED(m_usb->IsUsbConnected(0))) {
        const auto send_header = SendPacket::Build(CMD_QUIT);
        SendAndVerify(&send_header, sizeof(send_header), 1e+9);
    }
}

Result Usb::IsUsbConnected(u64 timeout) {
    return m_usb->IsUsbConnected(timeout);
}

Result Usb::WaitForConnection(std::string_view path, u64 timeout) {
    m_was_connected = false;

    // ensure that we are connected.
    R_TRY(m_open_result);
    R_TRY(m_usb->IsUsbConnected(timeout));

    const auto send_header = SendPacket::Build(CMD_EXPORT, path.length());
    R_TRY(SendAndVerify(&send_header, sizeof(send_header), timeout));
    R_TRY(SendAndVerify(path.data(), path.length(), timeout));

    m_was_connected = true;
    R_SUCCEED();
}

Result Usb::CloseFile() {
    const auto send_header = SendDataPacket::Build(0, 0, 0);

    return SendAndVerify(&send_header, sizeof(send_header));
}

void Usb::SignalCancel() {
    m_usb->Cancel();
}

Result Usb::Write(const void* buf, u64 off, u32 size) {
    const auto send_header = SendDataPacket::Build(off, size, crc32cCalculate(buf, size));

    R_TRY(SendAndVerify(&send_header, sizeof(send_header)));
    return SendAndVerify(buf, size);
}

// casts away const, but it does not modify the buffer!
Result Usb::SendAndVerify(const void* data, u32 size, u64 timeout, ResultPacket* out) {
    R_TRY(m_usb->TransferAll(false, const_cast<void*>(data), size, timeout));


    ResultPacket recv_header;
    R_TRY(m_usb->TransferAll(true, &recv_header, sizeof(recv_header), timeout));
    R_TRY(recv_header.Verify());

    if (out) *out = recv_header;
    R_SUCCEED();
}

Result Usb::SendAndVerify(const void* data, u32 size, ResultPacket* out) {
    return SendAndVerify(data, size, m_usb->GetTransferTimeout(), out);
}

} // namespace sphaira::usb::dump
