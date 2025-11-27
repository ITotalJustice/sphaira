#include "usb/usbhs.hpp"
#include "log.hpp"
#include "defines.hpp"
#include <ranges>
#include <cstring>

namespace sphaira::usb {
namespace {

struct Bcd {
    constexpr Bcd(u16 v) : value{v} {}

    u8 major() const { return (value >> 8) & 0xFF; }
    u8 minor() const { return (value >> 4) & 0xF; }
    u8 macro() const { return (value >> 0) & 0xF; }

    const u16 value;
};

Result usbHsParseReportData(UsbHsXferReport* reports, u32 count, u32 xferId, u32 *requestedSize, u32 *transferredSize) {
    Result rc = 0;
    u32 pos;
    UsbHsXferReport *entry = NULL;
    if (count>8) count = 8;

    for(pos=0; pos<count; pos++) {
        entry = &reports[pos];
        if (entry->xferId == xferId) break;
    }

    if (pos == count) return MAKERESULT(Module_Libnx, LibnxError_NotFound);
    rc = entry->res;

    if (R_SUCCEEDED(rc)) {
        if (requestedSize) *requestedSize = entry->requestedSize;
        if (transferredSize) *transferredSize = entry->transferredSize;
    }

    return rc;
}

} // namespace

UsbHs::UsbHs(u8 index, const UsbHsInterfaceFilter& filter, u64 transfer_timeout)
: Base{transfer_timeout}
, m_index{index}
, m_filter{filter} {

}

UsbHs::~UsbHs() {
    Close();
    usbHsDestroyInterfaceAvailableEvent(std::addressof(m_event), m_index);
    usbHsExit();
}

Result UsbHs::Init() {
    log_write("doing USB init\n");
    R_TRY(usbHsInitialize());
    R_TRY(usbHsCreateInterfaceAvailableEvent(&m_event, true, m_index, &m_filter));
    log_write("success USB init\n");
    R_SUCCEED();
}

Result UsbHs::IsUsbConnected(u64 timeout) {
    if (m_connected) {
        R_SUCCEED();
    }

    const std::array waiters{
        waiterForEvent(&m_event),
        waiterForUEvent(GetCancelEvent()),
    };

    s32 idx;
    R_TRY(waitObjects(&idx, waiters.data(), waiters.size(), timeout));

    if (idx == waiters.size() - 1) {
        return Result_UsbCancelled;
    }

    return Connect();
}

Result UsbHs::Connect() {
    Close();

    s32 total;
    R_TRY(usbHsQueryAvailableInterfaces(&m_filter, &m_interface, sizeof(m_interface), &total));
    R_TRY(usbHsAcquireUsbIf(&m_s, &m_interface));

    const auto bcdUSB = Bcd{m_interface.device_desc.bcdUSB};
    const auto bcdDevice = Bcd{m_interface.device_desc.bcdDevice};

    // log lsusb style.
    log_write("[USBHS] pathstr: %s\n", m_interface.pathstr);
    log_write("Bus: %03u Device: %03u ID: %04x:%04x\n\n", m_interface.busID, m_interface.deviceID, m_interface.device_desc.idVendor, m_interface.device_desc.idProduct);

    log_write("Device Descriptor:\n");
    log_write("\tbLength:            %u\n", m_interface.device_desc.bLength);
    log_write("\tbDescriptorType:    %u\n", m_interface.device_desc.bDescriptorType);
    log_write("\tbcdUSB:             %u:%u%u\n", bcdUSB.major(), bcdUSB.minor(), bcdUSB.macro());
    log_write("\tbDeviceClass:       %u\n", m_interface.device_desc.bDeviceClass);
    log_write("\tbDeviceSubClass:    %u\n", m_interface.device_desc.bDeviceSubClass);
    log_write("\tbDeviceProtocol:    %u\n", m_interface.device_desc.bDeviceProtocol);
    log_write("\tbMaxPacketSize0:    %u\n", m_interface.device_desc.bMaxPacketSize0);
    log_write("\tidVendor:           0x%x\n", m_interface.device_desc.idVendor);
    log_write("\tidProduct:          0x%x\n", m_interface.device_desc.idProduct);
    log_write("\tbcdDevice:          %u:%u%u\n", bcdDevice.major(), bcdDevice.minor(), bcdDevice.macro());
    log_write("\tiManufacturer:      %u\n", m_interface.device_desc.iManufacturer);
    log_write("\tiProduct:           %u\n", m_interface.device_desc.iProduct);
    log_write("\tiSerialNumber:      %u\n", m_interface.device_desc.iSerialNumber);
    log_write("\tbNumConfigurations: %u\n", m_interface.device_desc.bNumConfigurations);

    log_write("\tConfiguration Descriptor:\n");
    log_write("\t\tbLength:             %u\n", m_interface.config_desc.bLength);
    log_write("\t\tbDescriptorType:     %u\n", m_interface.config_desc.bDescriptorType);
    log_write("\t\twTotalLength:        %u\n", m_interface.config_desc.wTotalLength);
    log_write("\t\tbNumInterfaces:      %u\n", m_interface.config_desc.bNumInterfaces);
    log_write("\t\tbConfigurationValue: %u\n", m_interface.config_desc.bConfigurationValue);
    log_write("\t\tiConfiguration:      %u\n", m_interface.config_desc.iConfiguration);
    log_write("\t\tbmAttributes:        0x%x\n", m_interface.config_desc.bmAttributes);
    log_write("\t\tMaxPower:            %u (%u mA)\n", m_interface.config_desc.MaxPower, m_interface.config_desc.MaxPower * 2);

    struct usb_endpoint_descriptor invalid_desc{};
    for (u8 i = 0; i < std::size(m_s.inf.inf.input_endpoint_descs); i++) {
        const auto& desc = m_s.inf.inf.input_endpoint_descs[i];
        if (std::memcmp(&desc, &invalid_desc, sizeof(desc))) {
            log_write("\t[USBHS] desc[%u] wMaxPacketSize: 0x%X\n", i, desc.wMaxPacketSize);
        }
    }

    auto& input_descs = m_s.inf.inf.input_endpoint_descs[0];
    R_TRY(usbHsIfOpenUsbEp(&m_s, &m_endpoints[UsbSessionEndpoint_Out], 1, input_descs.wMaxPacketSize, &input_descs));

    auto& output_descs = m_s.inf.inf.output_endpoint_descs[0];
    R_TRY(usbHsIfOpenUsbEp(&m_s, &m_endpoints[UsbSessionEndpoint_In], 1, output_descs.wMaxPacketSize, &output_descs));

    m_connected = true;
    R_SUCCEED();
}

void UsbHs::Close() {
    usbHsEpClose(std::addressof(m_endpoints[UsbSessionEndpoint_In]));
    usbHsEpClose(std::addressof(m_endpoints[UsbSessionEndpoint_Out]));
    usbHsIfClose(std::addressof(m_s));

    m_endpoints[UsbSessionEndpoint_In] = {};
    m_endpoints[UsbSessionEndpoint_Out] = {};
    m_s = {};
    m_connected = false;
}

Event *UsbHs::GetCompletionEvent(UsbSessionEndpoint ep) {
    return usbHsEpGetXferEvent(&m_endpoints[ep]);
}

Result UsbHs::WaitTransferCompletion(UsbSessionEndpoint ep, u64 timeout) {
    const std::array waiters{
        waiterForEvent(GetCompletionEvent(ep)),
        waiterForEvent(usbHsGetInterfaceStateChangeEvent()),
        waiterForUEvent(GetCancelEvent()),
    };

    s32 idx;
    auto rc = waitObjects(&idx, waiters.data(), waiters.size(), timeout);

    // check if we got one of the cancel events.
    if (R_SUCCEEDED(rc) && idx == waiters.size() - 1) {
        log_write("got usb cancel event\n");
        rc = Result_UsbCancelled;
    } else if (R_SUCCEEDED(rc) && idx == waiters.size() - 2) {
        log_write("got usb timeout event\n");
        rc = KERNELRESULT(TimedOut);
        Close();
    }

    if (R_FAILED(rc)) {
        log_write("failed to wait for event\n");
        eventClear(GetCompletionEvent(ep));
        eventClear(usbHsGetInterfaceStateChangeEvent());
    }

    return rc;
}

Result UsbHs::TransferAsync(UsbSessionEndpoint ep, void *buffer, u32 remaining, u32 size, u32 *out_xfer_id) {
    return usbHsEpPostBufferAsync(&m_endpoints[ep], buffer, size, 0, out_xfer_id);
}

Result UsbHs::GetTransferResult(UsbSessionEndpoint ep, u32 xfer_id, u32 *out_requested_size, u32 *out_transferred_size) {
    u32 count;
    UsbHsXferReport report_data[8];

    R_TRY(eventClear(GetCompletionEvent(ep)));
    R_TRY(usbHsEpGetXferReport(&m_endpoints[ep], report_data, std::size(report_data), std::addressof(count)));
    R_TRY(usbHsParseReportData(report_data, count, xfer_id, out_requested_size, out_transferred_size));

    R_SUCCEED();
}

} // namespace sphaira::usb
