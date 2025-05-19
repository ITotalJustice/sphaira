#include "usb/usbds.hpp"
#include "log.hpp"
#include "defines.hpp"
#include <ranges>
#include <cstring>

namespace sphaira::usb {
namespace {

// untested, should work tho.
// TODO: pr missing speed fields to libnx.
Result usbDsGetSpeed(u32 *out) {
    if (hosversionBefore(8,0,0)) {
        return MAKERESULT(Module_Libnx, LibnxError_IncompatSysVer);
    }

    serviceAssumeDomain(usbDsGetServiceSession());
    return serviceDispatchOut(usbDsGetServiceSession(), hosversionAtLeast(11,0,0) ? 11 : 12, *out);
}

} // namespace

UsbDs::~UsbDs() {
    usbDsExit();
}

Result UsbDs::Init() {
    log_write("doing USB init\n");
    R_TRY(usbDsInitialize());

    static SetSysSerialNumber serial_number{};
    R_TRY(setsysInitialize());
    ON_SCOPE_EXIT(setsysExit());
    R_TRY(setsysGetSerialNumber(&serial_number));

    u8 iManufacturer, iProduct, iSerialNumber;
    static constexpr u16 supported_langs[1] = {0x0409};
    // Send language descriptor
    R_TRY(usbDsAddUsbLanguageStringDescriptor(nullptr, supported_langs, std::size(supported_langs)));
    // Send manufacturer
    R_TRY(usbDsAddUsbStringDescriptor(&iManufacturer, "Nintendo"));
    // Send product
    R_TRY(usbDsAddUsbStringDescriptor(&iProduct, "Nintendo Switch"));
    // Send serial number
    R_TRY(usbDsAddUsbStringDescriptor(&iSerialNumber, serial_number.number));

    // Send device descriptors
    struct usb_device_descriptor device_descriptor = {
        .bLength = USB_DT_DEVICE_SIZE,
        .bDescriptorType = USB_DT_DEVICE,
        .bcdUSB = 0x0110,
        .bDeviceClass = 0x00,
        .bDeviceSubClass = 0x00,
        .bDeviceProtocol = 0x00,
        .bMaxPacketSize0 = 0x40,
        .idVendor = 0x057e,
        .idProduct = 0x3000,
        .bcdDevice = 0x0100,
        .iManufacturer = iManufacturer,
        .iProduct = iProduct,
        .iSerialNumber = iSerialNumber,
        .bNumConfigurations = 0x01
    };

    // Full Speed is USB 1.1
    R_TRY(usbDsSetUsbDeviceDescriptor(UsbDeviceSpeed_Full, &device_descriptor));

    // High Speed is USB 2.0
    device_descriptor.bcdUSB = 0x0200;
    R_TRY(usbDsSetUsbDeviceDescriptor(UsbDeviceSpeed_High, &device_descriptor));

    // Super Speed is USB 3.0
    device_descriptor.bcdUSB = 0x0300;
    // Upgrade packet size to 512
    device_descriptor.bMaxPacketSize0 = 0x09;
    R_TRY(usbDsSetUsbDeviceDescriptor(UsbDeviceSpeed_Super, &device_descriptor));

    // Define Binary Object Store
    const u8 bos[0x16] = {
        0x05,                     // .bLength
        USB_DT_BOS,               // .bDescriptorType
        0x16, 0x00,               // .wTotalLength
        0x02,                     // .bNumDeviceCaps

        // USB 2.0
        0x07,                     // .bLength
        USB_DT_DEVICE_CAPABILITY, // .bDescriptorType
        0x02,                     // .bDevCapabilityType
        0x02, 0x00, 0x00, 0x00,   // dev_capability_data

        // USB 3.0
        0x0A,                     // .bLength
        USB_DT_DEVICE_CAPABILITY, // .bDescriptorType
        0x03,                     /* .bDevCapabilityType */
        0x00,                     /* .bmAttributes */
        0x0E, 0x00,               /* .wSpeedSupported */
        0x03,                     /* .bFunctionalitySupport */
        0x00,                     /* .bU1DevExitLat */
        0x00, 0x00                /* .bU2DevExitLat */
    };

    R_TRY(usbDsSetBinaryObjectStore(bos, sizeof(bos)));

    struct usb_interface_descriptor interface_descriptor = {
        .bLength = USB_DT_INTERFACE_SIZE,
        .bDescriptorType = USB_DT_INTERFACE,
        .bInterfaceNumber = USBDS_DEFAULT_InterfaceNumber, // set below
        .bNumEndpoints = static_cast<u8>(std::size(m_endpoints)),
        .bInterfaceClass = USB_CLASS_VENDOR_SPEC,
        .bInterfaceSubClass = USB_CLASS_VENDOR_SPEC,
        .bInterfaceProtocol = USB_CLASS_VENDOR_SPEC,
    };

    struct usb_endpoint_descriptor endpoint_descriptor_in = {
        .bLength = USB_DT_ENDPOINT_SIZE,
        .bDescriptorType = USB_DT_ENDPOINT,
        .bEndpointAddress = USB_ENDPOINT_IN,
        .bmAttributes = USB_TRANSFER_TYPE_BULK,
    };

    struct usb_endpoint_descriptor endpoint_descriptor_out = {
        .bLength = USB_DT_ENDPOINT_SIZE,
        .bDescriptorType = USB_DT_ENDPOINT,
        .bEndpointAddress = USB_ENDPOINT_OUT,
        .bmAttributes = USB_TRANSFER_TYPE_BULK,
    };

    const struct usb_ss_endpoint_companion_descriptor endpoint_companion = {
        .bLength = sizeof(struct usb_ss_endpoint_companion_descriptor),
        .bDescriptorType = USB_DT_SS_ENDPOINT_COMPANION,
        .bMaxBurst = 0x0F,
        .bmAttributes = 0x00,
        .wBytesPerInterval = 0x00,
    };

    R_TRY(usbDsRegisterInterface(&m_interface));

    interface_descriptor.bInterfaceNumber = m_interface->interface_index;
    endpoint_descriptor_in.bEndpointAddress += interface_descriptor.bInterfaceNumber + 1;
    endpoint_descriptor_out.bEndpointAddress += interface_descriptor.bInterfaceNumber + 1;

    // Full Speed Config
    endpoint_descriptor_in.wMaxPacketSize = 0x40;
    endpoint_descriptor_out.wMaxPacketSize = 0x40;
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Full, &interface_descriptor, USB_DT_INTERFACE_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Full, &endpoint_descriptor_in, USB_DT_ENDPOINT_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Full, &endpoint_descriptor_out, USB_DT_ENDPOINT_SIZE));

    // High Speed Config
    endpoint_descriptor_in.wMaxPacketSize = 0x200;
    endpoint_descriptor_out.wMaxPacketSize = 0x200;
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_High, &interface_descriptor, USB_DT_INTERFACE_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_High, &endpoint_descriptor_in, USB_DT_ENDPOINT_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_High, &endpoint_descriptor_out, USB_DT_ENDPOINT_SIZE));

    // Super Speed Config
    endpoint_descriptor_in.wMaxPacketSize = 0x400;
    endpoint_descriptor_out.wMaxPacketSize = 0x400;
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Super, &interface_descriptor, USB_DT_INTERFACE_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Super, &endpoint_descriptor_in, USB_DT_ENDPOINT_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Super, &endpoint_companion, USB_DT_SS_ENDPOINT_COMPANION_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Super, &endpoint_descriptor_out, USB_DT_ENDPOINT_SIZE));
    R_TRY(usbDsInterface_AppendConfigurationData(m_interface, UsbDeviceSpeed_Super, &endpoint_companion, USB_DT_SS_ENDPOINT_COMPANION_SIZE));

    //Setup endpoints.
    R_TRY(usbDsInterface_RegisterEndpoint(m_interface, &m_endpoints[UsbSessionEndpoint_In], endpoint_descriptor_in.bEndpointAddress));
    R_TRY(usbDsInterface_RegisterEndpoint(m_interface, &m_endpoints[UsbSessionEndpoint_Out], endpoint_descriptor_out.bEndpointAddress));

    R_TRY(usbDsInterface_EnableInterface(m_interface));
    R_TRY(usbDsEnable());

    log_write("success USB init\n");
    R_SUCCEED();
}

// the below code is taken from libnx, with the addition of a uevent to cancel.
Result UsbDs::IsUsbConnected(u64 timeout) {
    Result rc;
    UsbState state = UsbState_Detached;

    rc = usbDsGetState(&state);
    if (R_FAILED(rc)) return rc;
    if (state == UsbState_Configured) return 0;

    bool has_timeout = timeout != UINT64_MAX;
    u64 deadline = 0;

    const std::array waiters{
        waiterForEvent(usbDsGetStateChangeEvent()),
        waiterForUEvent(GetCancelEvent()),
    };

    if (has_timeout)
        deadline = armGetSystemTick() + armNsToTicks(timeout);

    do {
        if (has_timeout) {
            s64 remaining = deadline - armGetSystemTick();
            timeout = remaining > 0 ? armTicksToNs(remaining) : 0;
        }

        s32 idx;
        rc = waitObjects(&idx, waiters.data(), waiters.size(), timeout);
        eventClear(usbDsGetStateChangeEvent());

        // check if we got one of the cancel events.
        if (R_SUCCEEDED(rc) && idx == waiters.size() - 1) {
            rc = Result_Cancelled;
            break;
        }

        rc = usbDsGetState(&state);
    } while (R_SUCCEEDED(rc) && state != UsbState_Configured && timeout > 0);

    if (R_SUCCEEDED(rc) && state != UsbState_Configured && timeout == 0)
        return KERNELRESULT(TimedOut);

    return rc;
}

Result UsbDs::GetSpeed(UsbDeviceSpeed* out) {
    return usbDsGetSpeed((u32*)out);
}

Event *UsbDs::GetCompletionEvent(UsbSessionEndpoint ep) {
    return std::addressof(m_endpoints[ep]->CompletionEvent);
}

Result UsbDs::WaitTransferCompletion(UsbSessionEndpoint ep, u64 timeout) {
    const std::array waiters{
        waiterForEvent(GetCompletionEvent(ep)),
        waiterForEvent(usbDsGetStateChangeEvent()),
        waiterForUEvent(GetCancelEvent()),
    };

    s32 idx;
    auto rc = waitObjects(&idx, waiters.data(), waiters.size(), timeout);

    // check if we got one of the cancel events.
    if (R_SUCCEEDED(rc) && idx == waiters.size() - 1) {
        log_write("got usb cancel event\n");
        rc = Result_Cancelled;
    } else if (R_SUCCEEDED(rc) && idx == waiters.size() - 2) {
        log_write("got usbDsGetStateChangeEvent() event\n");
        rc = KERNELRESULT(TimedOut);
    }


    if (R_FAILED(rc)) {
        R_TRY(usbDsEndpoint_Cancel(m_endpoints[ep]));
        eventClear(GetCompletionEvent(ep));
        eventClear(usbDsGetStateChangeEvent());
    }

    return rc;
}

Result UsbDs::TransferAsync(UsbSessionEndpoint ep, void *buffer, u32 size, u32 *out_urb_id) {
    return usbDsEndpoint_PostBufferAsync(m_endpoints[ep], buffer, size, out_urb_id);
}

Result UsbDs::GetTransferResult(UsbSessionEndpoint ep, u32 urb_id, u32 *out_requested_size, u32 *out_transferred_size) {
    UsbDsReportData report_data;

    R_TRY(eventClear(GetCompletionEvent(ep)));
    R_TRY(usbDsEndpoint_GetReportData(m_endpoints[ep], std::addressof(report_data)));
    R_TRY(usbDsParseReportData(std::addressof(report_data), urb_id, out_requested_size, out_transferred_size));

    R_SUCCEED();
}

} // namespace sphaira::usb
