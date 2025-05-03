#pragma once

#include "base.hpp"
#include "fs.hpp"

#include <vector>
#include <string>
#include <new>
#include <switch.h>

namespace sphaira::yati::source {

struct Usb final : Base {
    enum { USBModule = 523 };

    enum : Result {
        Result_BadMagic = MAKERESULT(USBModule, 0),
        Result_BadVersion = MAKERESULT(USBModule, 1),
        Result_BadCount = MAKERESULT(USBModule, 2),
        Result_BadTransferSize = MAKERESULT(USBModule, 3),
        Result_BadTotalSize = MAKERESULT(USBModule, 4),
        Result_Cancelled = MAKERESULT(USBModule, 11),
    };

    Usb(u64 transfer_timeout);
    ~Usb();

    Result Read(void* buf, s64 off, s64 size, u64* bytes_read) override;
    Result Finished();

    Result Init();
    Result IsUsbConnected(u64 timeout);
    Result WaitForConnection(u64 timeout, std::vector<std::string>& out_names);
    void SetFileNameForTranfser(const std::string& name);

    auto GetCancelEvent() {
        return &m_uevent;
    }

    void SignalCancel() override {
        ueventSignal(GetCancelEvent());
    }

public:
    // custom allocator for std::vector that respects alignment.
    // https://en.cppreference.com/w/cpp/named_req/Allocator
    template <typename T, std::size_t Align>
    struct CustomVectorAllocator {
    public:
        // https://en.cppreference.com/w/cpp/memory/new/operator_new
        auto allocate(std::size_t n) -> T* {
            return new(align) T[n];
        }

        // https://en.cppreference.com/w/cpp/memory/new/operator_delete
        auto deallocate(T* p, std::size_t n) noexcept -> void {
            ::operator delete[] (p, n, align);
        }

    private:
        static constexpr inline std::align_val_t align{Align};
    };

    template <typename T>
    struct PageAllocator : CustomVectorAllocator<T, 0x1000> {
        using value_type = T; // used by std::vector
    };

    using PageAlignedVector = std::vector<u8, PageAllocator<u8>>;

private:
    enum UsbSessionEndpoint {
        UsbSessionEndpoint_In = 0,
        UsbSessionEndpoint_Out = 1,
    };

    Result SendCmdHeader(u32 cmdId, size_t dataSize);
    Result SendFileRangeCmd(u64 offset, u64 size);

    Event *GetCompletionEvent(UsbSessionEndpoint ep) const;
    Result WaitTransferCompletion(UsbSessionEndpoint ep, u64 timeout);
    Result TransferAsync(UsbSessionEndpoint ep, void *buffer, u32 size, u32 *out_urb_id) const;
    Result GetTransferResult(UsbSessionEndpoint ep, u32 urb_id, u32 *out_requested_size, u32 *out_transferred_size) const;
    Result TransferPacketImpl(bool read, void *page, u32 size, u32 *out_size_transferred, u64 timeout);
    Result TransferAll(bool read, void *data, u32 size, u64 timeout);

private:
    UsbDsInterface* m_interface{};
    UsbDsEndpoint* m_endpoints[2]{};
    u64 m_transfer_timeout{};
    UEvent m_uevent{};
    // std::vector<UEvent*> m_cancel_events{};
    // aligned buffer that transfer data is copied to and from.
    // a vector is used to avoid multiple alloc within the transfer loop.
    PageAlignedVector m_aligned{};
    std::string m_transfer_file_name{};
};

} // namespace sphaira::yati::source
