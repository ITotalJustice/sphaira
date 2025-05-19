#pragma once

#include <vector>
#include <string>
#include <new>
#include <switch.h>

namespace sphaira::usb {

struct Base {
    enum { USBModule = 523 };

    enum : Result {
        Result_Cancelled = MAKERESULT(USBModule, 100),
    };

    Base(u64 transfer_timeout);

    // sets up usb.
    virtual Result Init() = 0;

    // returns 0 if usb is connected to a device.
    virtual Result IsUsbConnected(u64 timeout) = 0;

    // transfers a chunk of data, check out_size_transferred for how much was transferred.
    Result TransferPacketImpl(bool read, void *page, u32 size, u32 *out_size_transferred, u64 timeout);
    Result TransferPacketImpl(bool read, void *page, u32 size, u32 *out_size_transferred) {
        return TransferPacketImpl(read, page, size, out_size_transferred, m_transfer_timeout);
    }

    // transfers all data.
    Result TransferAll(bool read, void *data, u32 size, u64 timeout);
    Result TransferAll(bool read, void *data, u32 size) {
        return TransferAll(read, data, size, m_transfer_timeout);
    }

    // returns the cancel event.
    auto GetCancelEvent() {
        return &m_uevent;
    }

    // cancels an in progress transfer.
    void Cancel() {
        ueventSignal(GetCancelEvent());
    }

    auto& GetTransferBuffer() {
        return m_aligned;
    }

    auto GetTransferTimeout() const {
        return m_transfer_timeout;
    }

public:
    // custom allocator for std::vector that respects alignment.
    // https://en.cppreference.com/w/cpp/named_req/Allocator
    template <typename T, std::size_t Align>
    struct CustomVectorAllocator {
    public:
        // https://en.cppreference.com/w/cpp/memory/new/operator_new
        auto allocate(std::size_t n) -> T* {
            n = (n + (Align - 1)) &~ (Align - 1);
            return new(align) T[n];
        }

        // https://en.cppreference.com/w/cpp/memory/new/operator_delete
        auto deallocate(T* p, std::size_t n) noexcept -> void {
            // ::operator delete[] (p, n, align);
            ::operator delete[] (p, align);
        }

    private:
        static constexpr inline std::align_val_t align{Align};
    };

    template <typename T>
    struct PageAllocator : CustomVectorAllocator<T, 0x1000> {
        using value_type = T; // used by std::vector
    };

    using PageAlignedVector = std::vector<u8, PageAllocator<u8>>;

protected:
    enum UsbSessionEndpoint {
        UsbSessionEndpoint_In = 0,
        UsbSessionEndpoint_Out = 1,
    };

    virtual Event *GetCompletionEvent(UsbSessionEndpoint ep) = 0;
    virtual Result WaitTransferCompletion(UsbSessionEndpoint ep, u64 timeout) = 0;
    virtual Result TransferAsync(UsbSessionEndpoint ep, void *buffer, u32 size, u32 *out_xfer_id) = 0;
    virtual Result GetTransferResult(UsbSessionEndpoint ep, u32 xfer_id, u32 *out_requested_size, u32 *out_transferred_size) = 0;

private:
    u64 m_transfer_timeout{};
    UEvent m_uevent{};
    PageAlignedVector m_aligned{};
};

} // namespace sphaira::usb
