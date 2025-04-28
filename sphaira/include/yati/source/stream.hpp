#pragma once

#include "base.hpp"
#include <vector>
#include <switch.h>

namespace sphaira::yati::source {

// streams are for data that do not allow for random access,
// such as FTP or MTP.
struct Stream : Base {
    virtual ~Stream() = default;
    virtual Result ReadChunk(void* buf, s64 size, u64* bytes_read) = 0;

    Result Read(void* buf, s64 off, s64 size, u64* bytes_read) override;

    bool IsStream() const override {
        return true;
    }

protected:
    Result m_open_result{};

private:
    s64 m_offset{};
};

} // namespace sphaira::yati::source
