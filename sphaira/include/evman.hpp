// simple thread-safe list of events.
#pragma once

#include <optional>
#include <variant>
#include <list>
#include <string>
#include <switch.h>
#include <nxlink.h>
#include "download.hpp"

namespace sphaira::evman {

struct LaunchNroEventData {
    std::string path;
    std::string argv;
};

struct ExitEventData {
    bool dummy;
};

using EventData = std::variant<
    LaunchNroEventData,
    ExitEventData,
    NxlinkCallbackData,
    curl::DownloadEventData
>;

// returns number of events
auto count() -> std::size_t;

// thread-safe
auto push(const EventData& e, bool remove_matching = true) -> bool;
auto push(EventData&& e, bool remove_matching = true) -> bool;

// events are returned FIFO style, so if you push event a,b,c
// then pop() will return a then b then c.
auto pop() -> std::optional<EventData>;

// this pops all events, this is ideal to stop the main thread from
// hanging if loads of events are pushed and popped at the same time.
auto popall() -> std::list<EventData>;

} // namespace sphaira::evman
