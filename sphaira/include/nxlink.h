// any data sent will be accepted and if all the data is sent whilst the
// thread is active, then an event will be sent to launch the .nro.
#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    NxlinkCallbackType_Connected, // data = none
    NxlinkCallbackType_WriteBegin, // data = file
    NxlinkCallbackType_WriteProgress, // data = progress
    NxlinkCallbackType_WriteEnd, // data = file
} NxlinkCallbackType;

typedef struct {
    char filename[0x301];
} NxlinkCallbackDataConnect;

typedef struct {
    char filename[0x301];
} NxlinkCallbackDataFile;

typedef struct {
    long long offset;
    long long size;
} NxlinkCallbackDataProgress;

typedef struct {
    NxlinkCallbackType type;
    union {
        NxlinkCallbackDataFile file;
        NxlinkCallbackDataProgress progress;
    };
} NxlinkCallbackData;

typedef void(*NxlinkCallback)(const NxlinkCallbackData* data);

// start the server thread and wait for connection.
bool nxlinkInitialize(NxlinkCallback callback);

// signal for the event to close and then join the thread.
void nxlinkExit();

// async the exit, call this first and then call exit later to avoid blocking.
void nxlinkSignalExit();

#ifdef __cplusplus
}
#endif
