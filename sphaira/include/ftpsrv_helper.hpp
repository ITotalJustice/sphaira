#pragma once

#include <functional>

namespace sphaira::ftpsrv {

bool Init();
void Exit();

using OnInstallStart = std::function<bool(void* user, const char* path)>;
using OnInstallWrite = std::function<bool(void* user, const void* buf, size_t size)>;
using OnInstallClose = std::function<void(void* user)>;

void InitInstallMode(void* user, OnInstallStart on_start, OnInstallWrite on_write, OnInstallClose on_close);
void DisableInstallMode();

unsigned GetPort();
bool IsAnon();
const char* GetUser();
const char* GetPass();

} // namespace sphaira::ftpsrv
