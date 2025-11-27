/*
 * defines.h
 *
 * Copyright (c) 2020-2024, DarkMatterCore <pabloacurielz@gmail.com>.
 *
 * This file is part of nxdumptool (https://github.com/DarkMatterCore/nxdumptool).
 *
 * nxdumptool is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * nxdumptool is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#pragma once

#ifndef __DEFINES_H__
#define __DEFINES_H__

/* Broadly useful language defines. */

#define MEMBER_SIZE(type, member)       sizeof(((type*)NULL)->member)

#define MAX_ELEMENTS(x)                 ((sizeof((x))) / (sizeof((x)[0])))

#define ALIGN_UP(x, y)                  (((x) + ((y) - 1)) & ~((y) - 1))
#define ALIGN_DOWN(x, y)                ((x) & ~((y) - 1))
#define IS_ALIGNED(x, y)                (((x) & ((y) - 1)) == 0)

#define IS_POWER_OF_TWO(x)              ((x) > 0 && ((x) & ((x) - 1)) == 0)

#define DIVIDE_UP(x, y)                 (((x) + ((y) - 1)) / (y))

#define CONCATENATE_IMPL(s1, s2)        s1##s2
#define CONCATENATE(s1, s2)             CONCATENATE_IMPL(s1, s2)

#define ANONYMOUS_VARIABLE(pref)        CONCATENATE(pref, __COUNTER__)

#define NON_COPYABLE(cls) \
    cls(const cls&) = delete; \
    cls& operator=(const cls&) = delete

#define NON_MOVEABLE(cls) \
    cls(cls&&) = delete; \
    cls& operator=(cls&&) = delete

#define ALWAYS_INLINE                   inline __attribute__((always_inline))
#define ALWAYS_INLINE_LAMBDA            __attribute__((always_inline))

#define CLEANUP(func)                   __attribute__((__cleanup__(func)))

#define NXDT_ASSERT(name, size)         static_assert(sizeof(name) == (size), "Bad size for " #name "! Expected " #size ".")

#endif  /* __DEFINES_H__ */
