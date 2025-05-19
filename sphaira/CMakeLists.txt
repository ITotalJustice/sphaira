cmake_minimum_required(VERSION 3.13)

set(sphaira_VERSION 0.10.2)

project(sphaira
    VERSION ${sphaira_VERSION}
    LANGUAGES C CXX
)

find_package(Git REQUIRED)

execute_process(
    COMMAND "${GIT_EXECUTABLE}" rev-parse --short HEAD
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    OUTPUT_VARIABLE GIT_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND "${GIT_EXECUTABLE}" symbolic-ref --short HEAD
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    OUTPUT_VARIABLE GIT_BRANCH
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND "${GIT_EXECUTABLE}" status --porcelain
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    OUTPUT_VARIABLE GIT_DIRTY
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if (GIT_DIRTY)
    set(sphaira_VERSION_HASH "${sphaira_VERSION} ${GIT_COMMIT}")
else()
    set(sphaira_VERSION_HASH "${sphaira_VERSION}")
endif()

add_executable(sphaira
    source/ui/menus/appstore.cpp
    source/ui/menus/file_viewer.cpp
    source/ui/menus/filebrowser.cpp
    source/ui/menus/homebrew.cpp
    source/ui/menus/irs_menu.cpp
    source/ui/menus/main_menu.cpp
    source/ui/menus/menu_base.cpp
    source/ui/menus/themezer.cpp
    source/ui/menus/ghdl.cpp
    source/ui/menus/usb_menu.cpp
    source/ui/menus/ftp_menu.cpp
    source/ui/menus/gc_menu.cpp
    source/ui/menus/game_menu.cpp
    source/ui/menus/grid_menu_base.cpp

    source/ui/error_box.cpp
    source/ui/notification.cpp
    source/ui/nvg_util.cpp
    source/ui/option_box.cpp
    source/ui/popup_list.cpp
    source/ui/progress_box.cpp
    source/ui/scrollable_text.cpp
    source/ui/sidebar.cpp
    source/ui/widget.cpp
    source/ui/list.cpp
    source/ui/scrolling_text.cpp

    source/app.cpp
    source/download.cpp
    source/option.cpp
    source/evman.cpp
    source/fs.cpp
    source/image.cpp
    source/location.cpp
    source/log.cpp
    source/main.cpp
    source/nro.cpp
    source/nxlink.cpp
    source/owo.cpp
    source/swkbd.cpp
    source/i18n.cpp
    source/ftpsrv_helper.cpp

    source/usb/base.cpp
    source/usb/usbds.cpp
    source/usb/usbhs.cpp
    source/usb/usb_uploader.cpp

    source/yati/yati.cpp
    source/yati/container/nsp.cpp
    source/yati/container/xci.cpp
    source/yati/source/file.cpp
    source/yati/source/stdio.cpp
    source/yati/source/usb.cpp
    source/yati/source/stream.cpp
    source/yati/source/stream_file.cpp

    source/yati/nx/es.cpp
    source/yati/nx/keys.cpp
    source/yati/nx/nca.cpp
    source/yati/nx/ncm.cpp
    source/yati/nx/ns.cpp
    source/yati/nx/nxdumptool_rsa.c
)

target_compile_definitions(sphaira PRIVATE
    -DAPP_VERSION="${sphaira_VERSION}"
    -DAPP_VERSION_HASH="${sphaira_VERSION_HASH}"
    -DCURL_NO_OLDIES=1
)

target_compile_options(sphaira PRIVATE
    -Wall
    -Wextra

    # unsure if it's a good idea to enable these by default as
    # it may cause breakage upon compiler updates.
    # -Werror
    # -Wfatal-errors

    # disabled as nx uses s64 for size and offset, however stl uses size_t instead, thus
    # there being a lot of warnings.
    -Wno-sign-compare
    # disabled as many overriden methods don't use the params.
    -Wno-unused-parameter
    # pedantic warning, missing fields are set to 0.
    -Wno-missing-field-initializers
    # disabled as it warns for strcat 2 paths together, but it will never
    # overflow due to fs enforcing a max path len anyway.
    -Wno-format-truncation

    # the below are taken from my gba emulator, they've served me well ;)
    -Wformat-overflow=2
    -Wundef
    -Wmissing-include-dirs
    -fstrict-aliasing
    -Wstrict-overflow=2
    -Walloca
    -Wduplicated-cond
    -Wwrite-strings
    -Wdate-time
    -Wlogical-op
    -Wpacked
    -Wcast-qual
    -Wcast-align
    -Wimplicit-fallthrough=5
    -Wsuggest-final-types
    -Wuninitialized
    -fimplicit-constexpr
    -Wmissing-requires
)

include(FetchContent)
set(FETCHCONTENT_QUIET FALSE)

FetchContent_Declare(ftpsrv
    GIT_REPOSITORY https://github.com/ITotalJustice/ftpsrv.git
    # GIT_TAG 1.2.2
    GIT_TAG f8a30fd
    SOURCE_SUBDIR NONE
)

FetchContent_Declare(libhaze
    GIT_REPOSITORY https://github.com/ITotalJustice/libhaze.git
    GIT_TAG 04f1526
)

FetchContent_Declare(libpulsar
    GIT_REPOSITORY https://github.com/ITotalJustice/switch-libpulsar.git
    GIT_TAG de656e4
)

FetchContent_Declare(nanovg
    GIT_REPOSITORY https://github.com/ITotalJustice/nanovg-deko3d.git
    GIT_TAG 845c9fc
)

FetchContent_Declare(stb
    GIT_REPOSITORY https://github.com/nothings/stb.git
    GIT_TAG 5c20573
)

FetchContent_Declare(yyjson
    GIT_REPOSITORY https://github.com/ibireme/yyjson.git
    GIT_TAG 0.10.0
)

FetchContent_Declare(minIni
    GIT_REPOSITORY https://github.com/ITotalJustice/minIni-nx.git
    GIT_TAG 11cac8b
)

FetchContent_Declare(zstd
    GIT_REPOSITORY https://github.com/facebook/zstd.git
    GIT_TAG v1.5.7
    SOURCE_SUBDIR build/cmake
)

set(USE_NEW_ZSTD ON)

set(ZSTD_BUILD_STATIC ON)
set(ZSTD_BUILD_SHARED OFF)
set(ZSTD_BUILD_COMPRESSION OFF)
set(ZSTD_BUILD_DECOMPRESSION ON)
set(ZSTD_BUILD_DICTBUILDER OFF)
set(ZSTD_LEGACY_SUPPORT OFF)
set(ZSTD_MULTITHREAD_SUPPORT OFF)
set(ZSTD_BUILD_PROGRAMS OFF)
set(ZSTD_BUILD_TESTS OFF)

set(MININI_LIB_NAME minIni)
set(MININI_USE_STDIO ON)
set(MININI_USE_NX ON)
set(MININI_USE_FLOAT OFF)

if (CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(NANOVG_DEBUG ON)
endif()
set(NANOVG_NO_JPEG OFF)
set(NANOVG_NO_PNG OFF)
set(NANOVG_NO_BMP ON)
set(NANOVG_NO_PSD ON)
set(NANOVG_NO_TGA ON)
set(NANOVG_NO_GIF ON)
set(NANOVG_NO_HDR ON)
set(NANOVG_NO_PIC ON)
set(NANOVG_NO_PNM ON)

set(YYJSON_DISABLE_READER OFF)
set(YYJSON_DISABLE_WRITER OFF)
set(YYJSON_DISABLE_UTILS ON)
set(YYJSON_DISABLE_FAST_FP_CONV ON)
set(YYJSON_DISABLE_NON_STANDARD ON)
set(YYJSON_DISABLE_UTF8_VALIDATION ON)
set(YYJSON_DISABLE_UNALIGNED_MEMORY_ACCESS OFF)

FetchContent_MakeAvailable(
    ftpsrv
    libhaze
    libpulsar
    nanovg
    stb
    minIni
    yyjson
    zstd
)

set(FTPSRV_LIB_BUILD TRUE)
set(FTPSRV_LIB_SOCK_UNISTD TRUE)
set(FTPSRV_LIB_VFS_CUSTOM ${ftpsrv_SOURCE_DIR}/src/platform/nx/vfs_nx.h)
set(FTPSRV_LIB_PATH_SIZE 0x301)
set(FTPSRV_LIB_SESSIONS 32)
set(FTPSRV_LIB_BUF_SIZE 1024*64)

# workaround until a64 container has latest libnx release.
if (NOT DEFINED USE_VFS_GC)
    set(USE_VFS_GC TRUE)
endif()

set(FTPSRV_LIB_CUSTOM_DEFINES
    USE_VFS_SAVE=$<BOOL:TRUE>
    USE_VFS_STORAGE=$<BOOL:TRUE>
    USE_VFS_GC=$<BOOL:${USE_VFS_GC}>
    USE_VFS_USBHSFS=$<BOOL:FALSE>
    VFS_NX_BUFFER_IO=$<BOOL:TRUE>
)

add_subdirectory(${ftpsrv_SOURCE_DIR} binary_dir)

add_library(ftpsrv_helper
    ${ftpsrv_SOURCE_DIR}/src/platform/nx/vfs_nx.c
    ${ftpsrv_SOURCE_DIR}/src/platform/nx/vfs/vfs_nx_none.c
    ${ftpsrv_SOURCE_DIR}/src/platform/nx/vfs/vfs_nx_root.c
    ${ftpsrv_SOURCE_DIR}/src/platform/nx/vfs/vfs_nx_fs.c
    ${ftpsrv_SOURCE_DIR}/src/platform/nx/vfs/vfs_nx_save.c
    ${ftpsrv_SOURCE_DIR}/src/platform/nx/vfs/vfs_nx_storage.c
    ${ftpsrv_SOURCE_DIR}/src/platform/nx/utils.c
)

target_link_libraries(ftpsrv_helper PUBLIC ftpsrv)
target_include_directories(ftpsrv_helper PUBLIC ${ftpsrv_SOURCE_DIR}/src/platform)

if (USE_VFS_GC)
    target_sources(ftpsrv_helper PRIVATE
        ${ftpsrv_SOURCE_DIR}/src/platform/nx/vfs/vfs_nx_gc.c
    )
endif()

add_library(stb INTERFACE)
target_include_directories(stb INTERFACE ${stb_SOURCE_DIR})

find_package(ZLIB REQUIRED)
find_library(minizip_lib minizip REQUIRED)
find_path(minizip_inc minizip REQUIRED)

find_package(CURL REQUIRED)
find_path(mbedtls_inc mbedtls REQUIRED)
find_library(mbedcrypto_lib mbedcrypto REQUIRED)

if (NOT USE_NEW_ZSTD)
    find_path(zstd_inc zstd.h REQUIRED)
    find_library(zstd_lib zstd REQUIRED)
endif()

set_target_properties(sphaira PROPERTIES
    C_STANDARD 11
    C_EXTENSIONS ON
    CXX_STANDARD 23
    CXX_EXTENSIONS ON
)

target_link_libraries(sphaira PRIVATE
    ftpsrv_helper
    libhaze
    libpulsar
    minIni
    nanovg
    stb
    yyjson

    ${minizip_lib}
    ZLIB::ZLIB
    CURL::libcurl
    ${mbedcrypto_lib}
)

if (USE_NEW_ZSTD)
    message(STATUS "USING UPSTREAM ZSTD")
    target_link_libraries(sphaira PRIVATE libzstd_static)
else()
    message(STATUS "USING LOCAL ZSTD")
    target_link_libraries(sphaira PRIVATE ${zstd_lib})
    target_include_directories(sphaira PRIVATE ${zstd_inc})
endif()

target_include_directories(sphaira PRIVATE
    include
    ${minizip_inc}
    ${mbedtls_inc}
)

# copy the romfs
file(COPY ${CMAKE_SOURCE_DIR}/assets/romfs DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# create assets target
dkp_add_asset_target(sphaira_romfs ${CMAKE_CURRENT_BINARY_DIR}/romfs)

# add hbl exefs to romfs, used for forwarders
dkp_install_assets(sphaira_romfs
    DESTINATION exefs
    TARGETS
        hbl_nso
        hbl_npdm
)

# add nanovg shaders to romfs
dkp_install_assets(sphaira_romfs
    DESTINATION shaders
    TARGETS
        fill_aa_fsh
        fill_fsh
        fill_vsh
)

# create nacp
nx_generate_nacp(
    OUTPUT sphaira.nacp
    NAME ${CMAKE_PROJECT_NAME}
    AUTHOR TotalJustice
    VERSION ${sphaira_VERSION}
)

# create nro
nx_create_nro(sphaira
    OUTPUT ${CMAKE_BINARY_DIR}/sphaira.nro
    ICON ${CMAKE_SOURCE_DIR}/assets/icon.jpg
    NACP sphaira.nacp
    ROMFS sphaira_romfs
)

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/switch/sphaira)

add_custom_command(
    TARGET sphaira_nro POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_BINARY_DIR}/sphaira.nro
        ${CMAKE_BINARY_DIR}/switch/sphaira/sphaira.nro
)

message(STATUS "generating nro in: ${CMAKE_BINARY_DIR}/sphaira.nro")
message(STATUS "run nxlink -s ${CMAKE_BINARY_DIR}/sphaira.nro")
