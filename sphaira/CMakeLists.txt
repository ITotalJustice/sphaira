cmake_minimum_required(VERSION 3.13)

set(sphaira_VERSION 0.5.0)

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

    source/ui/error_box.cpp
    source/ui/notification.cpp
    source/ui/nvg_util.cpp
    source/ui/option_box.cpp
    source/ui/option_list.cpp
    source/ui/popup_list.cpp
    source/ui/progress_box.cpp
    source/ui/scrollable_text.cpp
    source/ui/scrollbar.cpp
    source/ui/sidebar.cpp
    source/ui/widget.cpp

    source/app.cpp
    source/download.cpp
    source/option.cpp
    source/evman.cpp
    source/fs.cpp
    source/image.cpp
    source/log.cpp
    source/main.cpp
    source/nro.cpp
    source/nxlink.cpp
    source/owo.cpp
    source/swkbd.cpp
    source/web.cpp
    source/i18n.cpp
    source/ftpsrv_helper.cpp
)

target_compile_definitions(sphaira PRIVATE
    -DAPP_VERSION="${sphaira_VERSION}"
    -DAPP_VERSION_HASH="${sphaira_VERSION_HASH}"
)

include(FetchContent)
set(FETCHCONTENT_QUIET FALSE)

FetchContent_Declare(ftpsrv
    GIT_REPOSITORY https://github.com/ITotalJustice/ftpsrv.git
    GIT_TAG 8d5a14e
)

FetchContent_Declare(libhaze
    GIT_REPOSITORY https://github.com/ITotalJustice/libhaze.git
    GIT_TAG 3244b9e
)

FetchContent_Declare(libpulsar
    GIT_REPOSITORY https://github.com/ITotalJustice/switch-libpulsar.git
    GIT_TAG d729be3
)

FetchContent_Declare(nanovg
    GIT_REPOSITORY https://github.com/ITotalJustice/nanovg-deko3d.git
    GIT_TAG 1902b38
)

FetchContent_Declare(stb
    GIT_REPOSITORY https://github.com/nothings/stb.git
    GIT_TAG 5c20573
)

FetchContent_Declare(yyjson
    GIT_REPOSITORY https://github.com/ibireme/yyjson.git
    GIT_TAG 0.10.0
)

FetchContent_Declare(minIni-sphaira
    GIT_REPOSITORY https://github.com/ITotalJustice/minIni-nx.git
    GIT_TAG 63ec295
)

set(MININI_LIB_NAME minIni-sphaira)
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
set(NANOVG_STBI_STATIC OFF)
set(NANOVG_STBTT_STATIC ON)

set(YYJSON_DISABLE_READER OFF)
set(YYJSON_DISABLE_WRITER OFF)
set(YYJSON_DISABLE_UTILS ON)
set(YYJSON_DISABLE_FAST_FP_CONV ON)
set(YYJSON_DISABLE_NON_STANDARD ON)
set(YYJSON_DISABLE_UTF8_VALIDATION ON)
set(YYJSON_DISABLE_UNALIGNED_MEMORY_ACCESS OFF)

set(FTPSRV_LIB_BUILD TRUE)
set(FTPSRV_LIB_SOCK_UNISTD TRUE)
set(FTPSRV_LIB_VFS_CUSTOM ${CMAKE_CURRENT_SOURCE_DIR}/include/ftpsrv_helper.hpp)
set(FTPSRV_LIB_PATH_SIZE 0x301)
set(FTPSRV_LIB_SESSIONS 32)
set(FTPSRV_LIB_BUF_SIZE 1024*64)

FetchContent_MakeAvailable(
    ftpsrv
    libhaze
    libpulsar
    nanovg
    stb
    minIni-sphaira
    yyjson
)

# todo: upstream cmake
add_library(libhaze
    ${libhaze_SOURCE_DIR}/source/async_usb_server.cpp
    ${libhaze_SOURCE_DIR}/source/device_properties.cpp
    ${libhaze_SOURCE_DIR}/source/event_reactor.cpp
    ${libhaze_SOURCE_DIR}/source/haze.cpp
    ${libhaze_SOURCE_DIR}/source/ptp_object_database.cpp
    ${libhaze_SOURCE_DIR}/source/ptp_object_heap.cpp
    ${libhaze_SOURCE_DIR}/source/ptp_responder_android_operations.cpp
    ${libhaze_SOURCE_DIR}/source/ptp_responder_mtp_operations.cpp
    ${libhaze_SOURCE_DIR}/source/ptp_responder_ptp_operations.cpp
    ${libhaze_SOURCE_DIR}/source/ptp_responder.cpp
    ${libhaze_SOURCE_DIR}/source/usb_session.cpp
)
target_include_directories(libhaze PUBLIC ${libhaze_SOURCE_DIR}/include)
set_target_properties(libhaze PROPERTIES
    C_STANDARD 11
    C_EXTENSIONS ON
    CXX_STANDARD 20
    CXX_EXTENSIONS ON
    # force optimisations in debug mode as otherwise vapor errors
    # due to force_inline attribute failing...
    COMPILE_OPTIONS "$<$<CONFIG:Debug>:-Os>"
)

# todo: upstream cmake
add_library(libpulsar
    ${libpulsar_SOURCE_DIR}/src/archive/archive_file.c
    ${libpulsar_SOURCE_DIR}/src/archive/archive.c
    ${libpulsar_SOURCE_DIR}/src/bfgrp/bfgrp_location.c
    ${libpulsar_SOURCE_DIR}/src/bfgrp/bfgrp.c
    ${libpulsar_SOURCE_DIR}/src/bfsar/bfsar_file.c
    ${libpulsar_SOURCE_DIR}/src/bfsar/bfsar_group.c
    ${libpulsar_SOURCE_DIR}/src/bfsar/bfsar_sound.c
    ${libpulsar_SOURCE_DIR}/src/bfsar/bfsar_string.c
    ${libpulsar_SOURCE_DIR}/src/bfsar/bfsar_wave_archive.c
    ${libpulsar_SOURCE_DIR}/src/bfsar/bfsar.c
    ${libpulsar_SOURCE_DIR}/src/bfstm/bfstm_channel.c
    ${libpulsar_SOURCE_DIR}/src/bfstm/bfstm_info.c
    ${libpulsar_SOURCE_DIR}/src/bfstm/bfstm.c
    ${libpulsar_SOURCE_DIR}/src/bfwar/bfwar_file.c
    ${libpulsar_SOURCE_DIR}/src/bfwar/bfwar.c
    ${libpulsar_SOURCE_DIR}/src/bfwav/bfwav_info.c
    ${libpulsar_SOURCE_DIR}/src/bfwav/bfwav.c
    ${libpulsar_SOURCE_DIR}/src/bfwsd/bfwsd_sound_data.c
    ${libpulsar_SOURCE_DIR}/src/bfwsd/bfwsd_wave_id.c
    ${libpulsar_SOURCE_DIR}/src/bfwsd/bfwsd.c
    ${libpulsar_SOURCE_DIR}/src/player/player_load_formats.c
    ${libpulsar_SOURCE_DIR}/src/player/player_load_lookup.c
    ${libpulsar_SOURCE_DIR}/src/player/player_load.c
    ${libpulsar_SOURCE_DIR}/src/player/player.c
)
target_include_directories(libpulsar PUBLIC ${libpulsar_SOURCE_DIR}/include)
set_target_properties(libpulsar PROPERTIES
    C_STANDARD 11
    C_EXTENSIONS ON
)

add_library(stb INTERFACE)
target_include_directories(stb INTERFACE ${stb_SOURCE_DIR})

find_package(ZLIB REQUIRED)
find_library(minizip_lib minizip REQUIRED)
find_path(minizip_inc minizip REQUIRED)

find_package(CURL REQUIRED)
find_path(mbedtls_inc mbedtls REQUIRED)
find_library(mbedcrypto_lib mbedcrypto REQUIRED)

set_target_properties(sphaira PROPERTIES
    C_STANDARD 11
    C_EXTENSIONS ON
    CXX_STANDARD 23
    CXX_EXTENSIONS ON
)

target_link_libraries(sphaira PRIVATE
    ftpsrv
    libhaze
    libpulsar
    minIni-sphaira
    nanovg
    stb
    yyjson

    ${minizip_lib}
    ZLIB::ZLIB
    CURL::libcurl
    ${mbedcrypto_lib}
)

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
    VERSION ${CMAKE_PROJECT_VERSION}
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
