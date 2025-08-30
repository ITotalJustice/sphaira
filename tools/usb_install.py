import crc32c
import glob
from io import BufferedReader
import sys
import os
from pathlib import Path
from usb_common import Usb, USB_ENUM as UE

try:
    import rarfile
    has_rar_support = True
except:
    has_rar_support = False

# list of installable exts that sphaira supports.
INSTALLABLE_EXTS = (".nsp", ".xci", ".nsz", ".xcz")
# list of supported extensions passed via args.
ACCEPTED_EXTS = INSTALLABLE_EXTS + tuple(".rar")

# real path, internal path (same if not .rar)
paths: list[tuple[str, str]] = []

def send_file_info_result(usb: Usb, result: int, file_size: int, flags: int):
    size_lsb = file_size & 0xFFFFFFFF
    size_msb = ((file_size >> 32) & 0xFFFF) | (flags << 16)
    usb.send_result(result, size_msb, size_lsb)

def file_transfer_loop(usb: Usb, file: BufferedReader, flags: int) -> None:
    print("> Transfer Loop")

    while True:
        # get offset + size.
        [off, size, _] = usb.get_send_data_header()

        # check if we should finish now.
        if off == 0 and size == 0:
            usb.send_result(UE.RESULT_OK)
            break

        # if we cannot seek, ensure that sphaira doesn't try to seek backwards.
        if (flags & UE.FLAG_STREAM) and off < file.tell():
            print(">> Error: Tried to seek on file without random access.")
            usb.send_result(UE.RESULT_ERROR)
            continue

        # read file and calculate the hash.
        try:
            file.seek(off)
            buf = file.read(size)
        except BlockingIOError as e:
            print(f">> Error: Failed to read: {e.filename} at: {off} size: {size} error: {e}")
            usb.send_result(UE.RESULT_ERROR)
            continue

        # respond back with the length of the data and the crc32c.
        usb.send_result(UE.RESULT_OK, len(buf), crc32c.crc32c(buf))

        # send the data.
        usb.write(buf)

def wait_for_input(usb: Usb, file_index: int) -> None:
    print("now waiting for intput\n")

    # open file / rar. (todo: learn how to make a class with inheritance)
    try:
        path, internal_path = paths[file_index]
        flags: int = UE.FLAG_NONE

        if path.endswith(".rar"):
            with rarfile.RarFile(path, part_only=True) as rf:
                info = rf.getinfo(internal_path)
                with rf.open(internal_path) as file:
                    # if the file is compressed, disable seek.
                    if info.compress_type != rarfile.RAR_M0:
                        flags |= UE.FLAG_STREAM

                    print("opened file: {} flags: {}".format(internal_path, flags))
                    send_file_info_result(usb, UE.RESULT_OK, info.file_size, flags)
                    file_transfer_loop(usb, file, flags)
        else:
            with open(path, "rb") as file:
                print("opened file {}".format(path))
                file.seek(0, os.SEEK_END)
                file_size = file.tell()
                send_file_info_result(usb, UE.RESULT_OK, file_size, flags)
                file_transfer_loop(usb, file, flags)

    except OSError as e:
        print("Error: failed to open: {} error: {}".format(e.filename, str(e)))
        usb.send_result(UE.RESULT_ERROR)

def add_file_to_install_list(path: str) -> None:
    # if the type if a rar, check if it contains a support ext internally.
    if path.endswith(".rar"):
        if has_rar_support:
            with rarfile.RarFile(path, part_only=True) as rf:
                for f in rf.infolist():
                    if f.filename.endswith(INSTALLABLE_EXTS):
                        print("Adding file: {} type: RAR".format(f.filename))
                        paths.append([path, f.filename])
                        break
        else:
            print("Warning: rar support disabled as rarfile is not installed")
            print("To enable rar support, enable with `pip install rarfile` and install unrar")

    elif path.endswith(INSTALLABLE_EXTS):
        print("Adding file: {} type: FILE".format(path))
        paths.append([path, path])

if __name__ == '__main__':
    print(SPLASH)

    # check which mode the user has selected.
    args = len(sys.argv)
    if (args != 2):
        print("either run python usb_total.py game.nsp OR drag and drop the game onto the python file (if python is in your path)")
        sys.exit(1)

    # build a list of files to install.
    path = sys.argv[1]
    if os.path.isfile(path):
        add_file_to_install_list(path)
    elif os.path.isdir(path):
        for f in glob.glob(path + "/**/*.*", recursive=True):
            if os.path.isfile(f):
                add_file_to_install_list(f)
    else:
        raise ValueError('Must be a file!')

    usb: Usb = Usb()

    try:
        # get usb endpoints.
        usb.wait_for_connect()

        # build string table.
        string_table: bytes
        for _, path in paths:
            string_table += bytes(Path(path).name.__str__(), 'utf8') + b'\n'

        # this reads the send header and checks the magic.
        usb.get_send_header()

        # send recv and string table.
        usb.send_result(UE.RESULT_OK, len(string_table))
        usb.write(string_table)

        # wait for command.
        while True:
            cmd, arg3, arg4 = usb.get_send_header()

            if cmd == UE.CMD_QUIT:
                usb.send_result(UE.RESULT_OK)
                break
            elif cmd == UE.CMD_OPEN:
                wait_for_input(usb, arg3)
            else:
                usb.send_result(UE.RESULT_ERROR)
                break

    except Exception as inst:
        print("An exception occurred " + str(inst))
