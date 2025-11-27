import crc32c
import sys
import os
from pathlib import Path
from usb_common import *

def get_file_name(usb: Usb, name_length: int) -> str:
    return bytes(usb.read(name_length)).decode('utf-8')

def create_file_folder(root: Path, file_path: Path) -> Path:
    # todo: check if it already exists.
    full_path = Path(root + "/" + file_path)
    full_path.parent.mkdir(exist_ok=True, parents=True)
    print("created folder")

    return full_path

def wait_for_input(usb: Usb, path: Path) -> None:
    print("now waiting for intput\n")

    with open(path, "wb") as file:
        print("opened file {}".format(path))

        while True:
            [off, size, crc32c_want] = usb.get_send_data_header()

            # todo: this isn't needed really.
            usb.send_result(RESULT_OK)

            # check if we should finish now.
            if (off == 0 and size == 0):
                break

            # read the buffer and calculate the crc32c.
            buf = usb.read(size)
            crc32c_got = crc32c.crc32c(buf)

            # validate the crc32c matches.
            if (crc32c_want != crc32c_got):
                usb.send_result(RESULT_ERROR)
                continue

            try:
                file.seek(off)
                file.write(buf)
                usb.send_result(RESULT_OK)
            except BlockingIOError as e:
                print("Error: failed to write: {} at: {} size: {} error: {}".format(e.filename, off, size, str(e)))
                usb.send_result(RESULT_ERROR)

if __name__ == '__main__':
    print("hello world")

    # check which mode the user has selected.
    args = len(sys.argv)
    if (args != 2):
        print("pass the folder path")
        sys.exit(1)

    root_path = sys.argv[1]

    if (not os.path.isdir(root_path)):
        raise ValueError('must be a dir!')

    usb = Usb()

    try:
        # get usb endpoints.
        usb.wait_for_connect()

        # wait for command.
        while True:
            [cmd, arg3, arg4] = usb.get_send_header()

            if (cmd == CMD_QUIT):
                usb.send_result(RESULT_OK)
                break
            elif (cmd == CMD_EXPORT):
                usb.send_result(RESULT_OK)

                # todo: handle and return errors here.
                file_name = get_file_name(usb, arg3)
                full_path = create_file_folder(root_path, file_name)
                usb.send_result(RESULT_OK)

                wait_for_input(usb, full_path)
            else:
                usb.send_result(RESULT_ERROR)
                break

    except Exception as inst:
        print("An exception occurred " + str(inst))
