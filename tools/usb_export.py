import crc32c
import sys
import os
from usb_common import Usb, USB_ENUM as UE, SPLASH

def get_file_name(usb: Usb, name_length: int) -> str:
    return bytes(usb.read(name_length)).decode('utf-8')

def create_file_folder(root: os.PathLike, target: os.PathLike) -> bool, os.PathLike:
    """
    Creates a recursive folder structure at a given location
    Returns a boolean indicating if it already exists and the absolute path 
    """
    path = os.abspath(os.path.join(root, target))
    parent = os.path.dirname(path)
    if not (os.path.exists(parent)):
        os.makedirs(path)
        print(f"Created folder {path}")
    else:
        print(f"Parent folder already exists {path}")
    exists_already = os.path.exists(path)
    return exists_already, path

def wait_for_input(usb: Usb, path: os.PathLike) -> None:
    print("now waiting for intput\n")

    with open(path, "wb") as f:
        print(f"Opened file {path}")

        while True:
            [off, size, crc32c_want] = usb.get_send_data_header()

            # todo: this isn't needed really.
            usb.send_result(UE.RESULT_OK)

            # check if we should finish now.
            if off == 0 and size == 0:
                break

            # read the buffer and calculate the crc32c.
            buf = usb.read(size)
            crc32c_got = crc32c.crc32c(buf)

            # validate the crc32c matches.
            if crc32c_want != crc32c_got:
                usb.send_result(UE.RESULT_ERROR)
                continue

            try:
                f.seek(off)
                f.write(buf)
                usb.send_result(UE.RESULT_OK)
            except BlockingIOError as e:
                print(f"Error: failed to write: {e.filename} at: {off} size: {size} error: {str(e)}")
                usb.send_result(UE.RESULT_ERROR)

if __name__ == '__main__':
    # Check which mode the user has selected.
    if (len(sys.argv) != 2):
        print("Run python usb_total.py \"path/to/game.nsp\" OR drag-and-drop the game onto the .py file (Python must be in your path)")
        sys.exit(1)

    root_path = sys.argv[1]

    if (not os.path.isdir(root_path)):
        raise ValueError('')

    usb = Usb()

    try:
        # get usb endpoints.
        usb.wait_for_connect()

        # wait for command.
        while True:
            [cmd, arg3, arg4] = usb.get_send_header()

            if (cmd == UE.CMD_QUIT):
                usb.send_result(UE.RESULT_OK)
                break
            elif (cmd == UE.CMD_EXPORT):
                usb.send_result(UE.RESULT_OK)

                # todo: handle and return errors here.
                file_name = get_file_name(usb, arg3)
                exists_already, full_path = create_file_folder(root_path, file_name)
                usb.send_result(UE.RESULT_OK)

                wait_for_input(usb, full_path)
            else:
                usb.send_result(UE.RESULT_ERROR)
                break

    except Exception as e:
        print(f"An exception occurred - {e} ")
