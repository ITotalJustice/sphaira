# based on usb.py from Tinfoil, by Adubbz.
import struct
import sys
import os
import usb.core
import usb.util
import time
import glob
from pathlib import Path

# magic number (SPHA) for the script and switch.
MAGIC = 0x53504841
# version of the usb script.
VERSION = 2
# list of supported extensions.
EXTS = (".nsp", ".xci", ".nsz", ".xcz")

def verify_switch(bcdUSB, count, in_ep, out_ep):
    header = in_ep.read(8, timeout=0)
    switch_magic = struct.unpack('<I', header[0:4])[0]
    switch_version = struct.unpack('<I', header[4:8])[0]

    if switch_magic != MAGIC:
        raise Exception("Unexpected magic {}".format(switch_magic))

    if switch_version != VERSION:
        raise Exception("Unexpected version {}".format(switch_version))

    send_data = struct.pack('<IIII', MAGIC, VERSION, bcdUSB, count)
    out_ep.write(data=send_data, timeout=0)

def send_file_info(path, in_ep, out_ep):
    file_name = Path(path).name
    file_size = Path(path).stat().st_size
    file_name_len = len(file_name)

    send_data = struct.pack('<QQ', file_size, file_name_len)
    out_ep.write(data=send_data, timeout=0)
    out_ep.write(data=file_name, timeout=0)

def wait_for_input(path, in_ep, out_ep):
    buf = None
    predicted_off = 0
    print("now waiting for intput\n")

    with open(path, "rb") as file:
        while True:
            header = in_ep.read(24, timeout=0)

            range_offset = struct.unpack('<Q', header[8:16])[0]
            range_size = struct.unpack('<Q', header[16:24])[0]

            if (range_offset == 0 and range_size == 0):
                break

            if (buf != None and range_offset == predicted_off and range_size == len(buf)):
                # print("predicted the read off {} size {}".format(predicted_off, len(buf)))
                pass
            else:
                file.seek(range_offset)
                buf = file.read(range_size)

            if (len(buf) != range_size):
                # print("off: {} size: {}".format(range_offset, range_size))
                raise ValueError('bad buf size!!!!!')

            result = out_ep.write(data=buf, timeout=0)
            if (len(buf) != result):
                print("off: {} size: {}".format(range_offset, range_size))
                raise ValueError('bad result!!!!!')

            predicted_off = range_offset + range_size
            buf = file.read(range_size)

if __name__ == '__main__':
    print("hello world")

    # check which mode the user has selected.
    args = len(sys.argv)
    if (args != 2):
        print("either run python usb_total.py game.nsp OR drag and drop the game onto the python file (if python is in your path)")
        sys.exit(1)

    path = sys.argv[1]
    files = []

    if os.path.isfile(path) and path.endswith(EXTS):
        files.append(path)
    elif os.path.isdir(path):
        for f in glob.glob(path + "/**/*.*", recursive=True):
            if os.path.isfile(f) and f.endswith(EXTS):
                files.append(f)
    else:
        raise ValueError('must be a file!')

    # for file in files:
    #     print("found file: {}".format(file))

    # Find the switch
    print("waiting for switch...\n")
    dev = None

    while (dev is None):
        dev = usb.core.find(idVendor=0x057E, idProduct=0x3000)
        time.sleep(0.5)

    print("found the switch!\n")

    cfg = None

    try:
        cfg = dev.get_active_configuration()
        print("found active config")
    except usb.core.USBError:
        print("no currently active config")
        cfg = None

    if cfg is None:
        dev.set_configuration()
        cfg = dev.get_active_configuration()

    is_out_ep = lambda ep: usb.util.endpoint_direction(ep.bEndpointAddress) == usb.util.ENDPOINT_OUT
    is_in_ep = lambda ep: usb.util.endpoint_direction(ep.bEndpointAddress) == usb.util.ENDPOINT_IN
    out_ep = usb.util.find_descriptor(cfg[(0,0)], custom_match=is_out_ep)
    in_ep = usb.util.find_descriptor(cfg[(0,0)], custom_match=is_in_ep)
    assert out_ep is not None
    assert in_ep is not None

    print("iManufacturer: {} iProduct: {} iSerialNumber: {}".format(dev.manufacturer, dev.product, dev.serial_number))
    print("bcdUSB: {} bMaxPacketSize0: {}".format(hex(dev.bcdUSB), dev.bMaxPacketSize0))

    try:
        verify_switch(dev.bcdUSB, len(files), in_ep, out_ep)

        for file in files:
            print("installing file: {}".format(file))
            send_file_info(file, in_ep, out_ep)
            wait_for_input(file, in_ep, out_ep)
        dev.reset()
    except Exception as inst:
        print("An exception occurred " + str(inst))
