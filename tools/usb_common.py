import struct
from time import sleep
from usb.core import USBError, find as usb_find
from usb.util import (
    ENDPOINT_IN,
    ENDPOINT_OUT,
    endpoint_direction,
    find_descriptor
)

SPLASH = """                                           
      :@@@@@@@@@@@@@@@@@@@@@@@@@@:      
      #@                        @#      
      #@                        @#      
      #@                        @#      
      #@    @@@@@@    @@@@@@    @#      
      #@    @@@@@@    @@@@@@    @#      
      #@    @@@@@@    @@@@@@    @#      
      #@                        @#      
      #@                        @#      
      #@                        @#      
    @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@   
   @          @%         @@@@@@@@@@@@  
  #@  *@@@@*  @%         @@@@@@@@@@@@@  
  #@ @@@@@@@@ @%         @@@@@@@@@@@@@  
  #@ @@@@@@@@ @%         @@@@@@@@@@@@@  
  #@  *@@@@*  @%         @@@@@@@@@@@@@  
  #@          @%         @@@@@@@@@@@@@  
  #@          @%         @@@@@@@@@@@@@  
  #@          @%         @@@@@@@@@@@@@  
  #@          @%         @@@@@@@@@@@@@  
  #@          @%         @@@@@@@@@@@@@  
  #@          @%         @@@@@=--=@@@@  
  #@          @%         @@@-      -@@  
  #@          @%         @@@        @@  
  #@          @%         @@@-      -@@  
  =@@         @%         @@@@@=--=@@@@  
   =@@        @%         @@@@@@@@@@@@   
     @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@                                      
"""

class USB_ENUM:
    # (SPH0) for the script and Switch
    MAGIC = 0x53504830 

    # Commands
    CMD_QUIT = 0
    CMD_OPEN = 1
    CMD_EXPORT = 1

    # Result Codes
    RESULT_OK = 0
    RESULT_ERROR = 1

    # Flags
    FLAG_NONE = 0
    FLAG_STREAM = 1 << 0

    # Switch Vendor / Product ID
    VENDOR_ID = 0x057E
    PRODUCT_ID = 0x3000

    ENABLE_ZLT = 0


def find_switch() -> object | None:
    return usb_find(
        idVendor=USB_ENUM.VENDOR_ID,
        idProduct=USB_ENUM.PRODUCT_ID
    )

class Usb:
    def __init__(self):
        self._out_ep = None
        self._in_ep = None
        self._packet_size = 0
        self._packet_index = 0

    def wait_for_connect(self) -> None:
        print(SPLASH)
        print("Waiting for Switch...", end="")

        dev = None
        while dev is None:
            if (dev := find_switch()):
                break
            print(".", end="")
            sleep(0.5)

        print("Found the Switch!\n")
        cfg = None

        print("Getting configuration...")
        try:
            cfg = dev.get_active_configuration()
            print("Found active config")
        except USBError:
            print("No currently active config")
            cfg = None

        if cfg is None:
            dev.reset()
            dev.set_configuration()
            cfg = dev.get_active_configuration()

        is_out_ep = lambda ep: endpoint_direction(ep.bEndpointAddress) == ENDPOINT_OUT
        is_in_ep = lambda ep: endpoint_direction(ep.bEndpointAddress) == ENDPOINT_IN
        self._out_ep = find_descriptor(cfg[(0,0)], custom_match=is_out_ep)
        self._in_ep = find_descriptor(cfg[(0,0)], custom_match=is_in_ep)

        if not self._out_ep:
            raise ValueError("Failed to get USB OUT address")
        if not self._in_ep:
            raise ValueError("Failed to get USB IN address")

        print(f"iManufacturer: {dev.manufacturer} \
            iProduct: {dev.product} \
            iSerialNumber: {dev.serial_number}")

        print(f"bcdUSB: {hex(dev.bcdUSB)} \
            bMaxPacketSize0: {dev.bMaxPacketSize0}")
        self._packet_size = 1 << dev.bMaxPacketSize0

    def read(self, size: int, timeout: int = 0) -> bytes:
        if (ENABLE_ZLT and size and not (size % self._packet_size)):
            size += 1
        return self._in_ep.read(size, timeout)

    def write(self, buf: bytes, timeout: int = 0) -> int:
        packet = self._packet_index
        self._packet_index += 1
        # Todo, implement packet index long
        return self._out_ep.write(data=buf, timeout=timeout)

    def get_send_header(self) -> tuple[int, int, int]:
        header = self.read(16)
        magic, arg2, arg3, arg4 = struct.unpack('<IIII', header)

        if magic != MAGIC:
            raise Exception("Unexpected magic {}".format(magic))

        return arg2, arg3, arg4

    def get_send_data_header(self) -> tuple[int, int, int]:
        header = self.read(16)
        return struct.unpack('<QII', header)

    def send_result(self, result: int, arg3: int = 0, arg4: int = 0) -> None:
        send_data = struct.pack('<IIII', MAGIC, result, arg3, arg4)
        self.write(send_data)