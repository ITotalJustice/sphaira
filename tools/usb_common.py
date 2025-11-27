import struct
import usb.core
import usb.util
import time
import crc32c

# magic number (SPH0) for the script and switch.
MAGIC = 0x53504830
PACKET_SIZE = 24

# commands
CMD_QUIT = 0
CMD_OPEN = 1
CMD_EXPORT = 1

# results
RESULT_OK = 0
RESULT_ERROR = 1

# flags
FLAG_NONE = 0
FLAG_STREAM = 1 << 0

class UsbPacket:
    STRUCT_FORMAT = "<6I"  # 6 unsigned 32-bit ints, little-endian

    def __init__(self, magic=MAGIC, arg2=0, arg3=0, arg4=0, arg5=0, crc32c_val=0):
        self.magic = magic
        self.arg2 = arg2
        self.arg3 = arg3
        self.arg4 = arg4
        self.arg5 = arg5
        self.crc32c = crc32c_val

    def pack(self):
        self.generate_crc32c()
        return struct.pack(self.STRUCT_FORMAT, self.magic, self.arg2, self.arg3, self.arg4, self.arg5, self.crc32c)

    @classmethod
    def unpack(cls, data):
        fields = struct.unpack(cls.STRUCT_FORMAT, data)
        return cls(*fields)

    def calculate_crc32c(self):
        data = struct.pack("<5I", self.magic, self.arg2, self.arg3, self.arg4, self.arg5)
        return crc32c.crc32c(data)

    def generate_crc32c(self):
        self.crc32c = self.calculate_crc32c()

    def verify(self):
        if self.crc32c != self.calculate_crc32c():
            raise ValueError("CRC32C mismatch")
        if self.magic != MAGIC:
            raise ValueError("Bad magic")
        return True

class SendPacket(UsbPacket):
    @classmethod
    def build(cls, cmd, arg3=0, arg4=0):
        packet = cls(MAGIC, cmd, arg3, arg4)
        packet.generate_crc32c()
        return packet

    def get_cmd(self):
        return self.arg2

class ResultPacket(UsbPacket):
    @classmethod
    def build(cls, result, arg3=0, arg4=0):
        packet = cls(MAGIC, result, arg3, arg4)
        packet.generate_crc32c()
        return packet

    def verify(self):
        super().verify()
        if self.arg2 != RESULT_OK:
            raise ValueError("Result not OK")
        return True

class SendDataPacket(UsbPacket):
    @classmethod
    def build(cls, offset, size, crc32c_val):
        arg2 = (offset >> 32) & 0xFFFFFFFF
        arg3 = offset & 0xFFFFFFFF
        packet = cls(MAGIC, arg2, arg3, size, crc32c_val)
        packet.generate_crc32c()
        return packet

    def get_offset(self):
        return (self.arg2 << 32) | self.arg3

    def get_size(self):
        return self.arg4

    def get_crc32c(self):
        return self.arg5

class Usb:
    def __init__(self):
        self.__out_ep = None
        self.__in_ep = None

    def wait_for_connect(self) -> None:
        print("waiting for switch")

        dev = None
        while (dev is None):
            dev = usb.core.find(idVendor=0x057E, idProduct=0x3000)
            if (dev is None):
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
            dev.reset()
            dev.set_configuration()
            cfg = dev.get_active_configuration()

        is_out_ep = lambda ep: usb.util.endpoint_direction(ep.bEndpointAddress) == usb.util.ENDPOINT_OUT
        is_in_ep = lambda ep: usb.util.endpoint_direction(ep.bEndpointAddress) == usb.util.ENDPOINT_IN
        self.__out_ep = usb.util.find_descriptor(cfg[(0,0)], custom_match=is_out_ep)
        self.__in_ep = usb.util.find_descriptor(cfg[(0,0)], custom_match=is_in_ep)
        assert self.__out_ep is not None
        assert self.__in_ep is not None

        print("iManufacturer: {} iProduct: {} iSerialNumber: {}".format(dev.manufacturer, dev.product, dev.serial_number))
        print("bcdUSB: {} bMaxPacketSize0: {}".format(hex(dev.bcdUSB), dev.bMaxPacketSize0))

    def read(self, size: int, timeout: int = 0) -> bytes:
        return self.__in_ep.read(size, timeout)

    def write(self, buf: bytes, timeout: int = 0) -> int:
        return self.__out_ep.write(data=buf, timeout=timeout)

    def get_send_header(self) -> tuple[int, int, int]:
        packet = SendPacket.unpack(self.read(PACKET_SIZE))
        packet.verify()
        return packet.get_cmd(), packet.arg3, packet.arg4

    def get_send_data_header(self) -> tuple[int, int, int]:
        packet = SendDataPacket.unpack(self.read(PACKET_SIZE))
        packet.verify()
        return packet.get_offset(), packet.get_size(), packet.get_crc32c()

    def send_result(self, result: int, arg3: int = 0, arg4: int = 0) -> None:
        send_data = ResultPacket.build(result, arg3, arg4).pack()
        self.write(send_data)
