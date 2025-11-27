import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
import crc32c

from usb_common import CMD_EXPORT, CMD_QUIT, RESULT_OK, RESULT_ERROR

class FakeUsb:
	def __init__(self, files=None):
		# files: list of tuples (filename: str, data: bytes)
		self.files = files or [("testfile.bin", b"testdata")]
		self._cmd_index = 0
		self._file_index = 0
		self._data_index = 0
		self.results = []
		self._reading_filename = True
		self._reading_data = False
		self._current_data = b""
		self._current_data_offset = 0
		self._current_data_sent = 0
		self._current_file = None
		self._send_data_header_calls = 0

	def wait_for_connect(self):
		pass

	def get_send_header(self):
		# Simulate command sequence: export for each file, then quit
		if self._cmd_index < len(self.files):
			filename, data = self.files[self._cmd_index]
			self._current_file = (filename, data)
			self._cmd_index += 1
			self._reading_filename = True
			self._reading_data = False
			self._current_data = data
			self._current_data_offset = 0
			self._current_data_sent = 0
			self._send_data_header_calls = 0
			return [CMD_EXPORT, len(filename.encode("utf-8")), 0]
		else:
			return [CMD_QUIT, 0, 0]

	def read(self, size):
		# Simulate reading file name or data
		if self._reading_filename:
			filename = self._current_file[0].encode("utf-8")
			self._reading_filename = False
			self._reading_data = True
			return filename[:size]
		elif self._reading_data:
			# Return file data for export
			data = self._current_data[self._current_data_sent:self._current_data_sent+size]
			self._current_data_sent += len(data)
			return data
		else:
			return b""

	def get_send_data_header(self):
		# Simulate sending data in one chunk, then finish
		if self._send_data_header_calls == 0:
			self._send_data_header_calls += 1
			data = self._current_data
			crc = crc32c.crc32c(data)
			return [0, len(data), crc]
		else:
			return [0, 0, 0]  # End of transfer

	def send_result(self, result):
		self.results.append(result)

# test case for usb_export.py
class TestUsbExport(unittest.TestCase):
	def setUp(self):
		self.root = "test_output"
		os.makedirs(self.root, exist_ok=True)
		# 100 files named test1.bin, test2.bin, ..., test10.bin, each with different sizes
		self.files = [
			(f"test{i+1}.bin", bytes([65 + i]) * (i * 100 + 1)) for i in range(100)
		]
		self.fake_usb = FakeUsb(files=self.files)

	def tearDown(self):
		# Clean up created files/folders
		for f in os.listdir(self.root):
			os.remove(os.path.join(self.root, f))
		os.rmdir(self.root)

	def test_export_multiple_files(self):
		from usb_export import get_file_name, create_file_folder, wait_for_input

		# Simulate the main loop for all files
		for filename, data in self.files:
			cmd, name_len, _ = self.fake_usb.get_send_header()
			self.assertEqual(cmd, CMD_EXPORT)

			file_name = get_file_name(self.fake_usb, name_len)
			self.assertEqual(file_name, filename)

			full_path = create_file_folder(self.root, file_name)
			self.fake_usb.send_result(RESULT_OK)

			wait_for_input(self.fake_usb, full_path)

			# Check file was created and contents match
			with open(full_path, "rb") as f:
				filedata = f.read()
			self.assertEqual(filedata, data)

		# After all files, should get CMD_QUIT
		cmd, _, _ = self.fake_usb.get_send_header()
		self.assertEqual(cmd, CMD_QUIT)
		self.assertIn(RESULT_OK, self.fake_usb.results)

if __name__ == "__main__":
	unittest.main()
