import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
import tempfile
import shutil
import crc32c

from usb_common import RESULT_OK

# Simpler FakeUsb for file transfer
class FakeUsb:
	def __init__(self, filedata):
		self.filedata = filedata  # bytes
		self.results = []
		self.writes = []
		self._send_data_header_calls = 0
		self._current_data = filedata

	def wait_for_connect(self):
		pass

	def get_send_data_header(self):
		# Simulate sending the file in one chunk, then finish
		if self._send_data_header_calls == 0:
			self._send_data_header_calls += 1
			data = self._current_data
			crc = crc32c.crc32c(data)
			return [0, len(data), crc]
		else:
			return [0, 0, 0]

	def send_result(self, result, arg2=0, arg3=0):
		self.results.append((result, arg2, arg3))

	def write(self, data):
		self.writes.append(data)

class TestUsbInstall(unittest.TestCase):
	def setUp(self):
		import random
		self.tempdir = tempfile.mkdtemp()
		# 100 files named test1.nsp, test2.xci, test3.nsz, test4.xcz, ..., cycling extensions, each with random sizes (0-2048 bytes)
		extensions = ["nsp", "xci", "nsz", "xcz"]
		self.files = [
			(f"test{i+1}.{extensions[i % 4]}", os.urandom(random.randint(0, 2048))) for i in range(100)
		]
		self.filepaths = []

		for fname, data in self.files:
			fpath = os.path.join(self.tempdir, fname)
			with open(fpath, "wb") as f:
				f.write(data)
			self.filepaths.append(fpath)

	def tearDown(self):
		shutil.rmtree(self.tempdir)

	def test_multiple_file_install(self):
		from usb_install import add_file_to_install_list, paths, wait_for_input
		paths.clear()

		for fpath in self.filepaths:
			add_file_to_install_list(fpath)

		for idx, (fname, data) in enumerate(self.files):
			fake_usb = FakeUsb(data)
			wait_for_input(fake_usb, idx)

			# Check that the file on disk matches expected data
			with open(self.filepaths[idx], "rb") as f:
				filedata = f.read()
			self.assertEqual(filedata, data)
			found = False

			for result, arg2, arg3 in fake_usb.results:
				if result == RESULT_OK and arg2 == len(data):
					found = True
			self.assertTrue(found)

	def test_directory_install(self):
		from usb_install import add_file_to_install_list, paths, wait_for_input
		paths.clear()

		for fpath in self.filepaths:
			add_file_to_install_list(fpath)

		for idx, (fname, data) in enumerate(self.files):
			fake_usb = FakeUsb(data)
			wait_for_input(fake_usb, idx)

			# Check that the file on disk matches expected data
			with open(self.filepaths[idx], "rb") as f:
				filedata = f.read()

			self.assertEqual(filedata, data)
			found = False

			for result, arg2, arg3 in fake_usb.results:
				if result == RESULT_OK and arg2 == len(data):
					found = True
			self.assertTrue(found)

if __name__ == "__main__":
	unittest.main()
