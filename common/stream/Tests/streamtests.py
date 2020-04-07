#!/usr/bin/env python3

import hashlib
import json
import os
import subprocess
import sys

BASE = os.environ.get('TSTSRCDIR', os.getcwd())
DATA = os.path.join(BASE, 'data')

BOM = b'\xEF\xBB\xBF'


class Doc:
	__slots__ = ['filename', 'has_bom', 'size_without_bom', 'md5',
		'md5_without_bom', 'start_without_bom', 'end_without_bom']

	def __init__(self, datadict):
		self.filename = datadict['filename']
		self.has_bom = datadict['has_bom']
		self.size_without_bom = datadict['size_without_bom']
		self.md5 = datadict['md5']
		self.md5_without_bom = datadict['md5_without_bom']
		self.start_without_bom = bytes(datadict['start_without_bom'], 'utf-8')
		self.end_without_bom = bytes(datadict['end_without_bom'], 'utf-8')

	def verify(self, text, expect_bom_stripped):
		bom_found = text.startswith(BOM)

		# | HAS_BOM | EXPECT_BOM_REMOVED | BOM_FOUND | RESULT                               |
		# |---------+--------------------+-----------+--------------------------------------|
		# | False   | *                  | False     | OK                                   |
		# | False   | *                  | True      | Somehow, a BOM was inserted!         |
		# | True    | False              | False     | The BOM should not have been removed |
		# | True    | False              | True      | OK                                   |
		# | True    | True               | False     | OK                                   |
		# | True    | True               | True      | The BOM should have been removed     |
		if not self.has_bom:
			if bom_found:
				return "Somehow, a BOM was inserted!"
		if self.has_bom:
			if expect_bom_stripped and bom_found:
				return "The BOM should have been removed"
			if not expect_bom_stripped and not bom_found:
				return "The BOM should not have been removed"

		if bom_found:
			text = text[len(BOM):]

		if len(text) != self.size_without_bom:
			return f"Size mismatch: found {len(text)} bytes, expected {self.size_without_bom}"

		digest = hashlib.md5(text).hexdigest()
		if digest != self.md5_without_bom:
			return "MD5 digest mismatch: generated output differs."

		n = 8
		start = text[:n]
		end = text[-n:]
		if start != self.start_without_bom:
			return f"Expected text to start with {repr(self.start_without_bom)}, found {repr(start)}"
		if end != self.end_without_bom:
			return f"Expected text to end with {repr(self.end_without_bom)}, found {repr(end)}"

		return None

	def path(self):
		return os.path.join(DATA, self.filename)


def get_docs():
	raw = json.load(open(os.path.join(DATA, 'testcases.json')))
	return [Doc(r) for r in raw]

def test_read(opener, strips_bom, doc):
	test = f"read {opener} {doc.filename}"

	print()

	cmd = [ 'streamcat', 'read', doc.path(), opener ]
	results = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	if results.returncode != 0 or results.stderr:
		print(f"Test {test}: streamcat returned with exit code {results.returncode}:\n{results.stderr or ''}")
		return False

	output = results.stdout or b""
	complaint = doc.verify(output, strips_bom)

	if complaint:
		print(f"Test {test} failed: {complaint}")
		return False

	print(f"Test {test} OK")
	return True


def all_read_tests(filename_filter):
	ok = True
	for d in get_docs():
		if not filename_filter(d.filename):
			continue

		# rstream does not strip BOM
		ok = ok & test_read('rstream', False, d)

		# rastream does strip the BOM
		ok = ok & test_read('rastream', True, d)

	return ok

# import os
# import subprocess
# import sys

# # Used by InputFile.path()

# class InputFile:
# 	__slots__ = ['filename', 'has_bom', 'size', 'md5sum', 'first100bytes', 'last100bytes']

# 	def __init__(self, filename, has_bom, size, md5sum, first100bytes, last100bytes):
# 		self.filename = filename
# 		self.has_bom = has_bom
# 		self.size = size
# 		self.md5sum = md5sum
# 		self.first100bytes = first100bytes
# 		self.last100bytes = last100bytes

# 	def path(self, compression):
# 		extension = '.' + compression if compression else ''
# 		return os.path.join(BASE, 'data', self.filename + extension)

# 	def verify(self, result_bytes, ):
# 		pass


# def main():
# 	pass


# if __name__ == "__main__":
#     sys.exit(main() or 0)
