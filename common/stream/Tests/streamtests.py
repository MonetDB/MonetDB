#!/usr/bin/env python3

import hashlib
import json
import os
import subprocess
import sys

BASE = os.environ.get('TSTSRCDIR', os.path.dirname(os.path.abspath(sys.argv[0])))
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

	def verify(self, text, text_mode):
		# DIRTY HACK
		# For the time being we completely ignore the line ending issue.
		# The reason is that reading a DOS file on Unix correctly yields
		# \r\n's. Because Unix does not do line ending translation.
		# However, the compression algorithms DO translate newlines
		# when reading in text mode, even on Unix.
		#
		# Until we figure out how we want to deal with that we pretend
		# the issue doesn't exist.
		text = text.replace(b'\r\n', b'\n')

		bom_found = text.startswith(BOM)

		# | HAS_BOM | TEXT_MODE | BOM_FOUND | RESULT                               |
		# |---------+-----------+-----------+--------------------------------------|
		# | False   | *         | False     | OK                                   |
		# | False   | *         | True      | Somehow, a BOM was inserted!         |
		# | True    | False     | False     | The BOM should not have been removed |
		# | True    | False     | True      | OK                                   |
		# | True    | True      | False     | OK                                   |
		# | True    | True      | True      | The BOM should have been removed     |
		if not self.has_bom:
			if bom_found:
				return "Somehow, a BOM was inserted!"
		if self.has_bom:
			if text_mode and bom_found:
				return "In text mode, the BOM should have been removed"
			if not text_mode and not bom_found:
				return "In binary mode, the BOM should not have been removed"

		if bom_found:
			text = text[len(BOM):]

		if len(text) != self.size_without_bom:
			return f"Size mismatch: found {len(text)} bytes, expected {self.size_without_bom}"

		digest = hashlib.md5(text).hexdigest()
		if digest != self.md5_without_bom:
			return "MD5 digest mismatch: generated output differs."

		start = text[:len(self.start_without_bom)]
		end = text[-len(self.end_without_bom):]
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

def test_read(opener, text_mode, doc):
	test = f"read {opener} {doc.filename}"

	print()

	cmd = [ 'streamcat', 'read', doc.path(), opener ]
	results = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	if results.returncode != 0 or results.stderr:
		print(f"Test {test}: streamcat returned with exit code {results.returncode}:\n{results.stderr or ''}")
		return False

	output = results.stdout or b""
	complaint = doc.verify(output, text_mode)

	if complaint:
		print(f"Test {test} failed: {complaint}")
		return False

	print(f"Test {test} OK")
	return True


def test_reads(doc):
	failures = 0

	# rstream does not strip BOM
	failures += not test_read('rstream', False, doc)

	# rastream does strip the BOM
	failures += not test_read('rastream', True, doc)

	return failures


def all_read_tests(filename_filter):
	failures = 0
	for d in get_docs():
		if not filename_filter(d.filename):
			continue

		# rstream does not strip BOM
		failures += test_reads(d)

	return failures


def main(kind, file):
	failures = 0
	if kind == "read":
		docs = [d for d in get_docs() if d.filename == file]
		print(f"Found {len(docs)} matches.")
		print()
		for d in docs:
			failures += test_reads(d)

	return failures == 0

if __name__ == "__main__":
	docname = "small.txt.gz"
	doc = [d for d in get_docs() if d.filename == docname][0]
	test_read('rastream', True, doc)