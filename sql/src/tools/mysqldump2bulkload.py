#!/usr/bin/env python

import re
import sys
import string
import fileinput

CREATE = 1
INSERT = 2

write = sys.stdout.write

def copy_line( line, write = write ):
	m = re.search('(INSERT INTO ([A-Za-z0-9_]*) .*VALUES \()', line) 
	if (m != None):
		write('SELECT \'' + line[m.start(2):m.end(2)] +'\';\n')
		write('COPY INTO ' + line[m.start(2):m.end(2)] + ' FROM STDIN USING DELIMITERS \',\', \'\\n\';\n')
		return line[0:m.end(1)]
	return ""

def create_line( line, write = write ):
	status = CREATE
 	if re.search('\).*;', line) != None:
		write(');\n')
		return 0
	m = re.search('auto_increment', line) 
	if (m != None):
		line = line[0:m.start(0)] + line[m.end(0):]
	m = re.search('unsigned', line) 
	if (m != None):
		line = line[0:m.start(0)] + line[m.end(0):]
	m = re.search('KEY[\t ]+([A-Za-z0-9_]+)[\t ]+(\(.*)', line) 
	if (m != None):
		write("  CONSTRAINT " + line[m.start(1) : m.end(1)] + " UNIQUE " + line[m.start(2) : m.end(2)] + "\n")
		return status
	write(line)
	return status

def main(write = write):
	status = 0
	base = ""
	l = 0
	for line in fileinput.input():
		if status == INSERT:
			if (line[0:l] == base):
				print line[l:line.rfind(')')]
			else:
				print # empty line to end copy into
				print "COMMIT;"
				status = 0;

		if status != INSERT and \
			not (line[0] == '#') and not (line[0:1] == '--'):
			if status == CREATE:
				status = create_line(line)
			elif re.search('DROP TABLE', line) != None:
				write('--' + line)
			elif re.search('CREATE TABLE', line) != None:
				status = CREATE
				write(line)
			elif re.search('INSERT INTO', line) != None:
				base = copy_line(line)
				l = len(base)
				status = INSERT
				print line[l:line.rfind(')')]
			else:
				write(line)

main()
