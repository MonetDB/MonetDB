#!/usr/bin/env python
#
# autogen scans the makefile.ag 
# and generates both the makefile.am and makefile.msc
#

import fileinput
import string
import re
import regsub
import am 
import msc 
from codegen import * 
import tokenize
from var import *
import os
import sys

def isName(t):
	return (t != "{" and t != "}" and t != "=")

class parser:
	def __init__ (self):
		self.curvar = groupvar("top")
		self.top = self.curvar
		self.cnt = 0
		self.state = "defs"
		self.stack = []
		self.curvar.add("TARGETS", {})

	#def parse(self, type, token, (srow, scol), (erow,ecol), line):
	def parse(self, token, srow, line):
		# order is important
		if (self.state == "defs" and token == "\n"):
			return
		if (self.state == "\\" and token == "\n"):
			self.state = "="
		elif (self.state == "defs" and isName(token)):
			if (self.curvar != None):
				self.stack.append(self.curvar)
			self.curvar = var(token)
			self.state = "var"
		elif (self.state == "=" and token == "\\"):
			self.state = "\\"
		elif (self.state == "var" and token == "="):
			self.state = "="
		elif (self.state == "=" and token == "{"):
			self.curvar = groupvar(self.curvar._name)
			self.state = "defs"
			self.cnt = self.cnt+1
		elif ((self.state == "defs" and token == "}") or (self.state == "=" and token == "\n") or (self.state == "var" and token == "\n")):
			last = len(self.stack)-1
			v = self.stack[last]
			del self.stack[last]
			v.add(self.curvar._name,self.curvar._values)
			self.curvar = v
			self.cnt = self.cnt - 1
			self.state = "defs"
		elif (self.state == "=" and isName(token)):
			if token == '""':
				token = ""
			if (self.top._values.has_key(token)):
				for i in self.top.value(token):
					self.curvar.append(i)
			else:
				self.curvar.append(token)
		elif (self.state == "var" and token != "="):
			print("Missing = " + token, srow );
		else:
			print("error", token, self.state)
		

def read_makefile(p,cwd):
  lineno = 0
  for line in fileinput.input(cwd + os.sep + 'Makefile.ag'):
    if (line[0] != "#"):
      for token in string.split(line):
        p.parse(token,lineno,line)
      p.parse("\n", lineno,line)
    lineno = lineno + 1

if len(sys.argv) > 1:
  topdir = sys.argv[1]
else:
  topdir = os.getcwd()

def main(cwd,topdir):
  p = parser()
  read_makefile(p,cwd)
  codegen(p.curvar,cwd,topdir)
  am.output(p.curvar,cwd,topdir)
  msc.output(p.curvar,cwd,topdir)
  if ('SUBDIRS' in p.curvar.keys()):
    for dir in p.curvar.value('SUBDIRS'):
	  d = cwd+os.sep+dir
	  if (os.path.exists(d)):
		  print(d)
		  main(d,topdir)
		  #cmd = "cd " + dir + "; " + sys.argv[0] + " " + topdir
		  #os.system (cmd)

main(topdir,topdir)
