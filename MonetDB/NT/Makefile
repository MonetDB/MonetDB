
################################################################

# Nothing much configurable below

# cl -? describes the options
CC = cl -GF -W3 -D -nologo -Zi
# optimize use -Ox

# No general LDFLAGS needed
LDFLAGS = /link
INSTALL = copy
KDIR = mkdir
ECHO = echo
CD = cd
#PYTHON = "c:\program files\python\python.exe" 
PYTHON = "c:\python16\python.exe" 

TOPDIR = .
CFLAGS = -I. -I$(TOPDIR) $(LIBC_INCS) -DHAVE_CONFIG_H

CXXEXT = "cxx"

all: all-msc all-recursive

all-recursive: src-recursive

..\makefile.msc: ..\src\utils\autogen\autogen.py \
	..\src\utils\autogen\codegen.py \
	..\src\utils\autogen\msc.py
	cd .. && $(PYTHON) src\utils\autogen\autogen.py

src-recursive: src ../makefile.msc 
	cd src && nmake /k /nologo -f akefile

src:
	if not exist src $(KDIR) src
	$(INSTALL) ..\src\akefile.msc src\akefile

all-msc: config.h unistd.h sys\socket.h

config.h: winconfig.h
	$(INSTALL) winconfig.h config.h

unistd.h: 
	$(ECHO) #ifndef UNISTD_H > unistd.h
	$(ECHO) #define UNISTD_H >> unistd.h
	$(ECHO) #include "io.h" >> unistd.h
	$(ECHO) #endif >> unistd.h

sys\socket.h: 
	if not exist sys $(KDIR) sys
	$(ECHO) #ifndef SOCKET_H > sys\socket.h
	$(ECHO) #define SOCKET_H >> sys\socket.h
	$(ECHO) #include "winsock.h" >> sys\socket.h
	$(ECHO) #endif >> sys\socket.h

