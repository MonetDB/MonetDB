
################################################################

# Nothing much configurable below

# cl -? describes the options
CC = cl -GF -W3 -MD -nologo -Zi
# optimize use -Ox

# No general LDFLAGS needed
LDFLAGS = /link
INSTALL = copy
MKDIR = mkdir
ECHO = echo
CD = cd

TOPDIR = .
CFLAGS = -I. -I$(TOPDIR) $(LIBC_INCS) -DHAVE_CONFIG_H

CXXEXT = "cxx"
!INCLUDE rules.msc

all: all-msc all-recursive

all-recursive: src-recursive

..\makefile.msc: ..\src\utils\autogen\autogen.py \
	..\src\utils\autogen\codegen.py \
	..\src\utils\autogen\msc.py
	cd .. && $(PYTHON) src\utils\autogen\autogen.py

src-recursive: src ../makefile.msc 
	cd src && nmake /k /nologo -f Makefile

src:
	if not exist src $(MKDIR) src
	$(INSTALL) ..\src\Makefile.msc src\Makefile

all-msc: config.h unistd.h sys\socket.h

config.h: winconfig.h
	$(INSTALL) winconfig.h config.h

unistd.h: 
	$(ECHO) #ifndef UNISTD_H > unistd.h
	$(ECHO) #define UNISTD_H >> unistd.h
	$(ECHO) #include "io.h" >> unistd.h
	$(ECHO) #endif >> unistd.h

sys\socket.h: 
	if not exist sys $(MKDIR) sys
	$(ECHO) #ifndef SOCKET_H > sys\socket.h
	$(ECHO) #define SOCKET_H >> sys\socket.h
	$(ECHO) #include "winsock.h" >> sys\socket.h
	$(ECHO) #endif >> sys\socket.h

