
MONET_DIST     = `monet-config --source`
MONET_LIBDIR   = `monet-config --modpath`
MONET_INCDIR   = `monet-config --includedir`
MONET_INCLUDES = `monet-config --cflags`

MX      = Mx
MXFLAGS = -notouch

MEL         = mel
MELINCLUDES = $(MONET_INCLUDES)

INSTALL = install -c
LIBTOOL = libtool
M4 = m4

# CFLAGS derived from Monet's `--enable-optimize' configuration option
CC     = gcc
CFLAGS = -O6 -fomit-frame-pointer -finline-functions -falign-loops=4 -falign-jumps=4 -falign-functions=4 -ffast-math -fexpensive-optimizations -funroll-all-loops  -funroll-loops -frerun-cse-after-loop -frerun-loop-opt -Walls -DHAVE_CONFIG_H $(MONET_INCLUDES)
# CFLAGS = -Walls -DHAVE_CONFIG_H $(MONET_INCLUDES)

#CC     = icc
#CFLAGS = -O3 -Ob2 -xMKW -ip -prof_dir /home/guest/keulen/src/Monet/pathfinder/profiling -prof_file bla -prof_use -DHAVE_CONFIG_H $(MONET_INCLUDES)
#LDFLAGS = /opt/intel/compiler70/ia32/lib/libirc.a


# Extract Monet module definition, C source, or C header from Mx file
%.S: %.c
	$(CC) $(CFLAGS) -S -o $@ $<

%.m: %.mx
	$(MX) $(MXFLAGS) -x m $<

%.c: %.mx
	$(MX) $(MXFLAGS) -x c $<

%.h: %.mx
	$(MX) $(MXFLAGS) -x h $<

%.mil: %.m
	$(MEL) $(MELINCLUDES) -o $@ -mil $<

%.proto.h: %.m
	$(MEL) $(MELINCLUDES) -o $@ -proto $<

%.glue.c: %.m
	$(MEL) $(MELINCLUDES) -o $@ -glue $<

%.lo: %.c
	$(LIBTOOL) --mode=compile $(CC) $(CFLAGS) -c $<

%.mx: %.mx.m4 config.m4  
	$(M4) -P $< >$@

%.mil: %.mil.m4 config.m4  
	$(M4) -P $< >$@

all: lib_pathfinder.la pathfinder.mil

install: lib_pathfinder.la pathfinder.mil
	$(LIBTOOL) --mode=install $(INSTALL) lib_pathfinder.la \
			$(MONET_LIBDIR)/lib_pathfinder.la
	install -m 644 pathfinder.mil $(MONET_LIBDIR)/pathfinder.mil

pathfinder.c: pathfinder.h pathfinder.proto.h
pathfinder.mx: docmgmt.mil.m4 xmlprint.mil axis.mil.m4\
					staircasejoin.def staircasejoin.c \
					levelsteps.def levelsteps.c \
					estimation.def estimation.c \
					ctxpropmgmt.def ctxpropmgmt.h ctxpropmgmt.c ctxpropmgmt.mil \
					pair.def pair.h pair.c

lib_pathfinder.la: pathfinder.lo pathfinder.glue.lo
	$(LIBTOOL) --mode=link $(CC) $(LDFLAGS) -o $@ -rpath $(MONET_LIBDIR) $^


run: install
	./runMonet pathfinder

.PHONY: clean

clean:
	rm -f pathfinder.{m,c,glue.c,h,proto.h}
	rm -f lib_pathfinder.la
	rm -rf .libs
	rm -f *.o *.lo
	rm -f *~

