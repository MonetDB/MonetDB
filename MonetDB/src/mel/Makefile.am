## Process this file with automake to produce Makefile.in

#AUTOMAKE_OPTIONS = no-dependencies

BUILT_SOURCES = \
	mel_tab.yy mel_yy.ll \
	FileInfo.cc acc.cc accops.cc any_arg.cc atom.cc atom_arg.cc atomops.cc \
	bat_arg.cc builtin.cc command.cc debug.cc depend.cc dependency.cc \
	drop.cc epilogue.cc glue.cc html.cc iterator.cc link.cc load.cc mel.cc \
	module.cc object.cc operator.cc ops.cc prelude.cc \
	proto.cc symbol.cc symtable.cc type_arg.cc use.cc var_arg.cc \
	FileInfo.h ListIterator.h acc.h accops.h any_arg.h atom.h atom_arg.h \
	atomops.h bat_arg.h builtin.h command.h debug.h depend.h dependency.h \
	drop.h epilogue.h glue.h html.h iterator.h language.h link.h list.h \
	load.h mel.h module.h object.h operator.h ops.h prelude.h \
	proto.h symbol.h symtable.h type_arg.h use.h var_arg.h 

INCLUDES = 

# interface:implementation:age
LT_VERSION=0:1:0

EXTRA_DIST = \
	FileInfo.mx      atomops.mx     drop.mx      list.mx      prelude.mx \
	ListIterator.mx  bat_arg.mx     epilogue.mx  load.mx      proto.mx \
	acc.mx           builtin.mx     glue.mx      mel.mx       symbol.mx \
	accops.mx        command.mx     html.mx      module.mx    symtable.mx \
	any_arg.mx       debug.mx       iterator.mx  object.mx    type_arg.mx \
	atom.mx          depend.mx      language.mx  operator.mx  use.mx \
	atom_arg.mx      dependency.mx  link.mx      ops.mx       var_arg.mx

#-version-info $(LT_VERSION)
bin_PROGRAMS = mel

YFLAGS = -d

mel_SOURCES = $(BUILT_SOURCES) mel_tab.h 

include $(top_srcdir)/rules.mk
