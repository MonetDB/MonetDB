## Process this file with automake to produce Makefile.in

#AUTOMAKE_OPTIONS = no-dependencies

BUILT_SOURCES = 

CFLAGS += -DUNIX -DMX_CXX_SUFFIX=\"cc\"

EXTRA_DIST = warning.gif epstogif

#-version-info $(LT_VERSION)
bin_PROGRAMS = Mx prefixMxFile idxmx epsffit

Mx_LDADD = 

Mx_SOURCES = \
	Mx.c Index.c Def.c Form.c Tok.c Code.c Display.c Print.c Io.c Sys.c \
	disclaimer.c  Tex2Html.c    \
	Mx.h  MxFcnDef.h  

prefixMxFile_SOURCES = \
	prefixMxFile.c disclaimer.c disclaimer.h

idxmx_SOURCES = \
        idxmx.c 

epsffit_SOURCES = \
	epsffit.c 

