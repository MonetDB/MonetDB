#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

AC_PREREQ(2.59)
AC_INIT([MonetDB Buildtools], 1.0, monet@cwi.nl)
AC_REVISION($Revision$)
AM_INIT_AUTOMAKE([MonetDB], [1.0])

AC_PROG_INSTALL
AC_PATH_PROGS([PYTHON], [python python2 python2.3 python2.4],
			  [AC_ERROR(No python could be found in your path)])

AC_ARG_ENABLE(mel,
	AC_HELP_STRING([--enable-mel], 
		[mel is only needed for MonetDB4]),
	[enable_mel=$enableval],[enable_mel="yes"])

case $enable_mel in
yes)	
	# HACK: because autoconf doesn't want to run configure with "added
	# arguments", we just call it ourselves here.  Pretty dirty, I know.

	(test -d mel || mkdir mel ; cd mel && eval "`cd .. && cd ${srcdir} && pwd`/mel/configure ${ac_configure_args} --with-mx=../Mx/Mx")
	;;
esac
AM_CONDITIONAL([ENABLE_MEL], [test "$enable_mel" = yes])


AC_CONFIG_FILES([Makefile autogen/Makefile conf/Makefile conf/Mbootstrap
                 conf/buildtools-config])
AM_CONFIG_HEADER([dummy.h])

AC_SUBST([PYTHON])

AC_CONFIG_SUBDIRS([Mx burg])

AC_OUTPUT
