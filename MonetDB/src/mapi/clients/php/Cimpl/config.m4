dnl The contents of this file are subject to the MonetDB Public
dnl License Version 1.0 (the "License"); you may not use this file
dnl except in compliance with the License. You may obtain a copy of
dnl the License at
dnl http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
dnl
dnl Software distributed under the License is distributed on an "AS
dnl IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
dnl implied. See the License for the specific language governing
dnl rights and limitations under the License.
dnl
dnl The Original Code is the Monet Database System.
dnl
dnl The Initial Developer of the Original Code is CWI.
dnl Portions created by CWI are Copyright (C) 1997-2004 CWI.
dnl All Rights Reserved.

dnl $Id$
dnl config.m4 for extension monetdb

dnl Comments in this file start with the string 'dnl'.
dnl Remove where necessary. This file will not work
dnl without editing.

dnl If your extension references something external, use with:

PHP_ARG_WITH(monetdb, for monetdb support,
dnl Make sure that the comment is aligned:
[  --with-monetdb[=DIR]          Include monetdb support])

dnl Otherwise use enable:

dnl PHP_ARG_ENABLE(monetdb, whether to enable monetdb support,
dnl Make sure that the comment is aligned:
dnl [  --enable-monetdb=DIR           Enable monetdb support])

if test "$PHP_MONETDB" != "no"; then
  dnl Write more examples of tests here...

  dnl # --with-monetdb -> check with-path
  SEARCH_PATH="/usr/local /usr /opt"
  SEARCH_FOR="include/MonetDB/C/Mapi.h"
  if test -r $PHP_MONETDB/; then # path given as parameter
    MONETDB_PREFIX=$PHP_MONETDB
  else # search default path list
     AC_MSG_CHECKING([for monetdb/Mapi files in default path])
     for i in $SEARCH_PATH ; do
       if test -r $i/$SEARCH_FOR; then
         MONETDB_PREFIX=$i
         AC_MSG_RESULT(found in $i)
       fi
     done
  fi

  if test -z "$MONETDB_PREFIX"; then
    AC_MSG_RESULT([not found])
    AC_MSG_ERROR([Please reinstall the MonetDB distribution])
  fi

  # --with-monetdb -> add include path
  PHP_ADD_INCLUDE($MONETDB_PREFIX/include/MonetDB/C)

  # --with-monetdb -> check for lib and symbol presence
  LIBNAME=Mapi   # you may want to change this
  LIBSYMBOL=mapi_error_str # you most likely want to change this 

  PHP_CHECK_LIBRARY($LIBNAME,$LIBSYMBOL,
  [
    PHP_ADD_LIBRARY_WITH_PATH($LIBNAME, $MONETDB_PREFIX/lib, MONETDB_SHARED_LIBADD)
    AC_DEFINE(HAVE_MONETDBLIB,1,[ ])
  ],[
    echo "-L$MONETDB_PREFIX/lib"
    AC_MSG_ERROR([wrong monetdb/Mapi version or lib not found])
  ],[
    -L$MONETDB_PREFIX/lib
  ])
 
  PHP_SUBST(MONETDB_SHARED_LIBADD)

  PHP_NEW_EXTENSION(monetdb, php_monetdb.c, $ext_shared)
fi
