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
dnl [  --enable-monetdb           Enable monetdb support])

if test "$PHP_MONETDB" != "no"; then
  dnl Write more examples of tests here...

  dnl # --with-monetdb -> check with-path
  SEARCH_PATH="/usr/local /usr /opt"
  SEARCH_FOR="include/C/Mapi.h"
  if test -r $PHP_MONETDB/; then # path given as parameter
    MONETDB_DIR=$PHP_MONETDB
  else # search default path list
     AC_MSG_CHECKING([for monetdb/Mapi files in default path])
     for i in $SEARCH_PATH ; do
       if test -r $i/$SEARCH_FOR; then
         MONETDB_DIR=$i
         AC_MSG_RESULT(found in $i)
       fi
     done
  fi

  if test -z "$MONETDB_DIR"; then
    AC_MSG_RESULT([not found])
    AC_MSG_ERROR([Please reinstall the MonetDB distribution])
  fi

  # --with-monetdb -> add include path
  PHP_ADD_INCLUDE($MONETDB_DIR/include/MonetDB/C)

  # --with-monetdb -> check for lib and symbol presence
  LIBNAME=Mapi   # you may want to change this
  LIBSYMBOL=mapi_error_str # you most likely want to change this 

  PHP_CHECK_LIBRARY($LIBNAME,$LIBSYMBOL,
  [
    PHP_ADD_LIBRARY_WITH_PATH($LIBNAME, $MONETDB_DIR/lib, MONETDB_SHARED_LIBADD)
    AC_DEFINE(HAVE_MONETDBLIB,1,[ ])
  ],[
    echo "-L$MONETDB_DIR/lib"
    AC_MSG_ERROR([wrong monetdb/Mapi version or lib not found])
  ],[
    -L$MONETDB_DIR/lib
  ])
 
  PHP_SUBST(MONETDB_SHARED_LIBADD)

  PHP_NEW_EXTENSION(monetdb, php_monetdb.c, $ext_shared)
fi
