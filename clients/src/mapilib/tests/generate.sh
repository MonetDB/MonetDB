#!/bin/sh

# this script will convert the structs in the header files to python code, so we
# can use them with ctypes.
#
# needs: 
#  * gccxml cvs checkout (0.9) - http://www.gccxml.org
#    $ cvs -d :pserver:anoncvs@www.gccxml.org:/cvsroot/GCC_XML login
#    $ cvs -d :pserver:anoncvs@www.gccxml.org:/cvsroot/GCC_XML co gccxml
#    $ mkdir gccxml-build && cd gccxml-build
#    $ cmake ../gccxml
#    $ make
#    # make install
#
#  * ctypeslib 0.9 branch
#    $ svn co http://svn.python.org/projects/ctypes/branches/ctypeslib-gccxml-0.9/
#    $ cd ctypeslib-gccxml-0.9
#    # python setup.py install

h2xml.py mapi_structs.h -I. -I../../.. -I.. \
    -I/opt/monetdb/current/include/MonetDB \
    -I/opt/monetdb/current/include/MonetDB/common -o mapi_structs.xml || exit 1
xml2py.py mapi_structs.xml > mapi_structs.py || exit 1


# We need the MapiStruct struct definition, otherwise we could use this:
#h2xml.py ../Mapi.h -I. -I../../.. -I..  -I/opt/monetdb/current/include/MonetDB \
#    -I/opt/monetdb/current/include/MonetDB/common -o Mapi.xml || exit 1
#xml2py.py Mapi.xml > Mapi.py || exit 1
