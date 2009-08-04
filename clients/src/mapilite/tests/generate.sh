#!/bin/sh

# this script will convert the structs in the header files to python code, so we
# can use them with ctypes.
#
# needs: 
#  * gccxml cvs checkout (0.9) - http://www.gccxml.org
#  * ctypeslib 0.9 branch - http://svn.python.org/projects/ctypes/branches/ctypeslib-gccxml-0.9/

h2xml.py mapi_structs.h -I. -I../../.. -I..  -I/opt/monetdb/current/include/MonetDB -I/opt/monetdb/current/include/MonetDB/common -o mapi_structs.xml
xml2py.py mapi_structs.xml > mapi_structs.py
