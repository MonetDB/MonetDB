#!/bin/sh

# Bug report #853867 says:
# "After running the script below for the 3rd time the server crashes in
#  vfprintf after an infinite recursive module load, TBL_loadmodule line
#  1582."
# Hence, we run the script 10 times, and see whether it works...

Mlog -x $MAPI_CLIENT < $1.mil
Mlog -x $MAPI_CLIENT < $1.mil
Mlog -x $MAPI_CLIENT < $1.mil
Mlog -x $MAPI_CLIENT < $1.mil
Mlog -x $MAPI_CLIENT < $1.mil
Mlog -x $MAPI_CLIENT < $1.mil
Mlog -x $MAPI_CLIENT < $1.mil
Mlog -x $MAPI_CLIENT < $1.mil
Mlog -x $MAPI_CLIENT < $1.mil
Mlog -x $MAPI_CLIENT < $1.mil

