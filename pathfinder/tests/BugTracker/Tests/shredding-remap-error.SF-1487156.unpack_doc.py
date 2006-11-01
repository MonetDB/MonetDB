import os
import string

TSTSRCDIR = os.environ['TSTSRCDIR']

CALL = "unzip shredding-remap-error.SF-1487156.unpack_doc.zip"

if os.name == "nt":
    os.system("call Mlog.bat '%s'" % CALL.replace('|','\\|'))
else:
    os.system("Mlog '%s'" % CALL.replace('|','\\|'))
os.system(CALL)
