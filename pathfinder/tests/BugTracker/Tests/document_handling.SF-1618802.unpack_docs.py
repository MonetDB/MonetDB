import os
import string

TSTSRCDIR = os.environ['TSTSRCDIR']

CALL = "unzip document_handling.SF-1618802.unpack_docs.zip"

if os.name == "nt":
    os.system("call Mlog.bat '%s'" % CALL.replace('|','\\|'))
else:
    os.system("Mlog '%s'" % CALL.replace('|','\\|'))
os.system(CALL)
