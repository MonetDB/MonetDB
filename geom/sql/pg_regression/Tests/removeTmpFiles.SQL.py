import os

filename=os.path.join(os.getenv("TSTSRCDIR"), "tmpfile_1")
if os.path.isfile(filename):
    os.remove(filename);

filename=os.path.join(os.getenv("TSTSRCDIR"), "tmpfile_2")
if os.path.isfile(filename):
    os.remove(filename);

