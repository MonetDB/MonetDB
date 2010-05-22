
import sys

# a ugly hack to support python 2 and 3 at the same time
(major, minor, micro, level, serial)  = sys.version_info
if (major == 3):
    from monetdb.mapi3 import *
else:
    from monetdb.mapi2 import *

