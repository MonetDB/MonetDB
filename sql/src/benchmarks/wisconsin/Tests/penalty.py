import os
import time

if  os.environ['OS'] == "SunOS":
	time.sleep(222)
else:
	time.sleep(66)

