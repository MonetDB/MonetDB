import os
MSERVER =  os.environ['MSERVER']
TSTDB = os.environ['TSTDB']
TST = os.environ['TST']
os.system("%s --dbname=%s --set gdk_mem_pagebits=16 < %s.milM" % (MSERVER, TSTDB, TST))
