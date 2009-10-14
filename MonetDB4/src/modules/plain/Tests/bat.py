import os
MSERVER =  os.environ['MSERVER']
TSTDB = os.environ['TSTDB']
TST = os.environ['TST']
TSTTRGDIR = os.environ['TSTTRGDIR']
os.system("%s --dbname=%s --set gdk_mem_pagebits=16 < %s.milS" % (MSERVER, TSTDB, os.path.join(TSTTRGDIR,TST)))
