from Mapi import * 
import fileinput;

s = server( hostname(), portnr(), environ['USER'] + '\n')
fi = fileinput.FileInput()
print("#MapiClient (python) connected to %s:%d as %s" % (hostname(), portnr(), environ['USER']))
sys.stdout.write( s.prompt )
line= fi.readline()
while( line != "quit;\n" ):
	res = s.cmd( line )
	print(res);
	sys.stdout.write( s.prompt )
	line = fi.readline() 
s.disconnect()
