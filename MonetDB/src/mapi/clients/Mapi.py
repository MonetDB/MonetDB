# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at 
# http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the Monet Database System.
# 
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2002 CWI.  
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

import string
from socket import *
from os import *

trace=          0
interactive=    0

class server:
    def cmd_intern( self, cmd ):
        try:
            self.socket.send(cmd)
            if (trace>0):
                print 'cmd ', cmd
        except IOError:
            print 'IO error '

    def result(self):
        result = self.getstring()
        self.getprompt()
        if (trace>0):
            print result
        return result

    def getstring(self):
        try:
            idx = string.find( self.buffer, "\1" )
            if (trace>1):
                self.buffer;
            str = ""
            while (idx < 0):
                if (trace>1):
                    print self.buffer
                str = str + self.buffer
                self.buffer = self.socket.recv(8096)
                idx = string.find( self.buffer, "\1" )

            str = str + self.buffer[0:idx]
            self.buffer = self.buffer[idx+1:]
            if (trace>1):
                print str
            return str
        except IOError:
            print 'IO error '
        except error:
            print 'end of file'
            sys.exit(1)
        return ''

    def getprompt(self):
        self.prompt = self.getstring()
        if (interactive==1):
            print self.prompt

    def __init__ ( self, server, port, user ):
        try:
            self.socket = socket(AF_INET, SOCK_STREAM)
            self.socket.connect((server, port))
            self.prompt = ''
            self.buffer = ''
        except IOError:
            print 'server refuses access'

        self.cmd_intern(user+'\n')
        self.result()
        if (trace>0):
            print 'connected ', self.socket

    def disconnect( self ):
        self.result = self.cmd_intern( 'quit;\n' )
        self.socket.close()
        self.socket = 0;

    def cmd( self, cmd ):
        self.cmd_intern(cmd)
        return self.result()


if __name__ == '__main__':
    import fileinput;

    s = server( "localhost" , 50000, environ['USER'])
    fi = fileinput.FileInput()
    sys.stdout.write( s.prompt )
    line= fi.readline()
    while( line != "quit;\n" ):
        res = s.cmd( line )
        print(res);
        sys.stdout.write( s.prompt )
        line = fi.readline()
    s.disconnect()
