import string 
from socket import *
from os import *

trace=		0
interactive=	0

class server:
	def cmd_intern( self, cmd ):
		try:
			self.socket.send(cmd)
			if (trace>0):
				print 'cmd ', cmd
		except IOError:
			print 'IO error '
	
	def result(self):
		result  = ''
		c = self.getchar()
		try:
			while ( c != '\1'):
				result = result + c
				c = self.getchar()
			self.getprompt()
			if (trace>0):
				print result
		except EOFError:
			print 'end of file'
		except error:
			print 'end of file'
			sys.exit(1)
		return result
	
	def getchar(self):
		try:
			c = self.socket.recv(1)
			if (trace>1):
				print c
			return c
		except IOError:
			print 'IO error '
		except error:
			print 'end of file'
			sys.exit(1)
		return ''

	def getprompt(self):
		self.prompt = ''
		c = self.getchar()
		try:
			while ( c != '\1'):
				self.prompt = self.prompt + c
				c = self.getchar()
		except EOFError:
			print 'end of file'
		if (interactive==1):
			print self.prompt
	
	def __init__ ( self, server, port, user ):
		try:
			self.socket = socket(AF_INET, SOCK_STREAM)
			self.socket.connect(server, port)
			self.prompt = ''
		except IOError:
			print 'server refuses access'
	
		self.cmd_intern(user)
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

def hostname():
	p = environ['MAPIPORT']
	if (p != ''):
		p = string.splitfields(p,':')[0]
	else:
		p = 'localhost'
	return p

def portnr():
	p = environ['MAPIPORT']
	if (p != ''):
		p = string.splitfields(p,':')[1]
	else:
		p = 50000
	return p


if __name__ == '__main__':
	import fileinput;

	s = server( 'localhost', 50000, 'niels\n')
	fi = fileinput.FileInput()
	sys.stdout.write( s.prompt )
	line= fi.readline()
	while( line != "quit;\n" ):
		res = s.cmd( line )
		print(res);
		sys.stdout.write( s.prompt )
		line = fi.readline() 
	s.disconnect()
