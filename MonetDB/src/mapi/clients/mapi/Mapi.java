/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */


package mapi;

import java.awt.*;
import java.applet.Applet;
import java.applet.*;
import java.io.*;
import java.net.Socket;

/*
This module was designed to open up a Monet server through the
application interface for developing Java applications. The method
provided is mimicked after the Monet-C application interface where
appropriate.

Of course, a much better approach is to support a more direct
modeling of the Monet data model and operations in java classes, but
this is considered too much work for the time being.

Much like the Mapi interface, Mapi assumes a cautious user, who
understands and has experience with MIL programming. In particular,
syntax errors may easily lead to synchronization errors with the
server running in the background.

The organization of the web application is as follows. The code base
for the Java class is the subdirectory '~monet/java'. Furthermore, the
directory '~monet/dbfarm' contains hard links to the public_html sub
directory associated with each database directory.

A class Mapi represents the objects that describe a single session
with the Monet server. It is often the target object to interact.

Its constructor makes a socket connection with a Monet server. Note
that Java imposes some severe security rules. The most difficult one
is that java programs and its http server should reside on the same
machine.

The Mapi constructor prepares a connection with the Monet server using
a host name, port, username, and password (optional). They can be
passed as parameters from an applet.
*/


public class Mapi
{


/*
The status of a Mapi object is accessible from the status field. It should
be consulted before most commands.
*/
	private final static int READY 		= 0;
	private final static int RECV 		= 1;
	private final static int DISABLED 	= 2;
	private final static int MONETERROR 	= 3;
	private int status = READY;

	private String commenttext [] = new String[3];
	private String querytext = "";

	private Socket socket;
	private DataOutputStream toMonet;
	private BufferedReader fromMonet;

	public Mapi( String host, int port, String user ) throws MapiException
	{
		try{
			socket   = new Socket( host, port );
			fromMonet= new BufferedReader(
			 	new InputStreamReader(socket.getInputStream()));
			toMonet= new DataOutputStream(socket.getOutputStream());
		} catch(IOException e) {
		    throw new MapiException( "Failed to establish contact\n" );
		}
		toMonet(user+"\n");
		promptMonet();
		promptMonet();
   	}


/*
The user can create multiple Mapi objects to keep pre-canned queries around.
*/
	public Mapi(Mapi j){
		status = j.status;
		socket = j.socket;
		fromMonet = j.fromMonet;
		toMonet = j.toMonet;
	}



/*
Terminates the session with the Monet server either explicitly or implicitly.
*/
	public void disconnect() throws MapiException {
	   	if ( status != DISABLED ){
		   	toMonet("quit;\n");
			status = DISABLED;
			try {
				socket.close();
			} catch(IOException e){}
		}
	}
	public void finalize() throws MapiException {
		disconnect();
	}


/*
This is the lowest level of operation to send the Command to the server
for execution.  The command should be fully prepared by the application,
including necessary newlines to force the server to process the request.
The copy of the string is kept around in the Mapi structure for a while.
If the command is zero the last request is re-shipped.
*/
	public void dorequest(String command) throws MapiException {
		if( command != null) querytext = command;
		toMonet(querytext);
	}


/*
These are the lowest level operations to retrieve a single line from the
server. If something goes wrong the application may try to skip input to
the next synchronization point.
A reply of "bin:size" indicates the reply coming is a binary string
of length size. The user should call the getbinreply to get the binary
reply.
*/
	public String readrest(char c) throws MapiException {
		String s = String.valueOf(c);
		try {
			String s1 = fromMonet.readLine();
	        	s = s + s1 + "\n";
		} catch(IOException e){
	   	    throw new MapiException( "Communication broken" );
		}
		return s;
	}

	public String getreply() throws MapiException {
   		switch(status) {
		   case DISABLED:
			String m = "Attempt to read from disabled connection\n";
   			throw new MapiException(m);
		   case READY:
		   	return "";
   		}
	   	char c = getChar();
		switch(c){
		   case '\n': return getreply();
		   case '\1': promptMonet();
			      return "";
		   case '#': setComment(readrest(c));
			     return getreply();
		   case '!': // read away until end of error.
		   	status = MONETERROR;
			String s = readrest(c);
			String m = getreply();
			while (m.length() > 0){
		   		s = s+m;
				m = getreply();
			}
			throw new MapiException(s);
                   default:
			return readrest(c) + getreply();
	 	}
	}
	public char[] getbinreply(int size) throws MapiException {
		char[] res = new char[size];
		try {
			fromMonet.read(res,0,size);
		} catch(IOException e){
		   	throw new MapiException( "Communication broken" );
		}
		return res;
	}
	public void sync() throws MapiException {
		if( status == READY ) return;
		promptMonet();
	}
	public void setComment(String msg) {
		if ( msg == null) return;
		int i = 0;
		for( i =0; i<3; i++)
		   if ( commenttext[i] == null)
			commenttext[i] = msg;
	}

	public String [] getComment(){
		return commenttext;
	}
	private void clrComment(){
		commenttext = new String[3];
	}



/*
Unlike Mapi in Java we can assume better string construction mechanisms.
The method 'query' ships a single query and retrieves the first answer in
the reply buffer. It appends the necessary newline character to force
execution at the server.  Beware, a string with multiple newlines will
easily cause confusion in synchronization of the results.
[todo, count them and help in this process] The method 'get' sends a
single query that is to produce a single answer row. It immediately
eats away the prompt marker when no error has occurred.
*/
   public String query(String query) throws MapiException {
		if (query.equals("quit;")){
			disconnect();
		}
   		toMonet(query.trim()+"\n");
   		String s = getreply();
   		return s;
  }
   public String get(String query) throws MapiException {
   		return query(query);
  }



/*
Some Mapi primitives are not yet available.
*/
	public void timeout(int t) {
		System.out.println("Timeout not yet implemented\n");
	}



/*
The low-level operation toMonet attempts to send a string to the server.
*/
	private void toMonet(String msg) throws MapiException {
	   	switch (status){
		case DISABLED:
		        throw new MapiException( "Connection was disabled" );
		case RECV:
		case MONETERROR:
			sync();
			break;
		}
		try{
			toMonet.writeBytes(msg);
			toMonet.flush();
		} catch( IOException e){
			throw new MapiException("Can not write to Monet" );
		}
		status = RECV;
	}



/*
The interaction protocol with the monet database server is very
simple. Each command (query) ends with '\n'. The results sent back
will end with '\1' monet prompt '\1'.
This private member function finds these '\1' markers.
Note, unlike Mapi we do not yet support other prompt markers.
*/
   private void promptMonet() throws MapiException
   {
      char c = '\0';
      try {
	do {
         	c = (char)fromMonet.read();
	 } while(  c != '\1');
      } catch (IOException e) {
	 throw new MapiException( "Failure to read next byte from Monet" );
      }
      status = READY;
   }



/*
This private member function reads a character from the input stream
and returns it.
*/
   private final char getChar( ) throws MapiException
   {
      byte b = 0;
      try {
         b = (byte)fromMonet.read();
      } catch (IOException e) {
	 throw new MapiException( "Failure to read next byte from Monet" );
      }
      return (char)b;
   }
}

