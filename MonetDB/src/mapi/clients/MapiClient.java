/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
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


import java.io.*;
import mapi.*;


public class MapiClient 
{
    public static void usage(){
	System.out.println("Usage: java MapiClient <host> <port> <user>" );
       	System.out.println("or   : java MapiClient <port> <user>" );
       	System.out.println("or   : java MapiClient <user>" );
       	System.out.println("or   : java MapiClient" );
	System.exit(1);
    }

    public static void main(String argv[]){
	String hostname = "localhost";
	int portnr = 50000;
	String user = "niels";
	if (argv.length > 3 ){
	   	usage();
	}
	if (argv.length == 1){
      		user = argv[0];
	} else if (argv.length == 2){
		portnr = Integer.parseInt(argv[0]);
      		user = argv[1];
	} else if (argv.length == 3){
		hostname = argv[0];
		portnr = Integer.parseInt(argv[1]);
      		user = argv[2];
	}

	try {
      		Mapi M = new Mapi( hostname, portnr, user );
		Reader r = new BufferedReader(new InputStreamReader(System.in));
		LineNumberReader input = new LineNumberReader(r);
		String s;
		System.out.print(">");
		while((s=input.readLine()) != null){
			if (s.equals("quit;")) break;
			System.out.println(M.query(s));
			System.out.print(">");
		}
	} catch (MapiException e){
                System.err.println( "MapiClient: "+e );
        } catch (IOException e){
                System.err.println( "MapiClient: "+e );
	}
   }
}
