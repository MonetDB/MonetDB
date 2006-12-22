/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2006 CWI.
 * All Rights Reserved.
 */

import java.io.*;
import mapi.*;

public class MapiClient 
{
    public static void usage(){
	System.out.println("Usage: java MapiClient [<host> [<port> [<user> [<password> [<language> [--utf8]]]]]]" );
	System.exit(1);
    }

    public static void main(String argv[]){
	String hostname = "localhost";
	int portnr = 50000;
	String user = "guest";
	String password = "anonymous";
	String lang = "mil";
	boolean useUTF8 = false;

	switch (argv.length) {
		case 6:
			useUTF8 = true;
		case 5:
			lang = argv[4];
		case 4:
			password = argv[3];
		case 3:
			user = argv[2];
		case 2:
			portnr = Integer.parseInt(argv[1]);
		case 1:
			hostname = argv[0];
		break;
		default:
			usage();
		break;
	}

	try {
		Mapi M = new Mapi( hostname, portnr, user, password, lang );
		Reader r = new BufferedReader(
			useUTF8 ? 
				new InputStreamReader(System.in, "UTF-8") :
				new InputStreamReader(System.in)
		);
		LineNumberReader input = new LineNumberReader(r);
		Writer out = 
			useUTF8 ?
				new OutputStreamWriter(System.out, "UTF-8") :
				new OutputStreamWriter(System.out);
		String s;
		System.out.print(M.getPrompt());
		while((s=input.readLine()) != null){
			if (s.equals("quit;")) break;
			M.quickQuery(s,out);
			System.out.print(M.getPrompt());
		}
	} catch (MapiException e){
                System.err.println( "MapiClient: "+e );
        } catch (IOException e){
                System.err.println( "MapiClient: "+e );
	}
   }
}
