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

// A sample java program to interact with MonetDB
// Make sure you have the Mapi.java package in your path
import mapi.*;

public class sample
{
    public static void main(String[] args) {
	
	// because we're not in an object constructor we cannot use
	// blank finals here
	boolean sqltest = false;
	boolean trace = false;
	int port = 0;
	String host = null;

	if (args.length < 2) {
		System.out.println("usage: <host> <port> [<trace> [<sqltest>]]");
		System.exit(-1);
	}

	switch (args.length) {
		case 4:
			sqltest = (Boolean.valueOf(args[3])).booleanValue();
		case 3:
			trace = (Boolean.valueOf(args[2])).booleanValue();
		case 2:
			// note: we do not catch the possible NumberFormatException
			port = (new Integer(args[1])).intValue();
		case 1:
			// this case cannot happen, but it is here for clarity
			host = args[0];
	}
	
	System.out.println("# Start test on " + host + ":" + port);
	Mapi mapi = new Mapi();
	try {
		mapi.connect(host, port, "guest", "anonymous", "mil");
	} catch (MapiException e) {
		System.out.println("Got exception from server: " + e.getMessage());
		System.exit(-1);
	}
	
	// Exceptions are nice, really... see? now you got to double check
	if (mapi.gotError()) die(mapi);
	
	mapi.trace(trace);

	// Exceptions are really nice, I told you before...
	if (sqltest) {
		if (mapi.query("create table emp(name varchar(20), age int);") != Mapi.MOK)
			die(mapi);
		if (mapi.query("insert into emp values(\"John\",23);") != Mapi.MOK)
			die(mapi);
		if (mapi.query("insert into emp values(\"Mary\",23);") != Mapi.MOK)
			die(mapi);
		if (mapi.query("select * from emp;") != Mapi.MOK)
			die(mapi);
	} else {
		if (mapi.query("var emp:= new(str,int);") != Mapi.MOK) die(mapi);
		if (mapi.query("emp.insert(\"John\",23);") != Mapi.MOK) die(mapi);
		if (mapi.query("emp.insert(\"Mary\",23);") != Mapi.MOK) die(mapi);
		if (mapi.query("print(emp);") != Mapi.MOK) die(mapi);
	}
	
	while (mapi.fetchRow() > 0) {
		String nme = mapi.fetchField(0);
		String age = mapi.fetchField(1);
		System.out.println(nme + " is " + age);
	}
	
	// possible exceptions? ;)
	mapi.disconnect();
  }

  public static void die(Mapi m){
	m.explain();
	System.exit(-1);
  }
}
