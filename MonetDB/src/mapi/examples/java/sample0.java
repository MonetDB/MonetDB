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
 * Portions created by CWI are Copyright (C) 1997-2004 CWI.
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

// A sample java program to interact with MonetDB
// Make sure you have the Mapi.java package in your path
import mapi.*;

class sample0
{
    public static void main(String args[]){
	final boolean sqltest=false;

	Mapi mapi= new Mapi();
	if( args.length != 1){
		System.out.println("usage:"+" <port>");
		System.exit(-1);
	}
	int port = (new Integer(args[0])).intValue();
	System.out.println("# Start test on localhost:"+args[0]);
	try{
		mapi.connect("localhost",port,"guest","anonymous","mil");
	} catch(MapiException e){
		System.out.println("Got exception from server");
		System.exit(-1);
	}
	if( mapi.gotError()) die(mapi);
	// mapi.trace(true);
	
	if(sqltest){
		if( mapi.query("create table emp(name varchar,age,int") != mapi.MOK)
			die(mapi);
		if( mapi.query("insert into emp values(\"John\",23)") != mapi.MOK)
			die(mapi);
		if( mapi.query("insert into emp values(\"Mary\",23)") != mapi.MOK)
			die(mapi);
		if( mapi.query("select * from emp") != mapi.MOK)
			die(mapi);
	} else {
		if( mapi.query("var emp:= new(str,int);") != mapi.MOK) die(mapi);
		if( mapi.query("emp.insert(\"John\",23);") != mapi.MOK) die(mapi);
		if( mapi.query("emp.insert(\"Mary\",23);") != mapi.MOK) die(mapi);
		if( mapi.query("print(emp);") != mapi.MOK)
			die(mapi);
	}
	while( mapi.fetchRow()>0){
		String nme= mapi.fetchField(0);
		String age= mapi.fetchField(1);
		System.out.println(nme+" is "+age);
	}
	mapi.disconnect();
  }

  public static void die(Mapi m){
	m.explain();
	System.exit(0);
  }
}
