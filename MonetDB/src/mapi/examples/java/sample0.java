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
	mapi.trace(true);
	
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
		if( mapi.query("emp:= new(str,int);") != mapi.MOK) die(mapi);
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
