
import java.io.*;
import Mapi.*;


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
