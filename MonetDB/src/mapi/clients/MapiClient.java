
import java.io.*;
import Mapi.*;


public class MapiClient 
{
    public static void usage(){
       	System.out.println("Usage: java MapiClient $MAPIPORT $USER" );
	System.exit(1);
    }

    public static void main(String argv[]){
	String hostname = "localhost";
	int portnr = 50000;
	String user = "niels";
	if (argv.length > 2 ){
	   	usage();
	}
	if (argv.length >= 1){
      		hostname = Mapi.hostname(argv[0]);
		portnr = Mapi.portnr(argv[0]);
	}
	if (argv.length == 2){
      		user = argv[1];
	}
	try {
      		Mapi M = new Mapi( hostname, portnr, user );
		Reader r = new BufferedReader(new InputStreamReader(System.in));
		LineNumberReader input = new LineNumberReader(r);
		String s;
		System.out.print(">");
		while((s=input.readLine()) != null){
			if (s == "quit;") break;
			System.out.println( M.query(s + "\n") );
			System.out.print(">");
		}
	} catch (MapiException e){
                System.err.println( "MapiClient: "+e );
        } catch (IOException e){
                System.err.println( "MapiClient: "+e );
	}
   }
}
