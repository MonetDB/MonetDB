import java.io.*;

public class Database {
	public static void main(String [] args) {
		PrintWriter pw = null;
		int letter,lengte,getal;
		String woord;
		try {
			pw = new PrintWriter(new FileOutputStream("mydb.sql"),true);
			pw.print("CREATE TABLE benchmark ( id int  ");
		for (int i = 0; i<149; i+=2) {
			pw.print(", atribute"+i+" int, atribute"+(i+1)+" varchar(20) ");
		}
		pw.print(", PRIMARY KEY(id));\n\n");
		for (int m = 0; m < 5000; m++) {
			pw.print("INSERT INTO benchmark VALUES ("+m+"");
			for (int j = 0; j<149; j+=2) {
				getal = (int)Math.round(Math.random()*Integer.MAX_VALUE);
				lengte = (int)Math.round(Math.random()*19)+1;
				woord = "";
				for (int k = 1; k <= lengte; k++) {
					letter = (int)Math.round(Math.random()*25)+65;
					woord = woord+(char)letter;
				}
				pw.print(","+getal+",'"+woord+"'");

			}
		pw.print(");\n\n");
		}
		pw.print("COMMIT;\n\n");
		}

		catch (IOException ex) {
			System.out.println(ex.getMessage());
		}
		finally {
			if (pw != null)
				pw.close();
			}
	}
}
