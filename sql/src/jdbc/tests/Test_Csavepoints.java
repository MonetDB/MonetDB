import java.sql.*;

public class Test_Csavepoints {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		//nl.cwi.monetdb.jdbc.MonetConnection.setDebug(true);
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/database", "monetdb", "monetdb");
		Statement stmt = con.createStatement();
		ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		// >> true: auto commit should be on by default
		System.out.println("true\t" + con.getAutoCommit());

		// savepoints require a non-autocommit connection
		try {
			System.out.print("savepoint...");
			con.setSavepoint();
			System.out.println("PASSED :(");
			System.out.println("ABORTING TEST!!!");
			con.close();
			System.exit(-1);
		} catch (SQLException e) {
			System.out.println("failed :)");
		}

		con.setAutoCommit(false);
		// >> true: auto commit should be on by default
		System.out.println("false\t" + con.getAutoCommit());

		try {
			System.out.print("savepoint...");
			Savepoint sp1 = con.setSavepoint();
			System.out.println("passed :)");

			stmt.executeUpdate("CREATE TABLE table_Test_Csavepoints ( id int, PRIMARY KEY (id) )");

			System.out.print("savepoint...");
			Savepoint sp2 = con.setSavepoint("empty table");
			System.out.println("passed :)");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			int i = 0;
			int items = 0;
			System.out.print("table " + items + " items");
			while (rs.next()) {
				System.out.print(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				System.out.println(" FAILED (" + i + ") :(");
				System.out.println("ABORTING TEST!!!");
				con.close();
				System.exit(-1);
			}
			System.out.println(" passed :)");

			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (1)");
			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (2)");
			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (3)");

			System.out.print("savepoint...");
			Savepoint sp3 = con.setSavepoint("three values");
			System.out.println("passed :)");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 3;
			System.out.print("table " + items + " items");
			while (rs.next()) {
				System.out.print(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				System.out.println(" FAILED (" + i + ") :(");
				System.out.println("ABORTING TEST!!!");
				con.close();
				System.exit(-1);
			}
			System.out.println(" passed :)");

			System.out.print("release...");
			con.releaseSavepoint(sp1);
			System.out.println("passed :)");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 3;
			System.out.print("table " + items + " items");
			while (rs.next()) {
				System.out.print(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				System.out.println(" FAILED (" + i + ") :(");
				System.out.println("ABORTING TEST!!!");
				con.close();
				System.exit(-1);
			}
			System.out.println(" passed :)");

			System.out.print("rollback...");
			con.rollback(sp2);
			System.out.println("passed :)");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 0;
			System.out.print("table " + items + " items");
			while (rs.next()) {
				System.out.print(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				System.out.println(" FAILED (" + i + ") :(");
				System.out.println("ABORTING TEST!!!");
				con.close();
				System.exit(-1);
			}
			System.out.println(" passed :)");

			con.rollback();
		} catch (SQLException e) {
			System.out.println("failed :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.close();
	}
}
