import java.sql.*;

public class Test_Cautocommit {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		//nl.cwi.monetdb.jdbc.MonetConnection.setDebug(true);
		Connection con1 = DriverManager.getConnection("jdbc:monetdb://localhost/database", "monetdb", "monetdb");
		Connection con2 = DriverManager.getConnection("jdbc:monetdb://localhost/database", "monetdb", "monetdb");
		Statement stmt1 = con1.createStatement();
		Statement stmt2 = con2.createStatement();
		ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		// >> true: auto commit should be on by default
		System.out.println("true\t" + con1.getAutoCommit());
		System.out.println("true\t" + con2.getAutoCommit());

		// test commit by checking if a change is visible in another connection
		try {
			System.out.print("create...");
			stmt1.executeUpdate("CREATE TABLE table_Test_Cautocommit ( id int )");
			System.out.println("passed :)");
			System.out.print("select...");
			rs = stmt2.executeQuery("SELECT * FROM table_Test_Cautocommit");
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			con1.close();
			con2.close();
			System.exit(-1);
		}

		// turn off auto commit
		con1.setAutoCommit(false);
		con2.setAutoCommit(false);

		// >> false: we just disabled it
		System.out.println("false\t" + con1.getAutoCommit());
		System.out.println("false\t" + con2.getAutoCommit());

		// a change would not be visible now
		try {
			System.out.print("drop...");
			stmt2.executeUpdate("DROP TABLE table_Test_Cautocommit");
			System.out.println("passed :)");
			System.out.print("select...");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Cautocommit");
			System.out.println("passed :)");
			System.out.print("commit...");
			con2.commit();
			System.out.println("passed :)");
			System.out.print("select...");
			try {
				rs = stmt1.executeQuery("SELECT * FROM table_Test_Cautocommit");
				System.out.println("FAILED :(");
			} catch (SQLException e) {
				System.out.println("passed :)");
			}
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED :(");
			System.out.println("ABORTING TEST!!!");
		}

		if (rs != null) rs.close();

		con1.close();
		con2.close();
	}
}
