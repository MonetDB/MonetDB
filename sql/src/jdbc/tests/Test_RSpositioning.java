import java.sql.*;

public class Test_RSpositioning {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		//nl.cwi.monetdb.jdbc.MonetConnection.setDebug(true);
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/database", "monetdb", "monetdb");
		Statement stmt = con.createStatement();
		DatabaseMetaData dbmd = con.getMetaData();

		// get a one rowed resultset
		ResultSet rs = stmt.executeQuery("SELECT 1");

		// >> true: we should be before the first result now
		System.out.println("true\t" + rs.isBeforeFirst());
		// >> false: we're not at the first result
		System.out.println("false\t" + rs.isFirst());
		// >> true: there is one result, so we can call next once
		System.out.println("true\t" + rs.next());
		// >> false: we're not before the first row anymore
		System.out.println("false\t" + rs.isBeforeFirst());
		// >> true: we're at the first result
		System.out.println("true\t" + rs.isFirst());
		// >> false: we're on the last row
		System.out.println("false\t" + rs.isAfterLast());
		// >> true: see above
		System.out.println("true\t" + rs.isLast());
		// >> false: there is one result, so this is it
		System.out.println("false\t" + rs.next());
		// >> true: yes, we're at the end
		System.out.println("true\t" + rs.isAfterLast());
		// >> false: no we're one over it
		System.out.println("false\t" + rs.isLast());
		// >> false: another try to move on should still fail
		System.out.println("false\t" + rs.next());
		// >> true: and we should stay positioned after the last
		System.out.println("true\t" + rs.isAfterLast());

		rs.close();

		// try the same with a 'virtual' result set
		rs = dbmd.getTables(null, null, "tables", null);

		// >> true: we should be before the first result now
		System.out.println("true\t" + rs.isBeforeFirst());
		// >> false: we're not at the first result
		System.out.println("false\t" + rs.isFirst());
		// >> true: there is one result, so we can call next once
		System.out.println("true\t" + rs.next());
		// >> false: we're not before the first row anymore
		System.out.println("false\t" + rs.isBeforeFirst());
		// >> true: we're at the first result
		System.out.println("true\t" + rs.isFirst());
		// >> false: we're on the last row
		System.out.println("false\t" + rs.isAfterLast());
		// >> true: see above
		System.out.println("true\t" + rs.isLast());
		// >> false: there is one result, so this is it
		System.out.println("false\t" + rs.next());
		// >> true: yes, we're at the end
		System.out.println("true\t" + rs.isAfterLast());
		// >> false: no we're one over it
		System.out.println("false\t" + rs.isLast());
		// >> false: another try to move on should still fail
		System.out.println("false\t" + rs.next());
		// >> true: and we should stay positioned after the last
		System.out.println("true\t" + rs.isAfterLast());

		rs.close();

		con.close();
	}
}
