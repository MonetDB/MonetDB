/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_Rpositioning {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		DatabaseMetaData dbmd = con.getMetaData();

		// get a one rowed resultset
		ResultSet rs = stmt.executeQuery("SELECT 1");

		// >> true: we should be before the first result now
		System.out.println("1. true\t" + rs.isBeforeFirst());
		// >> false: we're not at the first result
		System.out.println("2. false\t" + rs.isFirst());
		// >> true: there is one result, so we can call next once
		System.out.println("3. true\t" + rs.next());
		// >> false: we're not before the first row anymore
		System.out.println("4. false\t" + rs.isBeforeFirst());
		// >> true: we're at the first result
		System.out.println("5. true\t" + rs.isFirst());
		// >> false: we're on the last row
		System.out.println("6. false\t" + rs.isAfterLast());
		// >> true: see above
		System.out.println("7. true\t" + rs.isLast());
		// >> false: there is one result, so this is it
		System.out.println("8. false\t" + rs.next());
		// >> true: yes, we're at the end
		System.out.println("9. true\t" + rs.isAfterLast());
		// >> false: no we're one over it
		System.out.println("10. false\t" + rs.isLast());
		// >> false: another try to move on should still fail
		System.out.println("11. false\t" + rs.next());
		// >> true: and we should stay positioned after the last
		System.out.println("12.true\t" + rs.isAfterLast());

		rs.close();

		// try the same with a 'virtual' result set
		rs = dbmd.getTableTypes();

		// >> true: we should be before the first result now
		System.out.println("1. true\t" + rs.isBeforeFirst());
		// >> false: we're not at the first result
		System.out.println("2. false\t" + rs.isFirst());
		// >> true: there is one result, so we can call next once
		System.out.println("3. true\t" + rs.next());
		// >> false: we're not before the first row anymore
		System.out.println("4. false\t" + rs.isBeforeFirst());
		// >> true: we're at the first result
		System.out.println("5. true\t" + rs.isFirst());
		// move to last row
		rs.last();
		// >> false: we're on the last row
		System.out.println("6. false\t" + rs.isAfterLast());
		// >> true: see above
		System.out.println("7. true\t" + rs.isLast());
		// >> false: there is one result, so this is it
		System.out.println("8. false\t" + rs.next());
		// >> true: yes, we're at the end
		System.out.println("9. true\t" + rs.isAfterLast());
		// >> false: no we're one over it
		System.out.println("10. false\t" + rs.isLast());
		// >> false: another try to move on should still fail
		System.out.println("11. false\t" + rs.next());
		// >> true: and we should stay positioned after the last
		System.out.println("12. true\t" + rs.isAfterLast());

		rs.close();

		con.close();
	}
}
