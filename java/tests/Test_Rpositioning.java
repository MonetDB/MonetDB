/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
