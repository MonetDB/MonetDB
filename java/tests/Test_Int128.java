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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/* Test whether we can represent a full-size int128 as JDBC results */
public class Test_Int128 {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		BigInteger bi = new BigInteger(
				"123456789012345678909876543210987654321");
		BigDecimal bd = new BigDecimal(
				"1234567890123456789.9876543210987654321");
		try {
			con.setAutoCommit(false);
			Statement s = con.createStatement();
			s.executeUpdate("CREATE TABLE HUGEINTT (I HUGEINT)");
			s.executeUpdate("CREATE TABLE HUGEDECT (I DECIMAL(38,19))");

			PreparedStatement insertStatement = con
					.prepareStatement("INSERT INTO HUGEINTT VALUES (?)");
			insertStatement.setBigDecimal(1, new BigDecimal(bi));
			insertStatement.executeUpdate();
			insertStatement.close();
			
			s.executeUpdate("INSERT INTO HUGEDECT VALUES ("+bd+");");

			ResultSet rs = s.executeQuery("SELECT I FROM HUGEINTT");
			rs.next();
			BigInteger biRes = rs.getBigDecimal(1).toBigInteger();
			rs.close();
			rs = s.executeQuery("SELECT I FROM HUGEDECT");
			rs.next();
			BigDecimal bdRes = rs.getBigDecimal(1);
			rs.close();
			s.close();
			
			System.out.println("Expecting " + bi + ", got " + biRes);
			if (!bi.equals(biRes)) {
				throw new RuntimeException();
			}
			
			System.out.println("Expecting " + bd + ", got " + bdRes);
			if (!bd.equals(bdRes)) {
				throw new RuntimeException();
			}
			System.out.println("SUCCESS");

		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.close();
	}
}
