/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
