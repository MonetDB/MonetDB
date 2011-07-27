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

package nl.cwi.monetdb.jdbc;

import java.sql.*;
import java.util.*;
import java.net.URL;
import java.io.*;
import java.math.*;	// BigDecimal, etc.

/**
 * A PreparedStatement suitable for the MonetDB database implemented in
 * Java.
 * <br /><br />
 * Although this class is deprecated, and has been replaced by an
 * implementation that makes use of server side capabilities, this class
 * is still provided for users that somehow rely on the behaviours from
 * this Java implementation of a PreparedStatement.
 * <br /><br />
 * This implementation is purely for enabling applications to use the
 * interface PreparedStatement, for it does currently not use a PREPARE
 * call on the MonetDB SQL backend.
 * <br /><br />
 * Because we have no DBMS support here, we need to fake the lot to make
 * it work.  We try to spot `?', when not enclosed by single or double
 * quotes.  The positions of the question marks will later be replaced
 * by the appropriate SQL and sent to the server using the underlying
 * Statement.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 0.3
 * @deprecated
 */
public class MonetPreparedStatementJavaImpl
	extends MonetPreparedStatement
{
	private final String pQuery;

	private final int size;
	private final int[] pos;
	private final String[] value;

	/**
	 * Constructor which checks the arguments for validity.  This
	 * constructor is only accessible to classes from the jdbc package.
	 *
	 * @param connection the connection that created this Statement
	 * @param resultSetType type of ResultSet to produce
	 * @param resultSetConcurrency concurrency of ResultSet to produce
	 * @throws SQLException if an error occurs during login
	 * @throws IllegalArgumentException is one of the arguments is null or empty
	 */
	MonetPreparedStatementJavaImpl(
		MonetConnection connection,
		int resultSetType,
		int resultSetConcurrency,
		String prepareQuery)
		throws SQLException, IllegalArgumentException
	{
		super(
			connection,
			resultSetType,
			resultSetConcurrency,
			ResultSet.HOLD_CURSORS_OVER_COMMIT
		);

		pQuery = prepareQuery;

		// find qualifying ?'s
		List tpos = new ArrayList();

		boolean inString = false;
		boolean inIdentifier = false;
		boolean escaped = false;
		int len = pQuery.length();
		for (int i = 0; i < len; i++) {
			switch(pQuery.charAt(i)) {
				case '\\':
					escaped = !escaped;
				break;
				default:
					escaped = false;
				break;
				case '\'':
					/**
					 * We can not be in a string if we are in an identifier. So
					 * If we find a ' and are not in an identifier, and not in
					 * a string we can safely assume we will be now in a string.
					 * If we are in a string already, we should stop being in a
					 * string if we find a quote which is not prefixed by a \,
					 * for that would be an escaped quote. However, a nasty
					 * situation can occur where the string is like 'test \\'.
					 * As obvious, a test for a \ in front of a ' doesn't hold
					 * here. Because 'test \\\'' can exist as well, we need to
					 * know if a quote is prefixed by an escaping slash or not.
					 */
					if (!inIdentifier) {
						if (!inString && !escaped) {
							// although it makes no sense to escape a quote
							// outside a string, it is escaped, thus not meant
							// as quote for us, apparently
							inString = true;
						} else if (inString && !escaped) {
							inString = false;
						}
					} else {
						// reset escaped flag
						escaped = false;
					}
				break;
				case '"':
					if (!inString) {
						if (!inIdentifier && !escaped) {
							inIdentifier = true;
						} else if (inIdentifier && !escaped) {
							inIdentifier = false;
						}
					} else {
						// reset escaped flag
						escaped = false;
					}
				break;
				case '-':
					if (!escaped && !(inString || inIdentifier) && i + 1 < len && pQuery.charAt(i + 1) == '-') {
						int nl = pQuery.indexOf('\n', i + 1);
						if (nl == -1) {
							// no newline found, so we don't need to skip and
							// can stop right away
							len = pQuery.length();
						} else {
							// skip the comment when scanning for `?'
							i = nl;
						}
					}
					escaped = false;
				break;
				case '?':
					if (!escaped && !(inString || inIdentifier) && !escaped) {
						// mark this location
						tpos.add(new Integer(i));
					}
					escaped = false;
				break;
			}
		}

		// initialize the ? container arrays
		size = tpos.size();
		pos = new int[size];
		value = new String[size];

		// fill the position array
		for (int i = 0; i < size; i++) {
			pos[i] = ((Integer)(tpos.get(i))).intValue();
		}
	}


	//== methods interface PreparedStatement

	/**
	 * Clears the current parameter values immediately.
	 * <br /><br />
	 * In general, parameter values remain in force for repeated use of a
	 * statement. Setting a parameter value automatically clears its previous
	 * value. However, in some cases it is useful to immediately release the
	 * resources used by the current parameter values; this can be done by
	 * calling the method clearParameters.
	 */
	public void clearParameters() {
		for (int i = 0; i < pos.length; i++) {
			value[i] = null;
		}
	}

    /**
	 * Retrieves the number, types and properties of this PreparedStatement
	 * object's parameters.
	 *
	 * @return a ParameterMetaData object that contains information about the
	 *         number, types and properties of this PreparedStatement object's
	 *         parameters
	 * @throws SQLException if a database access error occurs
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new SQLException("Method currently not supported, sorry!");
	}

	/**
	 * Sets the value of the designated parameter using the given object. The
	 * second parameter must be of type Object; therefore, the java.lang
	 * equivalent objects should be used for built-in types.
	 * <br /><br />
	 * The JDBC specification specifies a standard mapping from Java Object
	 * types to SQL types. The given argument will be converted to the
	 * corresponding SQL type before being sent to the database.
	 * <br /><br />
	 * Note that this method may be used to pass datatabase-specific abstract
	 * data types, by using a driver-specific Java type. If the object is of a
	 * class implementing the interface SQLData, the JDBC driver should call
	 * the method SQLData.writeSQL to write it to the SQL data stream. If, on
	 * the other hand, the object is of a class implementing Ref, Blob, Clob,
	 * Struct, or Array, the driver should pass it to the database as a value
	 * of the corresponding SQL type.
	 * <br /><br />
	 * This method throws an exception if there is an ambiguity, for example,
	 * if the object is of a class implementing more than one of the interfaces
	 * named above.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @throws SQLException if a database access error occurs or the type of
	 *                      the given object is ambiguous
	 */
	public void setObject(int parameterIndex, Object x) throws SQLException {
		/**
		 * NOTE:
		 * This function should convert (see table B-5) the given object to the
		 * underlying datatype of the query requirements.
		 * This conversion is impossible since we don't know anything about the
		 * query at this point.  Future real prepare queries should solve this.
		 */
		if (x instanceof SQLData) {
			// do something with:
			// ((SQLData)x).writeSQL( [java.sql.SQLOutput] );
			// needs an SQLOutput stream... bit too far away from reality
			throw new SQLException("Operation setObject() with object of type SQLData currently not supported!");
		} else if (x instanceof Blob) {
			setBlob(parameterIndex, (Blob)x);
		} else if (x instanceof java.sql.Date) {
			setDate(parameterIndex, (java.sql.Date)x);
		} else if (x instanceof Timestamp) {
			setTimestamp(parameterIndex, (Timestamp)x);
		} else if (x instanceof Time) {
			setTime(parameterIndex, (Time)x);
		} else if (x instanceof Number) {
			// catches:
			// BigDecimal, BigInteger, Byte, Double, Float, Integer, Long, Short
			// just write them as is
			setValue(parameterIndex, x.toString());
		} else {
			// write the rest as 'varchar'
			setString(parameterIndex, x.toString());
		}
	}

	/**
	 * Sets the designated parameter to the given java.sql.Time value. The
	 * driver converts this to an SQL TIME value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setTime(int parameterIndex, Time x) throws SQLException {
		setValue(parameterIndex, "'" + x.toString() + "'");
	}

	/**
	 * Sets the designated parameter to the given java.sql.Time value, using
	 * the given Calendar object. The driver uses the Calendar object to
	 * construct an SQL TIME value, which the driver then sends to the
	 * database. With a Calendar object, the driver can calculate the time
	 * taking into account a custom timezone. If no Calendar object is
	 * specified, the driver uses the default timezone, which is that of the
	 * virtual machine running the application.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the time
	 * @throws SQLException if a database access error occurs
	 */
	public void setTime(int parameterIndex, Time x, Calendar cal)
		throws SQLException
	{
		mTimeZ.setTimeZone(cal.getTimeZone());

		String RFC822 = mTimeZ.format(x);
		setValue(parameterIndex, "'" +
				RFC822.substring(0, 15) + ":" + RFC822.substring(15) + "'");
	}

	/**
	 * Sets the designated parameter to the given java.sql.Timestamp value. The
	 * driver converts this to an SQL TIMESTAMP value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setTimestamp(int parameterIndex, Timestamp x)
		throws SQLException
	{
		setValue(parameterIndex, "'" + x.toString() + "'");
	}

    /**
	 * Sets the designated parameter to the given java.sql.Timestamp value,
	 * using the given Calendar object. The driver uses the Calendar object
	 * to construct an SQL TIMESTAMP value, which the driver then sends to
	 * the database. With a Calendar object, the driver can calculate the
	 * timestamp taking into account a custom timezone. If no Calendar object
	 * is specified, the driver uses the default timezone, which is that of the
	 * virtual machine running the application.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the
	 *            timestamp
	 * @throws SQLException if a database access error occurs
	 */
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
		throws SQLException
	{
		if (cal == null) cal = Calendar.getInstance();
		mTimestampZ.setTimeZone(cal.getTimeZone());
		String RFC822 = mTimestampZ.format(x);
		setValue(parameterIndex, "'" +
				RFC822.substring(0, 26) + ":" + RFC822.substring(26) + "'");
	}

	//== end methods interface PreparedStatement

	/**
	 * Transforms the prepare query into a simple SQL query by replacing
	 * the ?'s with the given column contents.
	 * Mind that the JDBC specs allow `reuse' of a value for a column over
	 * multiple executes.
	 *
	 * @return the simple SQL string for the prepare query
	 * @throws SQLException if not all columns are set
	 */
	private String transform() throws SQLException {
		StringBuffer ret = new StringBuffer(pQuery);
		// check if all columns are set and do a replace
		int offset = 0;
		for (int i = 0; i < pos.length; i++) {
			if (value[i] == null) throw
				new SQLException("Cannot execute, parameter " +  (i + 1) + " is missing.");

			ret.replace(offset + pos[i], offset + pos[i] + 1, value[i]);
			offset += value[i].length() - 1;
		}

		return(ret.toString());
	}
}
