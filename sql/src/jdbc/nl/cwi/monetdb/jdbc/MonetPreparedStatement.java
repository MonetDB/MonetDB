/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 *
 * The Original Code is the Monet Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2004 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.jdbc;

import java.sql.*;
import java.util.*;
import java.net.URL;
import java.io.*;
import java.math.*;	// BigDecimal, etc.

/**
 * A PreparedStatement suitable for the MonetDB database.
 * <br /><br />
 * This implementation is purely for enabling applications to use the interface
 * PreparedStatement, for it does currently not use a PREPARE call on the
 * MonetDB SQL backend.
 * <br /><br />
 * Because we have no DBMS support here, we need to fake the lot to
 * make it work.  We try to spot `?', when not enclosed by single
 * or double quotes.  The positions of the question marks will later
 * be replaced by the appropriate SQL and sent to the server using the
 * underlying Statement.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 0.2
 */
public class MonetPreparedStatement
	extends MonetStatement
	implements PreparedStatement
{
	private final String pQuery;

	private final int[] pos;
	private final String[] value;

	/**
	 * MonetStatement constructor which checks the arguments for validity, tries
	 * to set up a socket to MonetDB and attempts to login.
	 * This constructor is only accessible to classes from the jdbc package.
	 *
	 * @param connection the connection that created this Statement
	 * @param resultSetType type of ResultSet to produce
	 * @param resultSetConcurrency concurrency of ResultSet to produce
	 * @throws SQLException if an error occurs during login
	 * @throws IllegalArgumentException is one of the arguments is null or empty
	 */
	MonetPreparedStatement(
		MonetConnection connection,
		int resultSetType,
		int resultSetConcurrency,
		String prepareQuery)
		throws SQLException, IllegalArgumentException
	{
		super(
			connection,
			resultSetType,
			resultSetConcurrency
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
		int size = tpos.size();
		pos = new int[size];
		value = new String[size];

		// fill the position array
		for (int i = 0; i < size; i++) {
			pos[i] = ((Integer)(tpos.get(i))).intValue();
		}
	}


	//== methods interface PreparedStatement

	/**
	 * Adds a set of parameters to this PreparedStatement object's batch
	 * of commands.
	 *
	 * @throws SQLException if a database access error occurs
	 */
	public void addBatch() throws SQLException {
		super.addBatch(transform());
	}

	/** override the execute from the Statement to throw an SQLException */
	public void addBatch(String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!");
	}

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
	 * Executes the SQL statement in this PreparedStatement object, which may
	 * be any kind of SQL statement. Some prepared statements return multiple
	 * results; the execute method handles these complex statements as well as
	 * the simpler form of statements handled by the methods executeQuery and
	 * executeUpdate.
	 * <br /><br />

	 * The execute method returns a boolean to indicate the form of the first
	 * result. You must call either the method getResultSet or getUpdateCount
	 * to retrieve the result; you must call getMoreResults to move to any
	 * subsequent result(s).
	 *
	 * @return true if the first result is a ResultSet object; false if the
	 *              first result is an update count or there is no result
	 * @throws SQLException if a database access error occurs or an argument
	 *                      is supplied to this method
	 */
	public boolean execute() throws SQLException {
		return(super.execute(transform()));
	}

	/** override the execute from the Statement to throw an SQLException */
	public boolean execute(String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!");
	}

	/**
	 * Executes the SQL query in this PreparedStatement object and returns the
	 * ResultSet object generated by the query.
	 *
	 * @return a ResultSet object that contains the data produced by the query;
	 *         never null
	 * @throws SQLException if a database access error occurs or the SQL
	 *                      statement does not return a ResultSet object
	 */
	public ResultSet executeQuery() throws SQLException{
		if (execute() != true)
			throw new SQLException("Query did not produce a result set");

		return(getResultSet());
	}

	/** override the executeQuery from the Statement to throw an SQLException*/
	public ResultSet executeQuery(String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!");
	}

	/**
	 * Executes the SQL statement in this PreparedStatement object, which must
	 * be an SQL INSERT, UPDATE or DELETE statement; or an SQL statement that
	 * returns nothing, such as a DDL statement.
	 *
	 * @return either (1) the row count for INSERT, UPDATE, or DELETE
	 *         statements or (2) 0 for SQL statements that return nothing
	 * @throws SQLException if a database access error occurs or the SQL
	 *                     statement returns a ResultSet object
	 */
	public int executeUpdate() throws SQLException {
		if (execute() != false)
			throw new SQLException("Query produced a result set");

		return(getUpdateCount());
	}

	/** override the executeUpdate from the Statement to throw an SQLException*/
	public int executeUpdate(String q) throws SQLException {
		throw new SQLException("This method is not available in a PreparedStatement!");
	}

	/**
	 * Retrieves a ResultSetMetaData object that contains information about the
	 * columns of the ResultSet object that will be returned when this
	 * PreparedStatement object is executed.
	 * <br /><br />
	 * Because a PreparedStatement object is precompiled, it is possible to
	 * know about the ResultSet object that it will return without having to
	 * execute it. Consequently, it is possible to invoke the method
	 * getMetaData on a PreparedStatement object rather than waiting to execute
	 * it and then invoking the ResultSet.getMetaData method on the ResultSet
	 * object that is returned.
	 * <br /><br />
	 * NOTE: Using this method is expensive for this driver due to the lack of
	 * underlying DBMS support.  Currently not implemented
	 *
	 * @return the description of a ResultSet object's columns or null if the
	 *         driver cannot return a ResultSetMetaData object
	 * @throws SQLException if a database access error occurs
	 */
	public ResultSetMetaData getMetaData() throws SQLException {
		throw new SQLException("Method currently not supported, sorry!");
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
	 * Sets the designated parameter to the given Array object. The driver
	 * converts this to an SQL ARRAY value when it sends it to the database.
     *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param x an Array object that maps an SQL ARRAY value
	 * @throws SQLException if a database access error occurs
	 */
	public void setArray(int i, Array x) throws SQLException {
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the designated parameter to the given input stream, which will have
	 * the specified number of bytes. When a very large ASCII value is input to
	 * a LONGVARCHAR parameter, it may be more practical to send it via a
	 * java.io.InputStream. Data will be read from the stream as needed until
	 * end-of-file is reached. The JDBC driver will do any necessary conversion
	 * from ASCII to the database char format.
	 * <br /><br />
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the Java input stream that contains the ASCII parameter value
	 * @param length the number of bytes in the stream
	 * @throws SQLException if a database access error occurs
	 */
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
		throws SQLException
	{
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the designated parameter to the given java.math.BigDecimal value.
	 * The driver converts this to an SQL NUMERIC value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setBigDecimal(int parameterIndex, BigDecimal x)
		throws SQLException
	{
		setValue(parameterIndex, x.toString());
	}

	/**
	 * Sets the designated parameter to the given input stream, which will have
	 * the specified number of bytes. When a very large binary value is input
	 * to a LONGVARBINARY parameter, it may be more practical to send it via a
	 * java.io.InputStream object. The data will be read from the stream as
	 * needed until end-of-file is reached.
	 * <br /><br />
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the java input stream which contains the binary parameter value
	 * @param length the number of bytes in the stream
	 * @throws SQLException if a database access error occurs
	 */
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
		throws SQLException
	{
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the designated parameter to the given Blob object. The driver
	 * converts this to an SQL BLOB value when it sends it to the database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param x a Blob object that maps an SQL BLOB value
	 * @throws SQLException if a database access error occurs
	 */
	public void setBlob(int i, Blob x) throws SQLException {
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the designated parameter to the given Java boolean value. The
	 * driver converts this to an SQL BIT value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		setValue(parameterIndex, "" + x);
	}

	/**
	 * Sets the designated parameter to the given Java byte value. The driver
	 * converts this to an SQL TINYINT value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setByte(int parameterIndex, byte x) throws SQLException {
		setValue(parameterIndex, "" + x);
	}

	/**
	 * Sets the designated parameter to the given Java array of bytes. The
	 * driver converts this to an SQL VARBINARY or LONGVARBINARY (depending
	 * on the argument's size relative to the driver's limits on VARBINARY
	 * values) when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the designated parameter to the given Reader object, which is the
	 * given number of characters long. When a very large UNICODE value is
	 * input to a LONGVARCHAR parameter, it may be more practical to send it
	 * via a java.io.Reader object. The data will be read from the stream as
	 * needed until end-of-file is reached. The JDBC driver will do any
	 * necessary conversion from UNICODE to the database char format.
	 * <br /><br />
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 * <br /><br />
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param reader the java.io.Reader object that contains the Unicode data
	 * @param length the number of characters in the stream
	 * @throws SQLException if a database access error occurs
	 */
	public void setCharacterStream(
		int parameterIndex,
		Reader reader,
		int length)
		throws SQLException
	{
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the designated parameter to the given Clob object. The driver
	 * converts this to an SQL CLOB value when it sends it to the database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param x a Clob object that maps an SQL CLOB value
	 * @throws SQLException if a database access error occurs
	 */
	public void setClob(int i, Clob x) throws SQLException {
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the designated parameter to the given java.sql.Date value. The
	 * driver converts this to an SQL DATE value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
		setValue(parameterIndex, "'" + x.toString() + "'");
	}

	/**
	 * Sets the designated parameter to the given java.sql.Date value, using
	 * the given Calendar object. The driver uses the Calendar object to
	 * construct an SQL DATE value, which the driver then sends to the
	 * database. With a Calendar object, the driver can calculate the date
	 * taking into account a custom timezone. If no Calendar object is
	 * specified, the driver uses the default timezone, which is that of the
	 * virtual machine running the application.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the date
	 * @throws SQLException if a database access error occurs
	 */
	public void setDate(int parameterIndex, java.sql.Date x, Calendar cal)
		throws SQLException
	{
		synchronized (MonetConnection.mDate) {
			MonetConnection.mDate.setTimeZone(cal.getTimeZone());
			setValue(parameterIndex, "'" + MonetConnection.mDate.format(x) + "'");
		}
	}

	/**
	 * Sets the designated parameter to the given Java double value. The driver
	 * converts this to an SQL DOUBLE value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setDouble(int parameterIndex, double x) throws SQLException {
		setValue(parameterIndex, "" + x);
	}

	/**
	 * Sets the designated parameter to the given Java float value. The driver
	 * converts this to an SQL FLOAT value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param SQLException if a database access error occurs
	 */
	public void setFloat(int parameterIndex, float x) throws SQLException {
		setValue(parameterIndex, "" + x);
	}

	/**
	 * Sets the designated parameter to the given Java int value. The driver
	 * converts this to an SQL INTEGER value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setInt(int parameterIndex, int x) throws SQLException {
		setValue(parameterIndex, "" + x);
	}

	/**
	 * Sets the designated parameter to the given Java long value. The driver
	 * converts this to an SQL BIGINT value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setLong(int parameterIndex, long x) throws SQLException {
		setValue(parameterIndex, "" + x);
	}

	/**
	 * Sets the designated parameter to SQL NULL.
	 * <br /><br />
	 * Note: You must specify the parameter's SQL type.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param sqlType the SQL type code defined in java.sql.Types
	 * @throws SQLException if a database access error occurs
	 */
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		// should be: "CAST(NULL AS " + asMonetType(sqlType) + ")"
		setValue(parameterIndex, "NULL");
	}

	/**
	 * Sets the designated parameter to SQL NULL. This version of the method
	 * setNull should be used for user-defined types and REF type parameters.
	 * Examples of user-defined types include: STRUCT, DISTINCT, JAVA_OBJECT,
	 * and named array types.
	 * <br /><br />
	 * Note: To be portable, applications must give the SQL type code and the
	 * fully-qualified SQL type name when specifying a NULL user-defined or REF
	 * parameter. In the case of a user-defined type the name is the type name
	 * of the parameter itself. For a REF parameter, the name is the type name
	 * of the referenced type. If a JDBC driver does not need the type code or
	 * type name information, it may ignore it. Although it is intended for
	 * user-defined and Ref parameters, this method may be used to set a null
	 * parameter of any JDBC type. If the parameter does not have a
	 * user-defined or REF type, the given typeName is ignored.
	 *
	 * @param paramIndex the first parameter is 1, the second is 2, ...
	 * @param sqlType a value from java.sql.Types
	 * @param typeName the fully-qualified name of an SQL user-defined type;
	 *                 ignored if the parameter is not a user-defined type or
	 *                 REF
	 * @throws SQLException if a database access error occurs
	 */
	public void setNull(int paramIndex, int sqlType, String typeName)
		throws SQLException
	{
		throw new SQLException("Operation currently not supported!");
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
			throw new SQLException("Operation currently not supported!");
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
	 * Sets the value of the designated parameter with the given object. This
	 * method is like the method setObject blow, except that it assumes a scale
	 * of zero.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @param targetSqlType the SQL type (as defined in java.sql.Types) to be
	 *                      sent to the database
	 * @throws SQLException if a database access error occurs
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType)
		throws SQLException
	{
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the value of the designated parameter with the given object. The
	 * second argument must be an object type; for integral values, the
	 * java.lang equivalent objects should be used.
	 * <br /><br />
	 * The given Java object will be converted to the given targetSqlType
	 * before being sent to the database. If the object has a custom mapping
	 * (is of a class implementing the interface SQLData), the JDBC driver
	 * should call the method SQLData.writeSQL to write it to the SQL data
	 * stream. If, on the other hand, the object is of a class implementing
	 * Ref, Blob, Clob, Struct, or Array, the driver should pass it to the
	 * database as a value of the corresponding SQL type.
	 * <br /><br />
	 * Note that this method may be used to pass database-specific abstract
	 * data types.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @param targetSqlType the SQL type (as defined in java.sql.Types) to
	 *                      be sent to the database. The scale argument may
	 *                      further qualify this type.
	 * @param scale for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
	 *              this is the number of digits after the decimal point. For
	 *              all other types, this value will be ignored.
	 * @throws SQLException if a database access error occurs
	 * @see Types
	 */
	public void setObject(
		int parameterIndex,
		Object x,
		int targetSqlType,
		int scale)
		throws SQLException
	{
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the designated parameter to the given REF(<structured-type>) value.
	 * The driver converts this to an SQL REF value when it sends it to the
	 * database.
	 *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param x an SQL REF value
	 * @throws SQLException if a database access error occurs
	 */
	public void setRef(int i, Ref x) throws SQLException {
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the designated parameter to the given Java short value. The driver
	 * converts this to an SQL SMALLINT value when it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setShort(int parameterIndex, short x) throws SQLException {
		setValue(parameterIndex, "" + x);
	}

	/**
	 * Sets the designated parameter to the given Java String value. The driver
	 * converts this to an SQL VARCHAR or LONGVARCHAR value (depending on the
	 * argument's size relative to the driver's limits on VARCHAR values) when
	 * it sends it to the database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setString(int parameterIndex, String x) throws SQLException {
		setValue(
			parameterIndex,
			"'" + x.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'"
		);
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
		synchronized (MonetConnection.mTimeZ) {
			MonetConnection.mTimeZ.setTimeZone(cal.getTimeZone());

			String RFC822 = MonetConnection.mTimeZ.format(x);
			setValue(parameterIndex, "'" +
				RFC822.substring(0, 15) + ":" + RFC822.substring(15) + "'");
		}
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
		synchronized (MonetConnection.mTimestampZ) {
			MonetConnection.mTimestampZ.setTimeZone(cal.getTimeZone());
			String RFC822 = MonetConnection.mTimestampZ.format(x);
			setValue(parameterIndex, "'" +
				RFC822.substring(0, 26) + ":" + RFC822.substring(26) + "'");
		}
	}

	/**
	 * Deprecated.
	 * <br /><br />
	 * Sets the designated parameter to the given input stream, which will have
	 * the specified number of bytes. A Unicode character has two bytes, with
	 * the first byte being the high byte, and the second being the low byte.
	 * When a very large Unicode value is input to a LONGVARCHAR parameter, it
	 * may be more practical to send it via a java.io.InputStream object. The
	 * data will be read from the stream as needed until end-of-file is
	 * reached. The JDBC driver will do any necessary conversion from Unicode
	 * to the database char format.
	 * <br /><br />
	 * Note: This stream object can either be a standard Java stream object or
	 * your own subclass that implements the standard interface.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x a java.io.InputStream object that contains the Unicode
	 *          parameter value as two-byte Unicode characters
	 * @param length the number of bytes in the stream
	 * @throws SQLException if a database access error occurs
	 */
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
		throws SQLException
	{
		throw new SQLException("Operation currently not supported!");
	}

	/**
	 * Sets the designated parameter to the given java.net.URL value. The
	 * driver converts this to an SQL DATALINK value when it sends it to the
	 * database.
	 *
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x the java.net.URL object to be set
	 * @throws SQLException if a database access error occurs
	 */
	public void setURL(int parameterIndex, URL x) throws SQLException {
		throw new SQLException("Operation currently not supported!");
	}

	//== end methods interface PreparedStatement

	/**
	 * Sets the given index with the supplied value. If the given index is
	 * out of bounds, and SQLException is thrown.  The given value should
	 * never be null.
	 *
	 * @param index the parameter index
	 * @param val the exact String representation to set
	 * @throws SQLException if the given index is out of bounds
	 */
	private void setValue(int index, String val) throws SQLException {
		if (index <= 0 || index > pos.length) throw
			new SQLException("No such parameter index: " + index);

		value[index - 1] = val;
	}

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
