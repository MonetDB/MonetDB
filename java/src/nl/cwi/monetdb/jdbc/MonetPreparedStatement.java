/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
import java.text.SimpleDateFormat;

/**
 * A PreparedStatement suitable for the MonetDB database.
 * <br /><br />
 * This implementation of the PreparedStatement interface uses the
 * capabilities of the MonetDB/SQL backend to prepare and execute
 * queries.  The backend takes care of finding the '?'s in the input and
 * returns the types it expects for them.
 * <br /><br />
 * An example of a server response on a prepare query is:
 * <pre>
 * % prepare select name from tables where id &gt; ? and id &lt; ?;
 * &amp;5 0 2 3 2
 * # prepare,      prepare,        prepare # table_name
 * # type, digits, scale # name
 * # varchar,      int,    int # type
 * # 0,    0,      0 # length
 * [ "int",        9,      0       ]
 * [ "int",        9,      0       ]
 * </pre>
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 0.2
 */
public class MonetPreparedStatement
	extends MonetStatement
	implements PreparedStatement
{
	private final String[] monetdbType;
	private final int[] javaType;
	private final int[] digits;
	private final int[] scale;
	private final int id;
	private final int size;

	private final String[] values;
	private final StringBuffer buf;
	
	private final MonetConnection connection;

	/* only parse the date patterns once, use multiple times */
	/** Format of a timestamp with RFC822 time zone */
	final SimpleDateFormat mTimestampZ =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
	/** Format of a time with RFC822 time zone */
	final SimpleDateFormat mTimeZ =
		new SimpleDateFormat("HH:mm:ss.SSSZ");
	/** Format of a date used by Mserver */
	final SimpleDateFormat mDate =
		new SimpleDateFormat("yyyy-MM-dd");

	/**
	 * MonetPreparedStatement constructor which checks the arguments for
	 * validity.  A MonetPreparedStatement is backed by a
	 * MonetStatement, which deals with most of the required stuff of
	 * this class.
	 *
	 * @param connection the connection that created this Statement
	 * @param resultSetType type of ResultSet to produce
	 * @param resultSetConcurrency concurrency of ResultSet to produce
	 * @param prepareQuery the query string to prepare
	 * @throws SQLException if an error occurs during login
	 * @throws IllegalArgumentException is one of the arguments is null or empty
	 */
	MonetPreparedStatement(
			MonetConnection connection,
			int resultSetType,
			int resultSetConcurrency,
			int resultSetHoldability,
			String prepareQuery)
		throws SQLException, IllegalArgumentException
	{
		super(
			connection,
			resultSetType,
			resultSetConcurrency,
			resultSetHoldability
		);

		if (!super.execute("PREPARE " + prepareQuery))
			throw new SQLException("Unexpected server response");

		// cheat a bit to get the ID and the number of columns
		id = ((MonetConnection.ResultSetResponse)header).id;
		size = ((MonetConnection.ResultSetResponse)header).tuplecount;

		// initialise blank finals
		monetdbType = new String[size];
		javaType = new int[size];
		digits = new int[size];
		scale = new int[size];
		values = new String[size];
		buf = new StringBuffer(6 + 12 * size);

		this.connection = connection;

		// fill the arrays
		ResultSet rs = super.getResultSet();
		for (int i = 0; rs.next(); i++) {
			monetdbType[i] = rs.getString("type");
			javaType[i] = MonetDriver.getJavaType(monetdbType[i]);
			digits[i] = rs.getInt("digits");
			scale[i] = rs.getInt("scale");
		}
		rs.close();
	}

	/**
	 * Constructs an empty MonetPreparedStatement.  This constructor is
	 * in particular useful for extensions of this class.
	 *
	 * @param connection the connection that created this Statement
	 * @param resultSetType type of ResultSet to produce
	 * @param resultSetConcurrency concurrency of ResultSet to produce
	 * @throws SQLException if an error occurs during login
	 */
	MonetPreparedStatement(
			MonetConnection connection,
			int resultSetType,
			int resultSetConcurrency,
			int resultSetHoldability)
		throws SQLException
	{
		super(
			connection,
			resultSetType,
			resultSetConcurrency,
			resultSetHoldability
		);
		// initialise blank finals
		monetdbType = null;
		javaType = null;
		digits = null;
		scale = null;
		values = null;
		buf = null;
		id = -1;
		size = -1;

		this.connection = connection;
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

	/** override the addBatch from the Statement to throw an SQLException */
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
		for (int i = 0; i < values.length; i++) {
			values[i] = null;
		}
	}

	/**
	 * Executes the SQL statement in this PreparedStatement object,
	 * which may be any kind of SQL statement.  Some prepared statements
	 * return multiple results; the execute method handles these complex
	 * statements as well as the simpler form of statements handled by
	 * the methods executeQuery and executeUpdate.
	 * <br /><br />
	 * The execute method returns a boolean to indicate the form of the
	 * first result.  You must call either the method getResultSet or
	 * getUpdateCount to retrieve the result; you must call
	 * getMoreResults to move to any subsequent result(s).
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
	 * Retrieves a ResultSetMetaData object that contains information
	 * about the columns of the ResultSet object that will be returned
	 * when this PreparedStatement object is executed.
	 * <br /><br />
	 * Because a PreparedStatement object is precompiled, it is possible
	 * to know about the ResultSet object that it will return without
	 * having to execute it.  Consequently, it is possible to invoke the
	 * method getMetaData on a PreparedStatement object rather than
	 * waiting to execute it and then invoking the ResultSet.getMetaData
	 * method on the ResultSet object that is returned.
	 * <br /><br />
	 * NOTE: Using this method is expensive for this driver due to the
	 * lack of underlying DBMS support.  Currently not implemented
	 *
	 * @return the description of a ResultSet object's columns or null if the
	 *         driver cannot return a ResultSetMetaData object
	 * @throws SQLException if a database access error occurs
	 */
	public ResultSetMetaData getMetaData() throws SQLException {
		return(null);
	}

    /**
	 * Retrieves the number, types and properties of this
	 * PreparedStatement object's parameters.
	 *
	 * @return a ParameterMetaData object that contains information
	 *         about the number, types and properties of this
	 *         PreparedStatement object's parameters
	 * @throws SQLException if a database access error occurs
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return(new ParameterMetaData() {
			/**
			 * Retrieves the number of parameters in the
			 * PreparedStatement object for which this ParameterMetaData
			 * object contains information.
			 *
			 * @return the number of parameters
			 * @throws SQLException if a database access error occurs
			 */
			public int getParameterCount() throws SQLException {
				return(size);
			}

			/**
			 * Retrieves whether null values are allowed in the
			 * designated parameter.
			 * <br /><br />
			 * This is currently always unknown for MonetDB/SQL.
			 *
			 * @param param the first parameter is 1, the second is 2, ... 
			 * @return the nullability status of the given parameter;
			 *         one of ParameterMetaData.parameterNoNulls,
			 *         ParameterMetaData.parameterNullable, or
			 *         ParameterMetaData.parameterNullableUnknown 
			 * @throws SQLException if a database access error occurs
			 */
			public int isNullable(int param) throws SQLException {
				return(ParameterMetaData.parameterNullableUnknown);
			}

			/**
			 * Retrieves whether values for the designated parameter can
			 * be signed numbers.
			 *
			 * @param param the first parameter is 1, the second is 2, ... 
			 * @return true if so; false otherwise 
			 * @throws SQLException if a database access error occurs
			 */
			public boolean isSigned(int param) throws SQLException {
				if (param < 1 || param > size)
					throw new SQLException("No such parameter with index: " + param);

				// we can hardcode this, based on the colum type
				// (from ResultSetMetaData.isSigned)
				switch (javaType[param - 1]) {
					case Types.NUMERIC:
					case Types.DECIMAL:
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.BIGINT:
					case Types.REAL:
					case Types.FLOAT:
					case Types.DOUBLE:
						return(true);
					case Types.BIT: // we don't use type BIT, it's here for completeness
					case Types.BOOLEAN:
					case Types.DATE:
					case Types.TIME:
					case Types.TIMESTAMP:
					default:
						return(false);
				}
			}

			/**
			 * Retrieves the designated parameter's number of decimal
			 * digits.
			 *
			 * @param param the first parameter is 1, the second is 2, ... 
			 * @return precision
			 * @throws SQLException if a database access error occurs
			 */
			public int getPrecision(int param) throws SQLException {
				if (param < 1 || param > size)
					throw new SQLException("No such parameter with index: " + param);

				return(digits[param - 1]);
			}

			/**
			 * Retrieves the designated parameter's number of digits to
			 * right of the decimal point.
			 *
			 * @param param the first parameter is 1, the second is 2, ... 
			 * @return scale 
			 * @throws SQLException if a database access error occurs
			 */
			public int getScale(int param) throws SQLException {
				if (param < 1 || param > size)
					throw new SQLException("No such parameter with index: " + param);

				return(scale[param - 1]);
			}

			/**
			 * Retrieves the designated parameter's SQL type.
			 *
			 * @param param the first parameter is 1, the second is 2, ... 
			 * @return SQL type from java.sql.Types 
			 * @throws SQLException if a database access error occurs
			 */
			public int getParameterType(int param) throws SQLException {
				if (param < 1 || param > size)
					throw new SQLException("No such parameter with index: " + param);

				return(javaType[param - 1]);
			}

			/**
			 * Retrieves the designated parameter's database-specific
			 * type name.
			 *
			 * @param param the first parameter is 1, the second is 2, ... 
			 * @return type the name used by the database.  If the
			 *         parameter type is a user-defined type, then a
			 *         fully-qualified type name is returned. 
			 * @throws SQLException if a database access error occurs
			 */
			public String getParameterTypeName(int param) throws SQLException {
				if (param < 1 || param > size)
					throw new SQLException("No such parameter with index: " + param);

				return(monetdbType[param - 1]);
			}

			/**
			 * Retrieves the fully-qualified name of the Java class
			 * whose instances should be passed to the method
			 * PreparedStatement.setObject.
			 *
			 * @param param the first parameter is 1, the second is 2, ... 
			 * @return the fully-qualified name of the class in the Java
			 *         programming language that would be used by the
			 *         method PreparedStatement.setObject to set the
			 *         value in the specified parameter. This is the
			 *         class name used for custom mapping. 
			 * @throws SQLException if a database access error occurs
			 */
			public String getParameterClassName(int param) throws SQLException {
				if (param < 1 || param > size)
					throw new SQLException("No such parameter with index: " + param);

				return(MonetResultSet.getClassForType(javaType[param - 1]).getName());
			}

			/**
			 * Retrieves the designated parameter's mode.
			 * For MonetDB/SQL this is currently always unknown.
			 *
			 * @param param - the first parameter is 1, the second is 2, ... 
			 * @return mode of the parameter; one of
			 *         ParameterMetaData.parameterModeIn,
			 *         ParameterMetaData.parameterModeOut, or
			 *         ParameterMetaData.parameterModeInOut
			 *         ParameterMetaData.parameterModeUnknown. 
			 * @throws SQLException if a database access error occurs
			 */
			public int getParameterMode(int param) throws SQLException {
				return(ParameterMetaData.parameterModeUnknown);
			}
		});
	}

	/**
	 * Sets the designated parameter to the given Array object.  The
	 * driver converts this to an SQL ARRAY value when it sends it to
	 * the database.
     *
	 * @param i the first parameter is 1, the second is 2, ...
	 * @param x an Array object that maps an SQL ARRAY value
	 * @throws SQLException if a database access error occurs
	 */
	public void setArray(int i, Array x) throws SQLException {
		throw new SQLException("Operation setArray(int i, Array x) currently not supported!");
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
		throw new SQLException("Operation setAsciiStream(int parameterIndex, InputStream x, int length) currently not supported!");
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
		throw new SQLException("Operation setBinaryStream(int parameterIndex, InputStream x, int length) currently not supported!");
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
		throw new SQLException("Operation setBlob(int i, Blob x) currently not supported!");
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

	static final String HEXES = "0123456789ABCDEF";
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
		if (x == null) {
			setNull(parameterIndex, -1);
			return;
		}

		StringBuffer hex = new StringBuffer(x.length * 2);
		byte b;
		for (int i = 0; i < x.length; i++) {
			b = x[i];
			hex.append(HEXES.charAt((b & 0xF0) >> 4))
				.append(HEXES.charAt((b & 0x0F)));
		}
		setValue(parameterIndex, "blob '" + hex.toString() + "'");
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
		throw new SQLException("Operation setCharacterStream(int parameterIndex, Reader reader, int length) currently not supported!");
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
		// simply serialise the CLOB into a variable for now... far from
		// efficient, but might work for a few cases...
		// be on your marks: we have to cast the length down!
		setString(i, x.getSubString(1L, (int)(x.length())));
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
	public void setDate(int parameterIndex, java.sql.Date x)
		throws SQLException
	{
		setValue(parameterIndex, "date '" + x.toString() + "'");
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
		mDate.setTimeZone(cal.getTimeZone());
		setValue(parameterIndex, "date '" + mDate.format(x) + "'");
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
	 * @throws SQLException if a database access error occurs
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
		// we discard the given type here, the backend converts the
		// value NULL to whatever it needs for the column
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
		// MonetDB/SQL's NULL needs no type
		setNull(paramIndex, sqlType);
	}

	/**
	 * Sets the value of the designated parameter using the given
	 * object.  The second parameter must be of type Object; therefore,
	 * the java.lang equivalent objects should be used for built-in
	 * types.
	 * <br /><br />
	 * The JDBC specification specifies a standard mapping from Java
	 * Object types to SQL types. The given argument will be converted
	 * to the corresponding SQL type before being sent to the database.
	 * <br /><br />
	 * Note that this method may be used to pass datatabase-specific
	 * abstract data types, by using a driver-specific Java type. If the
	 * object is of a class implementing the interface SQLData, the JDBC
	 * driver should call the method SQLData.writeSQL to write it to the
	 * SQL data stream. If, on the other hand, the object is of a class
	 * implementing Ref, Blob, Clob, Struct, or Array, the driver should
	 * pass it to the database as a value of the corresponding SQL type.
	 * <br /><br />
	 * This method throws an exception if there is an ambiguity, for
	 * example, if the object is of a class implementing more than one
	 * of the interfaces named above.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the object containing the input parameter value
	 * @throws SQLException if a database access error occurs or the type of
	 *                      the given object is ambiguous
	 */
	public void setObject(int index, Object x) throws SQLException {
		if (index < 1 || index > size)
			throw new SQLException("No such parameter with index: " + index);

		setObject(index, x, javaType[index - 1]);
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
		setObject(parameterIndex, x, targetSqlType, 0);
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
	 * <br /><br />
	 * To meet the requirements of this interface, the Java object is
	 * converted in the driver, instead of using a SQL CAST construct.
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
		// this is according to table B-5
		if (x instanceof String) {
			switch (targetSqlType) {
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
				{
					int val;
					try {
						val = Integer.parseInt((String)x);
					} catch (NumberFormatException e) {
						val = 0;
					}
					setInt(parameterIndex, val);
				} break;
				case Types.BIGINT:
				{
					long val;
					try {
						val = Long.parseLong((String)x);
					} catch (NumberFormatException e) {
						val = 0;
					}
					setLong(parameterIndex, val);
				} break;
				case Types.REAL:
				{
					float val;
					try {
						val = Float.parseFloat((String)x);
					} catch (NumberFormatException e) {
						val = 0;
					}
					setFloat(parameterIndex, val);
				} break;
				case Types.FLOAT:
				case Types.DOUBLE:
				{
					double val;
					try {
						val = Double.parseDouble((String)x);
					} catch (NumberFormatException e) {
						val = 0;
					}
					setDouble(parameterIndex, val);
				} break;
				case Types.DECIMAL:
				case Types.NUMERIC:
				{
					BigDecimal val;
					try {
						val = new BigDecimal((String)x);
					} catch (NumberFormatException e) {
						try {
							val = new BigDecimal(0.0);
						} catch (NumberFormatException ex) {
							throw new SQLException("Internal error: unable to create template BigDecimal: " + ex.getMessage());
						}
					}
					val = val.setScale(scale, BigDecimal.ROUND_HALF_UP);
					setBigDecimal(parameterIndex, val);
				} break;
				case Types.BIT:
				case Types.BOOLEAN:
					setBoolean(parameterIndex, (Boolean.valueOf((String)x)).booleanValue());
				break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
					setString(parameterIndex, (String)x);
				break;
				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
					setBytes(parameterIndex, ((String)x).getBytes());
				break;
				case Types.DATE:
				{
					java.sql.Date val;
					try {
						val = java.sql.Date.valueOf((String)x);
					} catch (IllegalArgumentException e) {
						val = new java.sql.Date(0L);
					}
					setDate(parameterIndex, val);
				} break;
				case Types.TIME:
				{
					Time val;
					try {
						val = Time.valueOf((String)x);
					} catch (IllegalArgumentException e) {
						val = new Time(0L);
					}
					setTime(parameterIndex, val);
				} break;
				case Types.TIMESTAMP:
				{
					Timestamp val;
					try {
						val = Timestamp.valueOf((String)x);
					} catch (IllegalArgumentException e) {
						val = new Timestamp(0L);
					}
					setTimestamp(parameterIndex, val);
				} break;
				default:
					throw new SQLException("Conversion not allowed");
			}
		} else if (x instanceof Number) {
			Number num = (Number)x;
			switch (targetSqlType) {
				case Types.TINYINT:
					setByte(parameterIndex, num.byteValue());
				break;
				case Types.SMALLINT:
					setShort(parameterIndex, num.shortValue());
				break;
				case Types.INTEGER:
					setInt(parameterIndex, num.intValue());
				break;
				case Types.BIGINT:
					if (x instanceof BigDecimal) {
						setLong(parameterIndex, ((BigDecimal)x).setScale(scale, BigDecimal.ROUND_HALF_UP).longValue());
					} else {
						setLong(parameterIndex, num.longValue());
					}
				break;
				case Types.REAL:
					setFloat(parameterIndex, num.floatValue());
				break;
				case Types.FLOAT:
				case Types.DOUBLE:
					setDouble(parameterIndex, num.doubleValue());
				break;
				case Types.DECIMAL:
				case Types.NUMERIC:
					if (x instanceof BigDecimal) {
						setBigDecimal(parameterIndex, (BigDecimal)x);
					} else if (x instanceof BigInteger) {
						BigDecimal val;
						try {
							val = new BigDecimal((BigInteger)x, scale);
						} catch (NumberFormatException e) {
							try {
								val = new BigDecimal(0.0);
							} catch (NumberFormatException ex) {
								throw new SQLException("Internal error: unable to create template BigDecimal: " + ex.getMessage());
							}
						}
						setBigDecimal(parameterIndex, val);
					} else {
						BigDecimal val;
						try {
							val = new BigDecimal(num.doubleValue());
						} catch (NumberFormatException e) {
							try {
								val = new BigDecimal(0.0);
							} catch (NumberFormatException ex) {
								throw new SQLException("Internal error: unable to create template BigDecimal: " + ex.getMessage());
							}
						}
						setBigDecimal(parameterIndex, val);
					}
				break;
				case Types.BIT:
				case Types.BOOLEAN:
					if (num.doubleValue() != 0.0) {
						setBoolean(parameterIndex, true);
					} else {
						setBoolean(parameterIndex, false);
					}
				break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
					setString(parameterIndex, x.toString());
				break;
				default:
					throw new SQLException("Conversion not allowed");
			}
		} else if (x instanceof Boolean) {
			boolean val = ((Boolean)x).booleanValue();
			switch (targetSqlType) {
				case Types.TINYINT:
					setByte(parameterIndex, (byte)(val ? 1 : 0));
				break;
				case Types.SMALLINT:
					setShort(parameterIndex, (short)(val ? 1 : 0));
				break;
				case Types.INTEGER:
					setInt(parameterIndex, (int)(val ? 1 : 0));
				break;
				case Types.BIGINT:
					setLong(parameterIndex, (long)(val ? 1 : 0));
				break;
				case Types.REAL:
					setFloat(parameterIndex, (float)(val ? 1.0 : 0.0));
				break;
				case Types.FLOAT:
				case Types.DOUBLE:
					setDouble(parameterIndex, (double)(val ? 1.0 : 0.0));
				break;
				case Types.DECIMAL:
				case Types.NUMERIC:
				{
					BigDecimal dec;
					try {
						dec = new BigDecimal(val ? 1.0 : 0.0);
					} catch (NumberFormatException e) {
						throw new SQLException("Internal error: unable to create template BigDecimal: " + e.getMessage());
					}
					setBigDecimal(parameterIndex, dec);
				} break;
				case Types.BIT:
				case Types.BOOLEAN:
					setBoolean(parameterIndex, val);
				break;
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
					setString(parameterIndex, "" + val);
				break;
				default:
					throw new SQLException("Conversion not allowed");
			}
		} else if (x instanceof byte[]) {
			switch (targetSqlType) {
				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
					setBytes(parameterIndex, (byte[])x);
				break;
				default:
					throw new SQLException("Conversion not allowed");
			}
		} else if (x instanceof java.sql.Date ||
				x instanceof Timestamp ||
				x instanceof Time)
		{
			switch (targetSqlType) {
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
					setString(parameterIndex, x.toString());
				break;
				case Types.DATE:
					if (x instanceof Time) {
						throw new SQLException("Conversion not allowed");
					} else if (x instanceof java.sql.Date) {
						setDate(parameterIndex, (java.sql.Date)x);
					} else if (x instanceof Timestamp) {
						setDate(parameterIndex, new java.sql.Date(((Timestamp)x).getTime()));
					}
				break;
				case Types.TIME:
					if (x instanceof Time) {
						setTime(parameterIndex, (Time)x);
					} else if (x instanceof java.sql.Date) {
						throw new SQLException("Conversion not allowed");
					} else if (x instanceof Timestamp) {
						setTime(parameterIndex, new Time(((Timestamp)x).getTime()));
					}
				break;
				case Types.TIMESTAMP:
					if (x instanceof Time) {
						throw new SQLException("Conversion not allowed");
					} else if (x instanceof java.sql.Date) {
						setTimestamp(parameterIndex, new Timestamp(((java.sql.Date)x).getTime()));
					} else if (x instanceof Timestamp) {
						setTimestamp(parameterIndex, (Timestamp)x);
					}
				break;
				default:
					throw new SQLException("Conversion not allowed");
			}
		} else if (x instanceof Array) {
			setArray(parameterIndex, (Array)x);
		} else if (x instanceof Blob) {
			setBlob(parameterIndex, (Blob)x);
		} else if (x instanceof Clob) {
			setClob(parameterIndex, (Clob)x);
		} else if (x instanceof Struct) {
			// I have no idea how to do this...
			throw new SQLException("Operation setObject() with object of type Struct currently not supported!");
		} else if (x instanceof Ref) {
			setRef(parameterIndex, (Ref)x);
		} else if (x instanceof java.net.URL) {
			setURL(parameterIndex, (java.net.URL)x);
		} else if (x instanceof SQLData) {
			// do something with:
			// ((SQLData)x).writeSQL( [java.sql.SQLOutput] );
			// needs an SQLOutput stream... bit too far away from reality
			throw new SQLException("Operation setObject() with object of type SQLData currently not supported!");
		} else {	// java Class
			throw new SQLException("Operation setObject() with object of type Class currently not supported!");
		}
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
		throw new SQLException("Operation setRef(int i, Ref x) currently not supported!");
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
	 * Sets the designated parameter to the given java.sql.Time value.
	 * The driver converts this to an SQL TIME value when it sends it to
	 * the database.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setTime(int index, Time x) throws SQLException {
		if (index < 1 || index > size)
			throw new SQLException("No such parameter with index: " + index);

		setValue(index, monetdbType[index - 1] + " '" + x.toString() + "'");
	}

	/**
	 * Sets the designated parameter to the given java.sql.Time value,
	 * using the given Calendar object.  The driver uses the Calendar
	 * object to construct an SQL TIME value, which the driver then
	 * sends to the database.  With a Calendar object, the driver can
	 * calculate the time taking into account a custom timezone.  If no
	 * Calendar object is specified, the driver uses the default
	 * timezone, which is that of the virtual machine running the
	 * application.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the time
	 * @throws SQLException if a database access error occurs
	 */
	public void setTime(int index, Time x, Calendar cal)
		throws SQLException
	{
		if (index < 1 || index > size)
			throw new SQLException("No such parameter with index: " + index);

		mTimeZ.setTimeZone(cal.getTimeZone());

		String RFC822 = mTimeZ.format(x);
		setValue(index, monetdbType[index - 1] + " '" +
				RFC822.substring(0, 15) + ":" + RFC822.substring(15) + "'");
	}

	/**
	 * Sets the designated parameter to the given java.sql.Timestamp
	 * value.  The driver converts this to an SQL TIMESTAMP value when
	 * it sends it to the database.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @throws SQLException if a database access error occurs
	 */
	public void setTimestamp(int index, Timestamp x)
		throws SQLException
	{
		if (index < 1 || index > size)
			throw new SQLException("No such parameter with index: " + index);

		setValue(index, monetdbType[index - 1] + " '" + x.toString() + "'");
	}

    /**
	 * Sets the designated parameter to the given java.sql.Timestamp
	 * value, using the given Calendar object.  The driver uses the
	 * Calendar object to construct an SQL TIMESTAMP value, which the
	 * driver then sends to the database.  With a Calendar object, the
	 * driver can calculate the timestamp taking into account a custom
	 * timezone.  If no Calendar object is specified, the driver uses the
	 * default timezone, which is that of the virtual machine running
	 * the application.
	 *
	 * @param index the first parameter is 1, the second is 2, ...
	 * @param x the parameter value
	 * @param cal the Calendar object the driver will use to construct the
	 *            timestamp
	 * @throws SQLException if a database access error occurs
	 */
	public void setTimestamp(int index, Timestamp x, Calendar cal)
		throws SQLException
	{
		if (index < 1 || index > size)
			throw new SQLException("No such parameter with index: " + index);

		if (cal == null) cal = Calendar.getInstance();
		mTimestampZ.setTimeZone(cal.getTimeZone());
		String RFC822 = mTimestampZ.format(x);
		setValue(index, monetdbType[index - 1] + " '" +
				RFC822.substring(0, 26) + ":" + RFC822.substring(26) + "'");
	}

	/**
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
	 * @deprecated
	 * @param parameterIndex the first parameter is 1, the second is 2, ...
	 * @param x a java.io.InputStream object that contains the Unicode
	 *          parameter value as two-byte Unicode characters
	 * @param length the number of bytes in the stream
	 * @throws SQLException if a database access error occurs
	 */
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
		throws SQLException
	{
		throw new SQLException("Operation setUnicodeStream(int parameterIndex, InputStream x, int length) currently not supported!");
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
		throw new SQLException("Operation setURL(int parameterIndex, URL x) currently not supported!");
	}

	/**
	 * Releases this PreparedStatement object's database and JDBC
	 * resources immediately instead of waiting for this to happen when
	 * it is automatically closed.  It is generally good practice to
	 * release resources as soon as you are finished with them to avoid
	 * tying up database resources.
	 * <br /><br />
	 * Calling the method close on a PreparedStatement object that is
	 * already closed has no effect.
	 * <br /><br />
	 * <b>Note:</b> A PreparedStatement object is automatically closed
	 * when it is garbage collected. When a Statement object is closed,
	 * its current ResultSet object, if one exists, is also closed. 
	 */
	public void close() {
		try {
			if (!closed && id != -1)
				connection.sendControlCommand("release " + id);
		} catch (SQLException e) {
			// probably server closed connection
		}
		super.close();
	}

	/**
	 * Call close to release the server-sided handle for this
	 * PreparedStatement.
	 */
	protected void finalize() {
		close();
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
	void setValue(int index, String val) throws SQLException {
		if (index < 1 || index > size)
			throw new SQLException("No such parameter with index: " + index);

		values[index - 1] = val;
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
		buf.delete(0, buf.length());
		buf.append("exec ");
		buf.append(id);
		buf.append("(");
		// check if all columns are set and do a replace
		for (int i = 0; i < size; i++) {
			if (i > 0) buf.append(", ");
			if (values[i] == null) throw
				new SQLException("Cannot execute, parameter " +  (i + 1) + " is missing.");

			buf.append(values[i]);
		}
		buf.append(")");
		
		return(buf.toString());
	}
}
