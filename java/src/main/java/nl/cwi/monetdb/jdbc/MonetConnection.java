/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import nl.cwi.monetdb.jdbc.types.INET;
import nl.cwi.monetdb.jdbc.types.URL;
import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.cwi.monetdb.mcl.parser.HeaderLineParser;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.cwi.monetdb.mcl.parser.StartOfHeaderParser;

/**
 * A {@link Connection} suitable for the MonetDB database.
 * 
 * This connection represents a connection (session) to a MonetDB
 * database. SQL statements are executed and results are returned within
 * the context of a connection. This Connection object holds a physical
 * connection to the MonetDB database.
 * 
 * A Connection object's database should able to provide information
 * describing its tables, its supported SQL grammar, its stored
 * procedures, the capabilities of this connection, and so on. This
 * information is obtained with the getMetaData method.
 * 
 * Note: By default a Connection object is in auto-commit mode, which
 * means that it automatically commits changes after executing each
 * statement. If auto-commit mode has been disabled, the method commit
 * must be called explicitly in order to commit changes; otherwise,
 * database changes will not be saved.
 * 
 * The current state of this connection is that it nearly implements the
 * whole Connection interface.
 *
 * @author Fabian Groffen
 * @version 1.2
 */
public class MonetConnection extends MonetWrapper implements Connection {
	/** The hostname to connect to */
	private final String hostname;
	/** The port to connect on the host to */
	private final int port;
	/** The database to use (currently not used) */
	private final String database;
	/** The username to use when authenticating */
	private final String username;
	/** The password to use when authenticating */
	private final String password;
	/** A connection to mserver5 using a TCP socket */
	private final MapiSocket server;
	/** The Reader from the server */
	private final BufferedMCLReader in;
	/** The Writer to the server */
	private final BufferedMCLWriter out;

	/** A StartOfHeaderParser  declared for reuse. */
	private StartOfHeaderParser sohp = new StartOfHeaderParser();

	/** Whether this Connection is closed (and cannot be used anymore) */
	private boolean closed;

	/** Whether this Connection is in autocommit mode */
	private boolean autoCommit = true;

	/** The stack of warnings for this Connection object */
	private SQLWarning warnings = null;
	/** The Connection specific mapping of user defined types to Java
	 * types */
	private Map<String,Class<?>> typeMap = new HashMap<String,Class<?>>() {/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	{
			put("inet", INET.class);
			put("url",  URL.class);
	}};

	// See javadoc for documentation about WeakHashMap if you don't know what
	// it does !!!NOW!!! (only when you deal with it of course)
	/** A Map containing all (active) Statements created from this Connection */
	private Map<Statement,?> statements = new WeakHashMap<Statement, Object>();

	/** The number of results we receive from the server at once */
	private int curReplySize = -1;	// the server by default uses -1 (all)

	/** A template to apply to each query (like pre and post fixes) */
	String[] queryTempl;
	/** A template to apply to each command (like pre and post fixes) */
	String[] commandTempl;

	/** the SQL language */
	final static int LANG_SQL = 0;
	/** the MAL language (officially *NOT* supported) */
	final static int LANG_MAL = 3;
	/** an unknown language */
	final static int LANG_UNKNOWN = -1;
	/** The language which is used */
	final int lang;

	/** Whether or not BLOB is mapped to BINARY within the driver */
	private final boolean blobIsBinary;

	/**
	 * Constructor of a Connection for MonetDB. At this moment the
	 * current implementation limits itself to storing the given host,
	 * database, username and password for later use by the
	 * createStatement() call.  This constructor is only accessible to
	 * classes from the jdbc package.
	 *
	 * @param props a Property hashtable holding the properties needed for
	 *              connecting
	 * @throws SQLException if a database error occurs
	 * @throws IllegalArgumentException is one of the arguments is null or empty
	 */
	MonetConnection(Properties props)
		throws SQLException, IllegalArgumentException
	{
		this.hostname = props.getProperty("host");
		int port;
		try {
			port = Integer.parseInt(props.getProperty("port"));
		} catch (NumberFormatException e) {
			port = 0;
		}
		this.port = port;
		this.database = props.getProperty("database");
		this.username = props.getProperty("user");
		this.password = props.getProperty("password");
		String language = props.getProperty("language");
		boolean debug = Boolean.valueOf(props.getProperty("debug")).booleanValue();
		String hash = props.getProperty("hash");
		blobIsBinary = Boolean.valueOf(props.getProperty("treat_blob_as_binary")).booleanValue();
		int sockTimeout = 0;
		try {
			sockTimeout = Integer.parseInt(props.getProperty("so_timeout"));
		} catch (NumberFormatException e) {
			sockTimeout = 0;
		}
		// check input arguments
		if (hostname == null || hostname.trim().equals(""))
			throw new IllegalArgumentException("hostname should not be null or empty");
		if (port == 0)
			throw new IllegalArgumentException("port should not be 0");
		if (username == null || username.trim().equals(""))
			throw new IllegalArgumentException("user should not be null or empty");
		if (password == null || password.trim().equals(""))
			throw new IllegalArgumentException("password should not be null or empty");
		if (language == null || language.trim().equals("")) {
			language = "sql";
			addWarning("No language given, defaulting to 'sql'", "M1M05");
		}

		// initialise query templates (filled later, but needed below)
		queryTempl = new String[3]; // pre, post, sep
		commandTempl = new String[3]; // pre, post, sep

		server = new MapiSocket();

		if (hash != null) server.setHash(hash);
		if (database != null) server.setDatabase(database);
		server.setLanguage(language);

		// we're debugging here... uhm, should be off in real life
		if (debug) {
			try {
				String fname = props.getProperty("logfile", "monet_" +
					System.currentTimeMillis() + ".log");
				File f = new File(fname);
				int ext = fname.lastIndexOf(".");
				if (ext < 0) ext = fname.length();
				String pre = fname.substring(0, ext);
				String suf = fname.substring(ext);

				for (int i = 1; f.exists(); i++) {
					f = new File(pre + "-" + i + suf);
				}

				server.debug(f.getAbsolutePath());
			} catch (IOException ex) {
				throw new SQLException("Opening logfile failed: " + ex.getMessage(), "08M01");
			}
		}

		try {
			List<String> warnings = 
				server.connect(hostname, port, username, password);
			for (String warning : warnings) {
				addWarning(warning, "01M02");
			}
			
			// apply NetworkTimeout value from legacy (pre 4.1) driver
			// so_timeout calls
			server.setSoTimeout(sockTimeout);

			in = server.getReader();
			out = server.getWriter();

			String error = in.waitForPrompt();
			if (error != null)
				throw new SQLException(error.substring(6), "08001");
		} catch (IOException e) {
			throw new SQLException("Unable to connect (" + hostname + ":" + port + "): " + e.getMessage(), "08006");
		} catch (MCLParseException e) {
			throw new SQLException(e.getMessage(), "08001");
		} catch (MCLException e) {
			String[] connex = e.getMessage().split("\n");
			SQLException sqle = new SQLException(connex[0], "08001", e);
			for (int i = 1; i < connex.length; i++) {
				sqle.setNextException(new SQLException(connex[1], "08001"));
			}
			throw sqle;
		}

		// we seem to have managed to log in, let's store the
		// language used
		if ("sql".equals(language)) {
			lang = LANG_SQL;
		} else if ("mal".equals(language)) {
			lang = LANG_MAL;
		} else {
			lang = LANG_UNKNOWN;
		}
		
		// fill the query templates
		if (lang == LANG_SQL) {
			queryTempl[0] = "s";		// pre
			queryTempl[1] = "\n;";		// post
			queryTempl[2] = "\n;\n";	// separator

			commandTempl[0] = "X";		// pre
			commandTempl[1] = null;		// post
			commandTempl[2] = "\nX";	// separator
		} else if (lang == LANG_MAL) {
			queryTempl[0] = null;
			queryTempl[1] = ";\n";
			queryTempl[2] = ";\n";

			commandTempl[0] = null;		// pre
			commandTempl[1] = null;		// post
			commandTempl[2] = null;		// separator
		}

		// the following initialisers are only valid when the language
		// is SQL...
		if (lang == LANG_SQL) {
			// enable auto commit
			setAutoCommit(true);
			// set our time zone on the server
			Calendar cal = Calendar.getInstance();
			int offset = cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET);
			offset /= (60 * 1000); // milliseconds to minutes
			String tz = offset < 0 ? "-" : "+";
			tz += (Math.abs(offset) / 60 < 10 ? "0" : "") + (Math.abs(offset) / 60) + ":";
			offset -= (offset / 60) * 60;
			tz += (offset < 10 ? "0" : "") + offset;
			sendIndependentCommand("SET TIME ZONE INTERVAL '" + tz + "' HOUR TO MINUTE");
		}

		// we're absolutely not closed, since we're brand new
		closed = false;
	}

	//== methods of interface Connection

	/**
	 * Clears all warnings reported for this Connection object. After a
	 * call to this method, the method getWarnings returns null until a
	 * new warning is reported for this Connection object.
	 */
	@Override
	public void clearWarnings() {
		warnings = null;
	}

	/**
	 * Releases this Connection object's database and JDBC resources
	 * immediately instead of waiting for them to be automatically
	 * released. All Statements created from this Connection will be
	 * closed when this method is called.
	 * 
	 * Calling the method close on a Connection object that is already
	 * closed is a no-op.
	 */
	@Override
	public void close() {
		synchronized (server) {
			for (Statement st : statements.keySet()) {
				try {
					st.close();
				} catch (SQLException e) {
					// better luck next time!
				}
			}
			// close the socket
			server.close();
			// close active SendThread if any
			if (sendThread != null) {
				sendThread.shutdown();
				sendThread = null;
			}
			// report ourselves as closed
			closed = true;
		}
	}

	/**
	 * Makes all changes made since the previous commit/rollback
	 * permanent and releases any database locks currently held by this
	 * Connection object.  This method should be used only when
	 * auto-commit mode has been disabled.
	 *
	 * @throws SQLException if a database access error occurs or this
	 *         Connection object is in auto-commit mode
	 * @see #setAutoCommit(boolean)
	 */
	@Override
	public void commit() throws SQLException {
		// note: can't use sendIndependentCommand here because we need
		// to process the auto_commit state the server gives

		// create a container for the result
		ResponseList l = new ResponseList(
			0,
			0,
			ResultSet.FETCH_FORWARD,
			ResultSet.CONCUR_READ_ONLY
		);
		// send commit to the server
		try {
			l.processQuery("COMMIT");
		} finally {
			l.close();
		}
	}

	/**
	 * Factory method for creating Array objects.
	 * 
	 * Note: When createArrayOf is used to create an array object that
	 * maps to a primitive data type, then it is implementation-defined
	 * whether the Array object is an array of that primitive data type
	 * or an array of Object.
	 * 
	 * Note: The JDBC driver is responsible for mapping the elements
	 * Object array to the default JDBC SQL type defined in
	 * java.sql.Types for the given class of Object. The default mapping
	 * is specified in Appendix B of the JDBC specification. If the
	 * resulting JDBC type is not the appropriate type for the given
	 * typeName then it is implementation defined whether an
	 * SQLException is thrown or the driver supports the resulting
	 * conversion. 
	 *
	 * @param typeName the SQL name of the type the elements of the
	 *        array map to. The typeName is a database-specific name
	 *        which may be the name of a built-in type, a user-defined
	 *        type or a standard SQL type supported by this database.
	 *        This is the value returned by Array.getBaseTypeName
	 * @return an Array object whose elements map to the specified SQL
	 *         type
	 * @throws SQLException if a database error occurs, the JDBC type
	 *         is not appropriate for the typeName and the conversion is
	 *         not supported, the typeName is null or this method is
	 *         called on a closed connection
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this data type
	 */
	@Override
	public Array createArrayOf(String typeName, Object[] elements)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("createArrayOf(String, Object[]) not supported", "0A000");
	}

	/**
	 * Creates a Statement object for sending SQL statements to the
	 * database.  SQL statements without parameters are normally
	 * executed using Statement objects. If the same SQL statement is
	 * executed many times, it may be more efficient to use a
	 * PreparedStatement object.
	 * 
	 * Result sets created using the returned Statement object will by
	 * default be type TYPE_FORWARD_ONLY and have a concurrency level of
	 * CONCUR_READ_ONLY.
	 *
	 * @return a new default Statement object
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public Statement createStatement() throws SQLException {
		return createStatement(
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY,
					ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}

	/**
	 * Creates a Statement object that will generate ResultSet objects
	 * with the given type and concurrency. This method is the same as
	 * the createStatement method above, but it allows the default
	 * result set type and concurrency to be overridden.
	 *
	 * @param resultSetType a result set type; one of
	 *        ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
	 *        or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param resultSetConcurrency a concurrency type; one of
	 *        ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
	 * @return a new Statement object that will generate ResultSet objects with
	 *         the given type and concurrency
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public Statement createStatement(
			int resultSetType,
			int resultSetConcurrency)
		throws SQLException
	{
		return createStatement(
					resultSetType,
					resultSetConcurrency,
					ResultSet.HOLD_CURSORS_OVER_COMMIT);
	}

	/**
	 * Creates a Statement object that will generate ResultSet objects
	 * with the given type, concurrency, and holdability.  This method
	 * is the same as the createStatement method above, but it allows
	 * the default result set type, concurrency, and holdability to be
	 * overridden.
	 *
	 * @param resultSetType one of the following ResultSet constants:
	 * ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
	 * or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param resultSetConcurrency one of the following ResultSet
	 * constants: ResultSet.CONCUR_READ_ONLY or
	 * ResultSet.CONCUR_UPDATABLE
	 * @param resultSetHoldability one of the following ResultSet
	 * constants: ResultSet.HOLD_CURSORS_OVER_COMMIT or
	 * ResultSet.CLOSE_CURSORS_AT_COMMIT 
	 *
	 * @return a new Statement      object that will generate ResultSet
	 * objects with the given type, concurrency, and holdability 
	 * @throws SQLException if a database access error occurs or the
	 * given parameters are not ResultSet constants indicating type,
	 * concurrency, and holdability
	 */
	@Override
	public Statement createStatement(
			int resultSetType,
			int resultSetConcurrency,
			int resultSetHoldability)
		throws SQLException
	{
		try {
			Statement ret =
				new MonetStatement(
					this,
					resultSetType,
					resultSetConcurrency,
					resultSetHoldability
				);
			// store it in the map for when we close...
			statements.put(ret, null);
			return ret;
		} catch (IllegalArgumentException e) {
			throw new SQLException(e.toString(), "M0M03");
		}
		// we don't have to catch SQLException because that is declared to
		// be thrown
	}

	/**
	 * Constructs an object that implements the Clob interface. The
	 * object returned initially contains no data. The setAsciiStream,
	 * setCharacterStream and setString methods of the Clob interface
	 * may be used to add data to the Clob.
	 *
	 * @return a MonetClob instance
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support MonetClob objects that can be filled in
	 */
	@Override
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException("createClob() not supported", "0A000");
	}

	/**
	 * Constructs an object that implements the Blob interface. The
	 * object returned initially contains no data. The setBinaryStream
	 * and setBytes methods of the Blob interface may be used to add
	 * data to the Blob.
	 *
	 * @return a MonetBlob instance
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support MonetBlob objects that can be filled in
	 */
	@Override
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException("createBlob() not supported", "0A000");
	}

	/**
	 * Constructs an object that implements the NClob interface. The
	 * object returned initially contains no data. The setAsciiStream,
	 * setCharacterStream and setString methods of the NClob interface
	 * may be used to add data to the NClob.
	 *
	 * @return an NClob instance
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support MonetClob objects that can be filled in
	 */
	@Override
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException("createNClob() not supported", "0A000");
	}

	/**
	 * Factory method for creating Struct objects.
	 *
	 * @param typeName the SQL type name of the SQL structured type that
	 *        this Struct object maps to. The typeName is the name of a
	 *        user-defined type that has been defined for this database.
	 *        It is the value returned by Struct.getSQLTypeName.
	 * @param attributes the attributes that populate the returned
	 *        object
	 * @return a Struct object that maps to the given SQL type and is
	 *         populated with the given attributes
	 * @throws SQLException if a database error occurs, the typeName
	 *         is null or this method is called on a closed connection
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this data type
	 */
	@Override
	public Struct createStruct(String typeName, Object[] attributes)
		throws SQLException
	{
		throw new SQLFeatureNotSupportedException("createStruct() not supported", "0A000");
	}

	/**
	 * Constructs an object that implements the SQLXML interface. The
	 * object returned initially contains no data. The
	 * createXmlStreamWriter object and setString method of the SQLXML
	 * interface may be used to add data to the SQLXML object.
	 *
	 * @return An object that implements the SQLXML interface
	 * @throws SQLFeatureNotSupportedException the JDBC driver does
	 *         not support this data type
	 */
	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException("createSQLXML() not supported", "0A000");
	}

	/**
	 * Retrieves the current auto-commit mode for this Connection
	 * object.
	 *
	 * @return the current state of this Connection object's auto-commit
	 *         mode
	 * @see #setAutoCommit(boolean)
	 */
	@Override
	public boolean getAutoCommit() throws SQLException {
		return autoCommit;
	}

	/**
	 * Retrieves this Connection object's current catalog name.
	 *
	 * @return the current catalog name or null if there is none
	 * @throws SQLException if a database access error occurs or the
	 *         current language is not SQL
	 */
	@Override
	public String getCatalog() throws SQLException {
		if (lang != LANG_SQL)
			throw new SQLException("This method is only supported in SQL mode", "M0M04");

		// this is a dirty hack, but it works as long as MonetDB
		// only handles one catalog (dbfarm) at a time
		ResultSet rs = getMetaData().getCatalogs();
		try {
			return rs.next() ? rs.getString(1) : null;
		} finally {
			rs.close();
		}
	}
	
	/**
	 * Not implemented by MonetDB's JDBC driver.
	 *
	 * @param name The name of the client info property to retrieve
	 * @return The value of the client info property specified
	 */
	@Override
	public String getClientInfo(String name) {
		// This method will also return null if the specified client
		// info property name is not supported by the driver.
		return null;
	}

	/**
	 * Not implemented by MonetDB's JDBC driver.
	 *
	 * @return A Properties object that contains the name and current
	 *         value of each of the client info properties supported by
	 *         the driver.
	 */
	@Override
	public Properties getClientInfo() {
		return new Properties();
	}

	/**
	 * Retrieves the current holdability of ResultSet objects created
	 * using this Connection object.
	 *
	 * @return the holdability, one of
	 *         ResultSet.HOLD_CURSORS_OVER_COMMIT or
	 *         ResultSet.CLOSE_CURSORS_AT_COMMIT
	 */
	@Override
	public int getHoldability() {
		// TODO: perhaps it is better to have the server implement
		//       CLOSE_CURSORS_AT_COMMIT
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	/**
	 * Retrieves a DatabaseMetaData object that contains metadata about
	 * the database to which this Connection object represents a
	 * connection. The metadata includes information about the
	 * database's tables, its supported SQL grammar, its stored
	 * procedures, the capabilities of this connection, and so on.
	 *
	 * @throws SQLException if the current language is not SQL
	 * @return a DatabaseMetaData object for this Connection object
	 */
	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		if (lang != LANG_SQL)
			throw new SQLException("This method is only supported in SQL mode", "M0M04");

		return new MonetDatabaseMetaData(this);
	}

	/**
	 * Retrieves this Connection object's current transaction isolation
	 * level.
	 *
	 * @return the current transaction isolation level, which will be
	 *         Connection.TRANSACTION_SERIALIZABLE
	 */
	@Override
	public int getTransactionIsolation() {
		return TRANSACTION_SERIALIZABLE;
	}

	/**
	 * Retrieves the Map object associated with this Connection object.
	 * Unless the application has added an entry, the type map returned
	 * will be empty.
	 *
	 * @return the java.util.Map object associated with this Connection
	 *         object
	 */
	@Override
	public Map<String,Class<?>> getTypeMap() {
		return typeMap;
	}

	/**
	 * Retrieves the first warning reported by calls on this Connection
	 * object.  If there is more than one warning, subsequent warnings
	 * will be chained to the first one and can be retrieved by calling
	 * the method SQLWarning.getNextWarning on the warning that was
	 * retrieved previously.
	 * 
	 * This method may not be called on a closed connection; doing so
	 * will cause an SQLException to be thrown.
	 * 
	 * Note: Subsequent warnings will be chained to this SQLWarning.
	 *
	 * @return the first SQLWarning object or null if there are none
	 * @throws SQLException if a database access error occurs or this method is
	 *         called on a closed connection
	 */
	@Override
	public SQLWarning getWarnings() throws SQLException {
		if (closed)
			throw new SQLException("Cannot call on closed Connection", "M1M20");

		// if there are no warnings, this will be null, which fits with the
		// specification.
		return warnings;
	}

	/**
	 * Retrieves whether this Connection object has been closed.  A
	 * connection is closed if the method close has been called on it or
	 * if certain fatal errors have occurred.  This method is guaranteed
	 * to return true only when it is called after the method
	 * Connection.close has been called.
	 * 
	 * This method generally cannot be called to determine whether a
	 * connection to a database is valid or invalid.  A typical client
	 * can determine that a connection is invalid by catching any
	 * exceptions that might be thrown when an operation is attempted.
	 *
	 * @return true if this Connection object is closed; false if it is
	 *         still open
	 */
	@Override
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Retrieves whether this Connection object is in read-only mode.
	 * MonetDB currently doesn't support updateable result sets, but
	 * updates are possible.  Hence the Connection object is never in
	 * read-only mode.
	 *
	 * @return true if this Connection object is read-only; false otherwise
	 */
	@Override
	public boolean isReadOnly() {
		return false;
	}

	/**
	 * Returns true if the connection has not been closed and is still
	 * valid. The driver shall submit a query on the connection or use
	 * some other mechanism that positively verifies the connection is
	 * still valid when this method is called.
	 * 
	 * The query submitted by the driver to validate the connection
	 * shall be executed in the context of the current transaction.
	 *
	 * @param timeout The time in seconds to wait for the database
	 *        operation used to validate the connection to complete. If
	 *        the timeout period expires before the operation completes,
	 *        this method returns false. A value of 0 indicates a
	 *        timeout is not applied to the database operation.
	 * @return true if the connection is valid, false otherwise
	 * @throws SQLException if the value supplied for timeout is less
	 *         than 0
	 */
	@Override
	public boolean isValid(int timeout) throws SQLException {
		if (timeout < 0)
			throw new SQLException("timeout is less than 0", "M1M05");
		if (closed)
			return false;
		// ping db using select 1;
		try {
			Statement stmt = createStatement();
			stmt.executeQuery("SELECT 1");
			stmt.close();
			return true;
		} catch (SQLException e) {
			// close this connection
			close();
		}
		return false;
	}

	@Override
	public String nativeSQL(String sql) {return sql;}
	@Override
	public CallableStatement prepareCall(String sql) {return null;}
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {return null;}
	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {return null;}

	/**
	 * Creates a PreparedStatement object for sending parameterized SQL
	 * statements to the database.
	 * 
	 * A SQL statement with or without IN parameters can be pre-compiled
	 * and stored in a PreparedStatement object. This object can then be
	 * used to efficiently execute this statement multiple times.
	 * 
	 * Note: This method is optimized for handling parametric SQL
	 * statements that benefit from precompilation. If the driver
	 * supports precompilation, the method prepareStatement will send
	 * the statement to the database for precompilation. Some drivers
	 * may not support precompilation. In this case, the statement may
	 * not be sent to the database until the PreparedStatement object is
	 * executed. This has no direct effect on users; however, it does
	 * affect which methods throw certain SQLException objects.
	 * 
	 * Result sets created using the returned PreparedStatement object
	 * will by default be type TYPE_FORWARD_ONLY and have a concurrency
	 * level of CONCUR_READ_ONLY.
	 *
	 * @param sql an SQL statement that may contain one or more '?' IN
	 *        parameter placeholders
	 * @return a new default PreparedStatement object containing the
	 *         pre-compiled SQL statement
	 * @throws SQLException if a database access error occurs
	 */
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return prepareStatement(
					sql,
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY,
					ResultSet.HOLD_CURSORS_OVER_COMMIT
		);
	}

	/**
	 * Creates a PreparedStatement object that will generate ResultSet
	 * objects with the given type and concurrency.  This method is the
	 * same as the prepareStatement method above, but it allows the
	 * default result set type and concurrency to be overridden.
	 *
	 * @param sql a String object that is the SQL statement to be sent to the
	 *            database; may contain one or more ? IN parameters
	 * @param resultSetType a result set type; one of
	 *        ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
	 *        or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param resultSetConcurrency a concurrency type; one of
	 *        ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
	 * @return a new PreparedStatement object containing the pre-compiled SQL
	 *         statement that will produce ResultSet objects with the given
	 *         type and concurrency
	 * @throws SQLException if a database access error occurs or the given
	 *                      parameters are not ResultSet constants indicating
	 *                      type and concurrency
	 */
	@Override
	public PreparedStatement prepareStatement(
			String sql,
			int resultSetType,
			int resultSetConcurrency)
		throws SQLException
	{
		return prepareStatement(
					sql,
					resultSetType,
					resultSetConcurrency,
					ResultSet.HOLD_CURSORS_OVER_COMMIT
		);
	}

	/**
	 * Creates a PreparedStatement object that will generate ResultSet
	 * objects with the given type, concurrency, and holdability.
	 * 
	 * This method is the same as the prepareStatement method above, but
	 * it allows the default result set type, concurrency, and
	 * holdability to be overridden.
	 *
	 * @param sql a String object that is the SQL statement to be sent
	 * to the database; may contain one or more ? IN parameters
	 * @param resultSetType one of the following ResultSet constants:
	 * ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
	 * or ResultSet.TYPE_SCROLL_SENSITIVE
	 * @param resultSetConcurrency one of the following ResultSet
	 * constants: ResultSet.CONCUR_READ_ONLY or
	 * ResultSet.CONCUR_UPDATABLE
	 * @param resultSetHoldability one of the following ResultSet
	 * constants: ResultSet.HOLD_CURSORS_OVER_COMMIT or
	 * ResultSet.CLOSE_CURSORS_AT_COMMIT 
	 * @return a new PreparedStatement object, containing the
	 * pre-compiled SQL statement, that will generate ResultSet objects
	 * with the given type, concurrency, and holdability 
	 * @throws SQLException if a database access error occurs or the
	 * given parameters are not ResultSet constants indicating type,
	 * concurrency, and holdability
	 */
	@Override
	public PreparedStatement prepareStatement(
			String sql,
			int resultSetType,
			int resultSetConcurrency,
			int resultSetHoldability)
		throws SQLException
	{
		try {
			PreparedStatement ret = new MonetPreparedStatement(
				this,
				resultSetType,
				resultSetConcurrency,
				resultSetHoldability,
				sql
			);
			// store it in the map for when we close...
			statements.put(ret, null);
			return ret;
		} catch (IllegalArgumentException e) {
			throw new SQLException(e.toString(), "M0M03");
		}
		// we don't have to catch SQLException because that is declared to
		// be thrown
	}

	/**
	 * Creates a default PreparedStatement object that has the
	 * capability to retrieve auto-generated keys.  The given constant
	 * tells the driver whether it should make auto-generated keys
	 * available for retrieval.  This parameter is ignored if the SQL
	 * statement is not an INSERT statement.
	 * 
	 * Note: This method is optimized for handling parametric SQL
	 * statements that benefit from precompilation.  If the driver
	 * supports precompilation, the method prepareStatement will send
	 * the statement to the database for precompilation. Some drivers
	 * may not support precompilation.  In this case, the statement may
	 * not be sent to the database until the PreparedStatement object is
	 * executed.  This has no direct effect on users; however, it does
	 * affect which methods throw certain SQLExceptions.
	 * 
	 * Result sets created using the returned PreparedStatement object
	 * will by default be type TYPE_FORWARD_ONLY and have a concurrency
	 * level of CONCUR_READ_ONLY.
	 *
	 * @param sql an SQL statement that may contain one or more '?' IN
	 *        parameter placeholders
	 * @param autoGeneratedKeys a flag indicating whether auto-generated
	 *        keys should be returned; one of
	 *        Statement.RETURN_GENERATED_KEYS or
	 *        Statement.NO_GENERATED_KEYS
	 * @return a new PreparedStatement object, containing the
	 *         pre-compiled SQL statement, that will have the capability
	 *         of returning auto-generated keys
	 * @throws SQLException - if a database access error occurs or the
	 *         given parameter is not a Statement  constant indicating
	 *         whether auto-generated keys should be returned
	 */
	@Override
	public PreparedStatement prepareStatement(
			String sql,
			int autoGeneratedKeys)
		throws SQLException
	{
		if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS &&
				autoGeneratedKeys != Statement.NO_GENERATED_KEYS)
			throw new SQLException("Invalid argument, expected RETURN_GENERATED_KEYS or NO_GENERATED_KEYS", "M1M05");
		
		/* MonetDB has no way to disable this, so just do the normal
		 * thing ;) */
		return prepareStatement(
					sql,
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY
		);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {return null;}
	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) {return null;}

	/**
	 * Removes the given Savepoint object from the current transaction.
	 * Any reference to the savepoint after it have been removed will
	 * cause an SQLException to be thrown.
	 *
	 * @param savepoint the Savepoint object to be removed
	 * @throws SQLException if a database access error occurs or the given
	 *         Savepoint object is not a valid savepoint in the current
	 *         transaction
	 */
	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		if (!(savepoint instanceof MonetSavepoint)) throw
			new SQLException("This driver can only handle savepoints it created itself", "M0M06");

		MonetSavepoint sp = (MonetSavepoint)savepoint;

		// note: can't use sendIndependentCommand here because we need
		// to process the auto_commit state the server gives

		// create a container for the result
		ResponseList l = new ResponseList(
			0,
			0,
			ResultSet.FETCH_FORWARD,
			ResultSet.CONCUR_READ_ONLY
		);
		// send the appropriate query string to the database
		try {
			l.processQuery("RELEASE SAVEPOINT " + sp.getName());
		} finally {
			l.close();
		}
	}

	/**
	 * Undoes all changes made in the current transaction and releases
	 * any database locks currently held by this Connection object. This
	 * method should be used only when auto-commit mode has been
	 * disabled.
	 *
	 * @throws SQLException if a database access error occurs or this
	 *         Connection object is in auto-commit mode
	 * @see #setAutoCommit(boolean)
	 */
	@Override
	public void rollback() throws SQLException {
		// note: can't use sendIndependentCommand here because we need
		// to process the auto_commit state the server gives

		// create a container for the result
		ResponseList l = new ResponseList(
			0,
			0,
			ResultSet.FETCH_FORWARD,
			ResultSet.CONCUR_READ_ONLY
		);
		// send rollback to the server
		try {
			l.processQuery("ROLLBACK");
		} finally {
			l.close();
		}
	}

	/**
	 * Undoes all changes made after the given Savepoint object was set.
	 * 
	 * This method should be used only when auto-commit has been
	 * disabled.
	 *
	 * @param savepoint the Savepoint object to roll back to
	 * @throws SQLException if a database access error occurs, the
	 *         Savepoint object is no longer valid, or this Connection
	 *         object is currently in auto-commit mode
	 */
	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		if (!(savepoint instanceof MonetSavepoint)) throw
			new SQLException("This driver can only handle savepoints it created itself", "M0M06");

		MonetSavepoint sp = (MonetSavepoint)savepoint;

		// note: can't use sendIndependentCommand here because we need
		// to process the auto_commit state the server gives

		// create a container for the result
		ResponseList l = new ResponseList(
			0,
			0,
			ResultSet.FETCH_FORWARD,
			ResultSet.CONCUR_READ_ONLY
		);
		// send the appropriate query string to the database
		try {
			l.processQuery("ROLLBACK TO SAVEPOINT " + sp.getName());
		} finally {
			l.close();
		}
	}

	/**
	 * Sets this connection's auto-commit mode to the given state. If a
	 * connection is in auto-commit mode, then all its SQL statements
	 * will be executed and committed as individual transactions.
	 * Otherwise, its SQL statements are grouped into transactions that
	 * are terminated by a call to either the method commit or the
	 * method rollback. By default, new connections are in auto-commit
	 * mode.
	 * 
	 * The commit occurs when the statement completes or the next
	 * execute occurs, whichever comes first. In the case of statements
	 * returning a ResultSet object, the statement completes when the
	 * last row of the ResultSet object has been retrieved or the
	 * ResultSet object has been closed. In advanced cases, a single
	 * statement may return multiple results as well as output parameter
	 * values. In these cases, the commit occurs when all results and
	 * output parameter values have been retrieved.
	 * 
	 * NOTE: If this method is called during a transaction, the
	 * transaction is committed.
	 *
 	 * @param autoCommit true to enable auto-commit mode; false to disable it
	 * @throws SQLException if a database access error occurs
	 * @see #getAutoCommit()
	 */
	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		if (this.autoCommit != autoCommit) {
			sendControlCommand("auto_commit " + (autoCommit ? "1" : "0"));
			this.autoCommit = autoCommit;
		}
	}

	/**
	 * Sets the given catalog name in order to select a subspace of this
	 * Connection object's database in which to work.  If the driver
	 * does not support catalogs, it will silently ignore this request. 
	 */
	@Override
	public void setCatalog(String catalog) throws SQLException {
		// silently ignore this request
	}

	/**
	 * Not implemented by MonetDB's JDBC driver.
	 *
	 * @param name The name of the client info property to set
	 * @param value The value to set the client info property to. If the
	 *        value is null, the current value of the specified property
	 *        is cleared.
	 */
	@Override
	public void setClientInfo(String name, String value) {
		addWarning("clientInfo: " + name + "is not a recognised property", "01M07");
	}

	/**
	 * Not implemented by MonetDB's JDBC driver.
	 *
	 * @param props The list of client info properties to set
	 */
	@Override
	public void setClientInfo(Properties props) {
		for (Entry<Object, Object> entry : props.entrySet()) {
			setClientInfo(entry.getKey().toString(),
					entry.getValue().toString());
		}
	}

	@Override
	public void setHoldability(int holdability) {}

	/**
	 * Puts this connection in read-only mode as a hint to the driver to
	 * enable database optimizations.  MonetDB doesn't support any mode
	 * here, hence an SQLWarning is generated if attempted to set
	 * to true here.
	 *
	 * @param readOnly true enables read-only mode; false disables it
	 * @throws SQLException if a database access error occurs or this
	 *         method is called during a transaction.
	 */
	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		if (readOnly == true)
			addWarning("cannot setReadOnly(true): read-only Connection mode not supported", "01M08");
	}

	/**
	 * Creates an unnamed savepoint in the current transaction and
	 * returns the new Savepoint object that represents it.
	 *
	 * @return the new Savepoint object
	 * @throws SQLException if a database access error occurs or this Connection
	 *         object is currently in auto-commit mode
	 */
	@Override
	public Savepoint setSavepoint() throws SQLException {
		// create a new Savepoint object
		MonetSavepoint sp = new MonetSavepoint();

		// note: can't use sendIndependentCommand here because we need
		// to process the auto_commit state the server gives

		// create a container for the result
		ResponseList l = new ResponseList(
			0,
			0,
			ResultSet.FETCH_FORWARD,
			ResultSet.CONCUR_READ_ONLY
		);
		// send the appropriate query string to the database
		try {
			l.processQuery("SAVEPOINT " + sp.getName());
		} finally {
			l.close();
		}

		return sp;
	}

	/**
	 * Creates a savepoint with the given name in the current
	 * transaction and returns the new Savepoint object that represents
	 * it.
	 *
	 * @param name a String containing the name of the savepoint
	 * @return the new Savepoint object
	 * @throws SQLException if a database access error occurs or this Connection
	 *         object is currently in auto-commit mode
	 */
	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		// create a new Savepoint object
		MonetSavepoint sp;
		try {
			sp = new MonetSavepoint(name);
		} catch (IllegalArgumentException e) {
			throw new SQLException(e.getMessage(), "M0M03");
		}

		// note: can't use sendIndependentCommand here because we need
		// to process the auto_commit state the server gives

		// create a container for the result
		ResponseList l = new ResponseList(
			0,
			0,
			ResultSet.FETCH_FORWARD,
			ResultSet.CONCUR_READ_ONLY
		);
		// send the appropriate query string to the database
		try {
			l.processQuery("SAVEPOINT " + sp.getName());
		} finally {
			l.close();
		}

		return sp;
	}

	/**
	 * Attempts to change the transaction isolation level for this
	 * Connection object to the one given.  The constants defined in the
	 * interface Connection are the possible transaction isolation
	 * levels.
	 *
	 * @param level one of the following Connection constants:
	 *        Connection.TRANSACTION_READ_UNCOMMITTED,
	 *        Connection.TRANSACTION_READ_COMMITTED,
	 *        Connection.TRANSACTION_REPEATABLE_READ, or
	 *        Connection.TRANSACTION_SERIALIZABLE.
	 */
	@Override
	public void setTransactionIsolation(int level) {
		if (level != TRANSACTION_SERIALIZABLE) {
			addWarning("MonetDB only supports fully serializable " +
					"transactions, continuing with transaction level " +
					"raised to TRANSACTION_SERIALIZABLE", "01M09");
		}
	}

	/**
	 * Installs the given TypeMap object as the type map for this
	 * Connection object. The type map will be used for the custom
	 * mapping of SQL structured types and distinct types.
	 *
	 * @param map the java.util.Map object to install as the replacement for
	 *        this Connection  object's default type map
	 */
	@Override
	public void setTypeMap(Map<String, Class<?>> map) {
		typeMap = map;
	}

	/**
	 * Returns a string identifying this Connection to the MonetDB
	 * server.
	 *
	 * @return a String representing this Object
	 */
	@Override
	public String toString() {
		return "MonetDB Connection (" + getJDBCURL() + ") " + 
				(closed ? "connected" : "disconnected");
	}

	//== 1.7 methods (JDBC 4.1)

	/**
	 * Sets the given schema name to access.
	 *
	 * @param schema the name of a schema in which to work
	 * @throws SQLException if a database access error occurs or this
	 *         method is called on a closed connection
	 */
	@Override
	public void setSchema(String schema) throws SQLException {
		if (closed)
			throw new SQLException("Cannot call on closed Connection", "M1M20");
		createStatement().executeUpdate("SET SCHEMA \"" + schema + "\"");
	}

	/**
	 * Retrieves this Connection object's current schema name.
	 *
	 * @return the current schema name or null if there is none
	 * @throws SQLException if a database access error occurs or this
	 *         method is called on a closed connection
	 */
	@Override
	public String getSchema() throws SQLException {
		if (closed)
			throw new SQLException("Cannot call on closed Connection", "M1M20");
		ResultSet rs = createStatement().executeQuery("SELECT CURRENT_SCHEMA");
		try { 
			if (!rs.next())
				throw new SQLException("Row expected", "02000");
			return rs.getString(1);
		} finally {
			rs.close();
		}
	}

	/**
	 * Terminates an open connection. Calling abort results in:
	 *  * The connection marked as closed
	 *  * Closes any physical connection to the database
	 *  * Releases resources used by the connection
	 *  * Insures that any thread that is currently accessing the
	 *    connection will either progress to completion or throw an
	 *    SQLException. 
	 * Calling abort marks the connection closed and releases any
	 * resources. Calling abort on a closed connection is a no-op. 
	 *
	 * @param executor The Executor implementation which will be used by
	 *        abort
	 * @throws SQLException if a database access error occurs or the
	 *         executor is null
	 * @throws SecurityException if a security manager exists and
	 *         its checkPermission method denies calling abort
	 */
	@Override
	public void abort(Executor executor) throws SQLException {
		if (closed)
			return;
		if (executor == null)
			throw new SQLException("executor is null", "M1M05");
		// this is really the simplest thing to do, it destroys
		// everything (in particular the server connection)
		close();
	}

	/**
	 * Sets the maximum period a Connection or objects created from the
	 * Connection will wait for the database to reply to any one
	 * request. If any request remains unanswered, the waiting method
	 * will return with a SQLException, and the Connection or objects
	 * created from the Connection will be marked as closed. Any
	 * subsequent use of the objects, with the exception of the close,
	 * isClosed or Connection.isValid methods, will result in a
	 * SQLException.
	 *
	 * @param executor The Executor implementation which will be used by
	 *        setNetworkTimeout
	 * @param millis The time in milliseconds to wait for the
	 *        database operation to complete
	 * @throws SQLException if a database access error occurs, this
	 *         method is called on a closed connection, the executor is
	 *         null, or the value specified for seconds is less than 0.
	 */
	@Override
	public void setNetworkTimeout(Executor executor, int millis)
		throws SQLException
	{
		if (closed)
			throw new SQLException("Cannot call on closed Connection", "M1M20");
		if (executor == null)
			throw new SQLException("executor is null", "M1M05");
		if (millis < 0)
			throw new SQLException("milliseconds is less than zero", "M1M05");

		try {
			server.setSoTimeout(millis);
		} catch (SocketException e) {
			throw new SQLException(e.getMessage(), "08000");
		}
	}

	/**
	 * Retrieves the number of milliseconds the driver will wait for a
	 * database request to complete. If the limit is exceeded, a
	 * SQLException is thrown.
	 *
	 * @return the current timeout limit in milliseconds; zero means
	 *         there is no limit
	 * @throws SQLException if a database access error occurs or
	 *         this method is called on a closed Connection
	 */
	@Override
	public int getNetworkTimeout() throws SQLException {
		if (closed)
			throw new SQLException("Cannot call on closed Connection", "M1M20");

		try {
			return server.getSoTimeout();
		} catch (SocketException e) {
			throw new SQLException(e.getMessage(), "08000");
		}
	}

	//== end methods of interface Connection

	public String getJDBCURL() {
		String language = "";
		if (lang == LANG_MAL)
			language = "?language=mal";
		return "jdbc:monetdb://" + hostname + ":" + port + "/" +
			database + language;
	}

	/**
	 * Returns whether the BLOB type should be mapped to BINARY type.
	 */
	boolean getBlobAsBinary() {
		return blobIsBinary;
	}

	/**
	 * Sends the given string to MonetDB as regular statement, making
	 * sure there is a prompt after the command is sent.  All possible
	 * returned information is discarded.  Encountered errors are
	 * reported.
	 *
	 * @param command the exact string to send to MonetDB
	 * @throws SQLException if an IO exception or a database error occurs
	 */
	void sendIndependentCommand(String command) throws SQLException {
		synchronized (server) {
			try {
				out.writeLine(
						(queryTempl[0] == null ? "" : queryTempl[0]) +
						command +
						(queryTempl[1] == null ? "" : queryTempl[1]));
				String error = in.waitForPrompt();
				if (error != null)
					throw new SQLException(error.substring(6),
							error.substring(0, 5));
			} catch (SocketTimeoutException e) {
				close(); // JDBC 4.1 semantics: abort()
				throw new SQLException("connection timed out", "08M33");
			} catch (IOException e) {
				throw new SQLException(e.getMessage(), "08000");
			}
		}
	}

	/**
	 * Sends the given string to MonetDB as control statement, making
	 * sure there is a prompt after the command is sent.  All possible
	 * returned information is discarded.  Encountered errors are
	 * reported.
	 *
	 * @param command the exact string to send to MonetDB
	 * @throws SQLException if an IO exception or a database error occurs
	 */
	void sendControlCommand(String command) throws SQLException {
		// send X command
		synchronized (server) {
			try {
				out.writeLine(
						(commandTempl[0] == null ? "" : commandTempl[0]) +
						command +
						(commandTempl[1] == null ? "" : commandTempl[1]));
				String error = in.waitForPrompt();
				if (error != null)
					throw new SQLException(error.substring(6),
							error.substring(0, 5));
			} catch (SocketTimeoutException e) {
				close(); // JDBC 4.1 semantics, abort()
				throw new SQLException("connection timed out", "08M33");
			} catch (IOException e) {
				throw new SQLException(e.getMessage(), "08000");
			}
		}
	}

	/**
	 * Adds a warning to the pile of warnings this Connection object
	 * has.  If there were no warnings (or clearWarnings was called)
	 * this warning will be the first, otherwise this warning will get
	 * appended to the current warning.
	 *
	 * @param reason the warning message
	 */
	void addWarning(String reason, String sqlstate) {
		if (warnings == null) {
			warnings = new SQLWarning(reason, sqlstate);
		} else {
			warnings.setNextWarning(new SQLWarning(reason, sqlstate));
		}
	}

	/** the default number of rows that are (attempted to) read at once */
	private final static int DEF_FETCHSIZE = 250;
	/** The sequence counter */
	private static int seqCounter = 0;

	/** An optional thread that is used for sending large queries */
	private SendThread sendThread = null;

	/**
	 * A Response is a message sent by the server to indicate some
	 * action has taken place, and possible results of that action.
	 */
	// {{{ interface Response
	interface Response {
		/**
		 * Adds a line to the underlying Response implementation.
		 * 
		 * @param line the header line as String
		 * @param linetype the line type according to the MAPI protocol
		 * @return a non-null String if the line is invalid,
		 *         or additional lines are not allowed.
		 */
		public abstract String addLine(String line, int linetype);

		/**
		 * Returns whether this Reponse expects more lines to be added
		 * to it.
		 *
		 * @return true if a next line should be added, false otherwise
		 */
		public abstract boolean wantsMore();

		/**
		 * Indicates that no more header lines will be added to this
		 * Response implementation.
		 * 
		 * @throws SQLException if the contents of the Response is not
		 *         consistent or sufficient.
		 */
		public abstract void complete() throws SQLException;

		/**
		 * Instructs the Response implementation to close and do the
		 * necessary clean up procedures.
		 *
		 * @throws SQLException
		 */
		public abstract void close();
	}
	// }}}

	/**
	 * The ResultSetResponse represents a tabular result sent by the
	 * server.  This is typically an SQL table.  The MAPI headers of the
	 * Response look like:
	 * <pre>
	 * &amp;1 1 28 2 10
	 * # name,     value # name
	 * # varchar,  varchar # type
	 * </pre>
	 * there the first line consists out of<br />
	 * <tt>&amp;"qt" "id" "tc" "cc" "rc"</tt>.
	 */
	// {{{ ResultSetResponse class implementation
	class ResultSetResponse implements Response {
		/** The number of columns in this result */
		public final int columncount;
		/** The total number of rows this result set has */
		public final int tuplecount;
		/** The numbers of rows to retrieve per DataBlockResponse */
		private int cacheSize;
		/** The table ID of this result */
		public final int id;
		/** The names of the columns in this result */
		private String[] name;
		/** The types of the columns in this result */
		private String[] type;
		/** The max string length for each column in this result */
		private int[] columnLengths;
		/** The table for each column in this result */
		private String[] tableNames;
		/** The query sequence number */
		private final int seqnr;
		/** A List of result blocks (chunks of size fetchSize/cacheSize) */
		private DataBlockResponse[] resultBlocks;

		/** A bitmap telling whether the headers are set or not */
		private boolean[] isSet;
		/** Whether this Response is closed */
		private boolean closed;

		/** The Connection that we should use when requesting a new block */
		private MonetConnection.ResponseList parent;
		/** Whether the fetchSize was explitly set by the user */
		private boolean cacheSizeSetExplicitly = false;
		/** Whether we should send an Xclose command to the server
		 *  if we close this Response */
		private boolean destroyOnClose;
		/** the offset to be used on Xexport queries */
		private int blockOffset = 0;

		/** A parser for header lines */
		HeaderLineParser hlp;

		private final static int NAMES	= 0;
		private final static int TYPES	= 1;
		private final static int TABLES	= 2;
		private final static int LENS	= 3;


		/**
		 * Sole constructor, which requires a MonetConnection parent to
		 * be given.
		 *
		 * @param id the ID of the result set
		 * @param tuplecount the total number of tuples in the result set
		 * @param columncount the number of columns in the result set
		 * @param rowcount the number of rows in the current block
		 * @param parent the parent that created this Response and will
		 *               supply new result blocks when necessary
		 * @param seq the query sequence number
		 */
		ResultSetResponse(
				int id,
				int tuplecount,
				int columncount,
				int rowcount,
				MonetConnection.ResponseList parent,
				int seq)
			throws SQLException
		{
			isSet = new boolean[7];
			this.parent = parent;
			if (parent.cachesize == 0) {
				/* Below we have to calculate how many "chunks" we need
				 * to allocate to store the entire result.  However, if
				 * the user didn't set a cache size, as in this case, we
				 * need to stick to our defaults. */
				cacheSize = MonetConnection.DEF_FETCHSIZE;
				cacheSizeSetExplicitly = false;
			} else {
				cacheSize = parent.cachesize;
				cacheSizeSetExplicitly = true;
			}
			/* So far, so good.  Now the problem with EXPLAIN, DOT, etc
			 * queries is, that they don't support any block fetching,
			 * so we need to always fetch everything at once.  For that
			 * reason, the cache size is here set to the rowcount if
			 * it's larger, such that we do a full fetch at once.
			 * (Because we always set a reply_size, we can only get a
			 * larger rowcount from the server if it doesn't paginate,
			 * because it's a pseudo SQL result.) */
			if (rowcount > cacheSize)
				cacheSize = rowcount;
			seqnr = seq;
			closed = false;
			destroyOnClose = id > 0 && tuplecount > rowcount;

			this.id = id;
			this.tuplecount = tuplecount;
			this.columncount = columncount;
			this.resultBlocks =
				new DataBlockResponse[(tuplecount / cacheSize) + 1];

			hlp = new HeaderLineParser(columncount);

			resultBlocks[0] = new DataBlockResponse(
				rowcount,
				parent.rstype == ResultSet.TYPE_FORWARD_ONLY
			);
		}

		/**
		 * Parses the given string and changes the value of the matching
		 * header appropriately, or passes it on to the underlying
		 * DataResponse.
		 *
		 * @param tmpLine the string that contains the header
		 * @return a non-null String if the header cannot be parsed or
		 *         is unknown
		 */
		// {{{ addLine
		@Override
		public String addLine(String tmpLine, int linetype) {
			if (isSet[LENS] && isSet[TYPES] && isSet[TABLES] && isSet[NAMES]) {
				return resultBlocks[0].addLine(tmpLine, linetype);
			}

			if (linetype != BufferedMCLReader.HEADER)
				return "header expected, got: " + tmpLine;

			// depending on the name of the header, we continue
			try {
				switch (hlp.parse(tmpLine)) {
					case HeaderLineParser.NAME:
						name = (String[])(hlp.values.clone());
						isSet[NAMES] = true;
					break;
					case HeaderLineParser.LENGTH:
						columnLengths = (int[])(hlp.intValues.clone());
						isSet[LENS] = true;
					break;
					case HeaderLineParser.TYPE:
						type = (String[])(hlp.values.clone());
						isSet[TYPES] = true;
					break;
					case HeaderLineParser.TABLE:
						tableNames = (String[])(hlp.values.clone());
						isSet[TABLES] = true;
					break;
				}
			} catch (MCLParseException e) {
				return e.getMessage();
			}

			// all is well
			return null;
		}
		// }}}

		/**
		 * Returns whether this ResultSetResponse needs more lines.
		 * This method returns true if not all headers are set, or the
		 * first DataBlockResponse reports to want more.
		 */
		@Override
		public boolean wantsMore() {
			if (isSet[LENS] && isSet[TYPES] && isSet[TABLES] && isSet[NAMES]) {
				return resultBlocks[0].wantsMore();
			} else {
				return true;
			}
		}

		/**
		 * Returns an array of Strings containing the values between
		 * ',\t' separators.
		 *
		 * @param chrLine a character array holding the input data
		 * @param start where the relevant data starts
		 * @param stop where the relevant data stops
		 * @return an array of Strings
		 */
		final private String[] getValues(char[] chrLine, int start, int stop) {
			int elem = 0;
			String[] values = new String[columncount];
			
			for (int i = start; i < stop; i++) {
				if (chrLine[i] == '\t' && chrLine[i - 1] == ',') {
					values[elem++] =
						new String(chrLine, start, i - 1 - start);
					start = i + 1;
				}
			}
			// at the left over part
			values[elem++] = new String(chrLine, start, stop - start);

			return values;
		}

		/**
		 * Adds the given DataBlockResponse to this ResultSetResponse at
		 * the given block position.
		 *
		 * @param offset the offset number of rows for this block
		 * @param rr the DataBlockResponse to add
		 */
		void addDataBlockResponse(int offset, DataBlockResponse rr) {
			int block = (offset - blockOffset) / cacheSize;
			resultBlocks[block] = rr;
		}

		/**
		 * Marks this Response as being completed.  A complete Response
		 * needs to be consistent with regard to its internal data.
		 *
		 * @throws SQLException if the data currently in this Response is not
		 *                      sufficient to be consistant
		 */
		@Override
		public void complete() throws SQLException {
			String error = "";
			if (!isSet[NAMES]) error += "name header missing\n";
			if (!isSet[TYPES]) error += "type header missing\n";
			if (!isSet[TABLES]) error += "table name header missing\n";
			if (!isSet[LENS]) error += "column width header missing\n";
			if (error != "") throw new SQLException(error, "M0M10");
		}

		/**
		 * Returns the names of the columns
		 *
		 * @return the names of the columns
		 */
		String[] getNames() {
			return name;
		}

		/**
		 * Returns the types of the columns
		 *
		 * @return the types of the columns
		 */
		String[] getTypes() {
			return type;
		}

		/**
		 * Returns the tables of the columns
		 *
		 * @return the tables of the columns
		 */
		String[] getTableNames() {
			return tableNames;
		}

		/**
		 * Returns the lengths of the columns
		 *
		 * @return the lengths of the columns
		 */
		int[] getColumnLengths() {
			return columnLengths;
		}

		/**
		 * Returns the cache size used within this Response
		 *
		 * @return the cache size
		 */
		int getCacheSize() {
			return cacheSize;
		}

		/**
		 * Returns the current block offset
		 *
		 * @return the current block offset
		 */
		int getBlockOffset() {
			return blockOffset;
		}

		/**
		 * Returns the ResultSet type, FORWARD_ONLY or not.
		 *
		 * @return the ResultSet type
		 */
		int getRSType() {
			return parent.rstype;
		}

		/**
		 * Returns the concurrency of the ResultSet.
		 *
		 * @return the ResultSet concurrency
		 */
		int getRSConcur() {
			return parent.rsconcur;
		}

		/**
		 * Returns a line from the cache. If the line is already present in the
		 * cache, it is returned, if not apropriate actions are taken to make
		 * sure the right block is being fetched and as soon as the requested
		 * line is fetched it is returned.
		 *
		 * @param row the row in the result set to return
		 * @return the exact row read as requested or null if the requested row
		 *         is out of the scope of the result set
		 * @throws SQLException if an database error occurs
		 */
		String getLine(int row) throws SQLException {
			if (row >= tuplecount || row < 0)
				return null;

			int block = (row - blockOffset) / cacheSize;
			int blockLine = (row - blockOffset) % cacheSize;

			// do we have the right block loaded? (optimistic try)
			DataBlockResponse rawr;
			// load block if appropriate
			if ((rawr = resultBlocks[block]) == null) {
				/// TODO: ponder about a maximum number of blocks to keep
				///       in memory when dealing with random access to
				///       reduce memory blow-up

				// if we're running forward only, we can discard the old
				// block loaded
				if (parent.rstype == ResultSet.TYPE_FORWARD_ONLY) {
					for (int i = 0; i < block; i++)
						resultBlocks[i] = null;

					if (MonetConnection.seqCounter - 1 == seqnr &&
							!cacheSizeSetExplicitly &&
							tuplecount - row > cacheSize &&
							cacheSize < MonetConnection.DEF_FETCHSIZE * 10)
					{
						// there has no query been issued after this
						// one, so we can consider this an uninterrupted
						// continuation request.  Let's once increase
						// the cacheSize as it was not explicitly set,
						// since the chances are high that we won't
						// bother anyone else by doing so, and just
						// gaining some performance.

						// store the previous position in the
						// blockOffset variable
						blockOffset += cacheSize;

						// increase the cache size (a lot)
						cacheSize *= 10;

						// by changing the cacheSize, we also
						// change the block measures.  Luckily
						// we don't care about previous blocks
						// because we have a forward running
						// pointer only.  However, we do have
						// to recalculate the block number, to
						// ensure the next call to find this
						// new block.
						block = (row - blockOffset) / cacheSize;
						blockLine = (row - blockOffset) % cacheSize;
					}
				}

				// ok, need to fetch cache block first
				parent.executeQuery(
						commandTempl, 
						"export " + id + " " + ((block * cacheSize) + blockOffset) + " " + cacheSize 
				);
				rawr = resultBlocks[block];
				if (rawr == null) throw
					new AssertionError("block " + block + " should have been fetched by now :(");
			}

			return rawr.getRow(blockLine);
		}

		/**
		 * Closes this Response by sending an Xclose to the server indicating
		 * that the result can be closed at the server side as well.
		 */
		@Override
		public void close() {
			if (closed) return;
			// send command to server indicating we're done with this
			// result only if we had an ID in the header and this result
			// was larger than the reply size
			try {
				if (destroyOnClose) sendControlCommand("close " + id);
			} catch (SQLException e) {
				// probably a connection error...
			}

			// close the data block associated with us
			for (int i = 1; i < resultBlocks.length; i++) {
				DataBlockResponse r = resultBlocks[i];
				if (r != null) r.close();
			}

			closed = true;
		}

		/**
		 * Returns whether this Response is closed
		 *
		 * @return whether this Response is closed
		 */
		boolean isClosed() {
			return closed;
		}
	}
	// }}}
	
	/**
	 * The DataBlockResponse is tabular data belonging to a
	 * ResultSetResponse.  Tabular data from the server typically looks
	 * like:
	 * <pre>
	 * [ "value",	56	]
	 * </pre>
	 * where each column is separated by ",\t" and each tuple surrounded
	 * by brackets ("[" and "]").  A DataBlockResponse object holds the
	 * raw data as read from the server, in a parsed manner, ready for
	 * easy retrieval.
	 * 
	 * This object is not intended to be queried by multiple threads
	 * synchronously. It is designed to work for one thread retrieving
	 * rows from it.  When multiple threads will retrieve rows from this
	 * object, it is possible for threads to get the same data.
	 */
	// {{{ DataBlockResponse class implementation
	static class DataBlockResponse implements Response {
		/** The String array to keep the data in */
		private final String[] data;

		/** The counter which keeps the current position in the data array */
		private int pos;
		/** Whether we can discard lines as soon as we have read them */
		private boolean forwardOnly;

		/**
		 * Constructs a DataBlockResponse object
		 * @param size the size of the data array to create
		 * @param forward whether this is a forward only result
		 */
		DataBlockResponse(int size, boolean forward) {
			pos = -1;
			data = new String[size];
			forwardOnly = forward;
		}

		/**
		 * addLine adds a String of data to this object's data array.
		 * Note that an IndexOutOfBoundsException can be thrown when an
		 * attempt is made to add more than the original construction size
		 * specified.
		 * 
		 * @param line the header line as String
		 * @param linetype the line type according to the MAPI protocol
		 * @return a non-null String if the line is invalid,
		 *         or additional lines are not allowed.
		 */
		@Override
		public String addLine(String line, int linetype) {
			if (linetype != BufferedMCLReader.RESULT)
				return "protocol violation: unexpected line in data block: " + line;
			// add to the backing array
			data[++pos] = line;

			// all is well
			return null;
		}

		/**
		 * Returns whether this Reponse expects more lines to be added
		 * to it.
		 *
		 * @return true if a next line should be added, false otherwise
		 */
		@Override
		public boolean wantsMore() {
			// remember: pos is the value already stored
			return pos + 1 < data.length;
		}

		/**
		 * Indicates that no more header lines will be added to this
		 * Response implementation.  In most cases this is a redundant
		 * operation because the data array is full.  However... it can
		 * happen that this is NOT the case!
		 *
		 * @throws SQLException if not all rows are filled
		 */
		@Override
		public void complete() throws SQLException {
			if ((pos + 1) != data.length) throw
				new SQLException("Inconsistent state detected!  Current block capacity: " + data.length + ", block usage: " + (pos + 1) + ".  Did MonetDB send what it promised to?", "M0M10");
		}

		/**
		 * Instructs the Response implementation to close and do the
		 * necessary clean up procedures.
		 *
		 * @throws SQLException
		 */
		@Override
		public void close() {
			// feed all rows to the garbage collector
			for (int i = 0; i < data.length; i++) data[i] = null;
		}

		/**
		 * Retrieves the required row.  Warning: if the requested rows
		 * is out of bounds, an IndexOutOfBoundsException will be
		 * thrown.
		 *
		 * @param line the row to retrieve
		 * @return the requested row as String
		 */
		String getRow(int line) {
			if (forwardOnly) {
				String ret = data[line];
				data[line] = null;
				return ret;
			} else {
				return data[line];
			}
		}
	}
	// }}}

	/**
	 * The UpdateResponse represents an update statement response.  It
	 * is issued on an UPDATE, INSERT or DELETE SQL statement.  This
	 * response keeps a count field that represents the affected rows
	 * and a field that contains the last inserted auto-generated ID, or
	 * -1 if not applicable.<br />
	 * <tt>&amp;2 0 -1</tt>
	 */
	// {{{ UpdateResponse class implementation
	static class UpdateResponse implements Response {
		public final int count;
		public final String lastid;
		
		public UpdateResponse(int cnt, String id) {
			// fill the blank finals
			this.count = cnt;
			this.lastid = id;
		}

		@Override
		public String addLine(String line, int linetype) {
			return "Header lines are not supported for an UpdateResponse";
		}

		@Override
		public boolean wantsMore() {
			return false;
		}

		@Override
		public void complete() {
			// empty, because there is nothing to check
		}

		@Override
		public void close() {
			// nothing to do here...
		}
	}
	// }}}

	/**
	 * The SchemaResponse represents an schema modification response.
	 * It is issued on statements like CREATE, DROP or ALTER TABLE.
	 * This response keeps a field that represents the success state, as
	 * defined by JDBC, which is currently in MonetDB's case alwats
	 * SUCCESS_NO_INFO.  Note that this state is not sent by the
	 * server.<br />
	 * <tt>&amp;3</tt>
	 */
	// {{{ SchemaResponse class implementation
	class SchemaResponse implements Response {
		public final int state = Statement.SUCCESS_NO_INFO;
		
		@Override
		public String addLine(String line, int linetype) {
			return "Header lines are not supported for a SchemaResponse";
		}

		@Override
		public boolean wantsMore() {
			return false;
		}

		@Override
		public void complete() {
			// empty, because there is nothing to check
		}

		@Override
		public void close() {
			// nothing to do here...
		}
	}
	// }}}

	/**
	 * The AutoCommitResponse represents a transaction message.  It
	 * stores (a change in) the server side auto commit mode.<br />
	 * <tt>&amp;4 (t|f)</tt>
	 */
	// {{{ AutoCommitResponse class implementation
	class AutoCommitResponse extends SchemaResponse {
		public final boolean autocommit;
		
		public AutoCommitResponse(boolean ac) {
			// fill the blank final
			this.autocommit = ac;
		}
	}
	// }}}

	/**
	 * A list of Response objects.  Responses are added to this list.
	 * Methods of this class are not synchronized.  This is left as
	 * responsibility to the caller to prevent concurrent access.
	 */
	// {{{ ResponseList class implementation
	class ResponseList {
		/** The cache size (number of rows in a DataBlockResponse object) */
		final int cachesize;
		/** The maximum number of results for this query */
		final int maxrows;
		/** The ResultSet type to produce */
		final int rstype;
		/** The ResultSet concurrency to produce */
		final int rsconcur;
		/** The sequence number of this ResponseList */
		final int seqnr;
		/** A list of the Responses associated with the query,
		 *  in the right order */
		private List<Response> responses;
		/** A map of ResultSetResponses, used for additional
		 *  DataBlockResponse mapping */
		private Map<Integer, ResultSetResponse> rsresponses;

		/** The current header returned by getNextResponse() */
		private int curResponse;

		/**
		 * Main constructor.  The query argument can either be a String
		 * or List.  An SQLException is thrown if another object
		 * instance is supplied.
		 *
		 * @param cachesize overall cachesize to use
		 * @param maxrows maximum number of rows to allow in the set
		 * @param rstype the type of result sets to produce
		 * @param rsconcur the concurrency of result sets to produce
		 */
		ResponseList(
				int cachesize,
				int maxrows,
				int rstype,
				int rsconcur
		) throws SQLException {
			this.cachesize = cachesize;
			this.maxrows = maxrows;
			this.rstype = rstype;
			this.rsconcur = rsconcur;
			responses = new ArrayList<Response>();
			curResponse = -1;
			seqnr = MonetConnection.seqCounter++;
		}

		/**
		 * Retrieves the next available response, or null if there are
		 * no more responses.
		 *
		 * @return the next Response available or null
		 */
		Response getNextResponse() throws SQLException {
			if (rstype == ResultSet.TYPE_FORWARD_ONLY) {
				// free resources if we're running forward only
				if (curResponse >= 0 && curResponse < responses.size()) {
					Response tmp = responses.get(curResponse);
					if (tmp != null) tmp.close();
					responses.set(curResponse, null);
				}
			}
			curResponse++;
			if (curResponse >= responses.size()) {
				// ResponseList is obviously completed so, there are no
				// more responses
				return null;
			} else {
				// return this response
				return responses.get(curResponse);
			}
		}

		/**
		 * Closes the Reponse at index i, if not null.
		 *
		 * @param i the index position of the header to close
		 */
		void closeResponse(int i) {
			if (i < 0 || i >= responses.size()) return;
			Response tmp = responses.set(i, null);
			if (tmp != null)
				tmp.close();
		}

		/**
		 * Closes the current response.
		 */
		void closeCurrentResponse() {
			closeResponse(curResponse);
		}

		/**
		 * Closes the current and previous responses.
		 */
		void closeCurOldResponses() {
			for (int i = curResponse; i >= 0; i--) {
				closeResponse(i);
			}
		}

		/**
		 * Closes this ResponseList by closing all the Responses in this
		 * ResponseList.
		 */
		void close() {
			for (int i = 0; i < responses.size(); i++) {
				closeResponse(i);
			}
		}

		/**
		 * Returns whether this ResponseList has still unclosed
		 * Responses.
		 */
		boolean hasUnclosedResponses() {
			for (Response r : responses) {
				if (r != null)
					return true;
			}
			return false;
		}

		/**
		 * Executes the query contained in this ResponseList, and
		 * stores the Responses resulting from this query in this
		 * ResponseList.
		 *
		 * @throws SQLException if a database error occurs
		 */
		void processQuery(String query) throws SQLException {
			executeQuery(queryTempl, query);
		}

		/**
		 * Internal executor of queries.
		 *
		 * @param templ the template to fill in
		 * @param the query to execute
		 * @throws SQLException if a database error occurs
		 */
		void executeQuery(String[] templ, String query)
			throws SQLException
		{
			boolean sendThreadInUse = false;
			String error = null;

			try {
				synchronized (server) {
					// make sure we're ready to send query; read data till we
					// have the prompt it is possible (and most likely) that we
					// already have the prompt and do not have to skip any
					// lines.  Ignore errors from previous result sets.
					in.waitForPrompt();

					// {{{ set reply size
					/**
					 * Change the reply size of the server.  If the given
					 * value is the same as the current value known to use,
					 * then ignore this call.  If it is set to 0 we get a
					 * prompt after the server sent it's header.
					 */
					int size = cachesize == 0 ? DEF_FETCHSIZE : cachesize;
					size = maxrows != 0 ? Math.min(maxrows, size) : size;
					// don't do work if it's not needed
					if (lang == LANG_SQL && size != curReplySize && templ != commandTempl) {
						sendControlCommand("reply_size " + size);

						// store the reply size after a successful change
						curReplySize = size;
					}
					// }}} set reply size

					// If the query is larger than the TCP buffer size, use a
					// special send thread to avoid deadlock with the server due
					// to blocking behaviour when the buffer is full.  Because
					// the server will be writing back results to us, it will
					// eventually block as well when its TCP buffer gets full,
					// as we are blocking an not consuming from it.  The result
					// is a state where both client and server want to write,
					// but block.
					if (query.length() > MapiSocket.BLOCK) {
						// get a reference to the send thread
						if (sendThread == null)
							sendThread = new SendThread(out);
						// tell it to do some work!
						sendThread.runQuery(templ, query);
						sendThreadInUse = true;
					} else {
						// this is a simple call, which is a lot cheaper and will
						// always succeed for small queries.
						out.writeLine(
								(templ[0] == null ? "" : templ[0]) +
								query +
								(templ[1] == null ? "" : templ[1]));
					}

					// go for new results
					String tmpLine = in.readLine();
					int linetype = in.getLineType();
					Response res = null;
					while (linetype != BufferedMCLReader.PROMPT) {
						// each response should start with a start of header
						// (or error)
						switch (linetype) {
							case BufferedMCLReader.SOHEADER:
								// make the response object, and fill it
								try {
									switch (sohp.parse(tmpLine)) {
										case StartOfHeaderParser.Q_PARSE:
											throw new MCLParseException("Q_PARSE header not allowed here", 1);
										case StartOfHeaderParser.Q_TABLE:
										case StartOfHeaderParser.Q_PREPARE: {
											int id = sohp.getNextAsInt();
											int tuplecount = sohp.getNextAsInt();
											int columncount = sohp.getNextAsInt();
											int rowcount = sohp.getNextAsInt();
											// enforce the maxrows setting
											if (maxrows != 0 && tuplecount > maxrows)
												tuplecount = maxrows;
											res = new ResultSetResponse(
													id,
													tuplecount,
													columncount,
													rowcount,
													this,
													seqnr
											);
											// only add this resultset to
											// the hashmap if it can possibly
											// have an additional datablock
											if (rowcount < tuplecount) {
												if (rsresponses == null)
													rsresponses = new HashMap<Integer, ResultSetResponse>();
												rsresponses.put(
														Integer.valueOf(id),
														(ResultSetResponse) res
												);
											}
										} break;
										case StartOfHeaderParser.Q_UPDATE:
											res = new UpdateResponse(
													sohp.getNextAsInt(),   // count
													sohp.getNextAsString() // key-id
													);
										break;
										case StartOfHeaderParser.Q_SCHEMA:
											res = new SchemaResponse();
										break;
										case StartOfHeaderParser.Q_TRANS:
											boolean ac = sohp.getNextAsString().equals("t") ? true : false;
											if (autoCommit && ac) {
												addWarning("Server enabled auto commit " +
														"mode while local state " +
														"already was auto commit.", "01M11"
														);
											}
											autoCommit = ac;
											res = new AutoCommitResponse(ac);
										break;
										case StartOfHeaderParser.Q_BLOCK: {
											// a new block of results for a
											// response...
											int id = sohp.getNextAsInt(); 
											sohp.getNextAsInt();	// columncount
											int rowcount = sohp.getNextAsInt();
											int offset = sohp.getNextAsInt();
											ResultSetResponse t =
												rsresponses.get(Integer.valueOf(id));
											if (t == null) {
												error = "M0M12!no ResultSetResponse with id " + id + " found";
												break;
											}

											DataBlockResponse r =
												new DataBlockResponse(
													rowcount,	// rowcount
													t.getRSType() == ResultSet.TYPE_FORWARD_ONLY
												);

											t.addDataBlockResponse(offset, r);
											res = r;
										} break;
									}
								} catch (MCLParseException e) {
									error = "M0M10!error while parsing start of header:\n" +
										e.getMessage() +
										" found: '" + tmpLine.charAt(e.getErrorOffset()) + "'" +
										" in: \"" + tmpLine + "\"" +
										" at pos: " + e.getErrorOffset();
									// flush all the rest
									in.waitForPrompt();
									linetype = in.getLineType();
									break;
								}

								// immediately handle errors after parsing
								// the header (res may be null)
								if (error != null) {
									in.waitForPrompt();
									linetype = in.getLineType();
									break;
								}

								// here we have a res object, which
								// we can start filling
								while (res.wantsMore()) {
									error = res.addLine(
											in.readLine(),
											in.getLineType()
									);
									if (error != null) {
										// right, some protocol violation,
										// skip the rest of the result
										error = "M0M10!" + error;
										in.waitForPrompt();
										linetype = in.getLineType();
										break;
									}
								}
								if (error != null)
									break;
								// it is of no use to store
								// DataBlockReponses, you never want to
								// retrieve them directly anyway
								if (!(res instanceof DataBlockResponse))
									responses.add(res);

								// read the next line (can be prompt, new
								// result, error, etc.) before we start the
								// loop over
								tmpLine = in.readLine();
								linetype = in.getLineType();
							break;
							case BufferedMCLReader.INFO:
								addWarning(tmpLine.substring(1), "01000");

								// read the next line (can be prompt, new
								// result, error, etc.) before we start the
								// loop over
								tmpLine = in.readLine();
								linetype = in.getLineType();
							break;
							default:	// Yeah... in Java this is correct!
								// we have something we don't
								// expect/understand, let's make it an error
								// message
								tmpLine = "!M0M10!protocol violation, unexpected line: " + tmpLine;
								// don't break; fall through...
							case BufferedMCLReader.ERROR:
								// read everything till the prompt (should be
								// error) we don't know if we ignore some
								// garbage here... but the log should reveal
								// that
								error = in.waitForPrompt();
								linetype = in.getLineType();
								if (error != null) {
									error = tmpLine.substring(1) + "\n" + error;
								} else {
									error = tmpLine.substring(1);
								}
							break;
						}
					}
				}

				// if we used the sendThread, make sure it has finished
				if (sendThreadInUse) {
					String tmp = sendThread.getErrors();
					if (tmp != null) {
						if (error == null) {
							error = "08000!" + tmp;
						} else {
							error += "\n08000!" + tmp;
						}
					}
				}
				if (error != null) {
					SQLException ret = null;
					String[] errors = error.split("\n");
					for (int i = 0; i < errors.length; i++) {
						if (ret == null) {
							ret = new SQLException(errors[i].substring(6),
									errors[i].substring(0, 5));
						} else {
							ret.setNextException(new SQLException(
										errors[i].substring(6),
										errors[i].substring(0, 5)));
						}
					}
					throw ret;
				}
			} catch (SocketTimeoutException e) {
				close(); // JDBC 4.1 semantics, abort()
				throw new SQLException("connection timed out", "08M33");
			} catch (IOException e) {
				closed = true;
				throw new SQLException(e.getMessage() + " (mserver still alive?)", "08000");
			}
		}
	}
	// }}}

	/**
	 * A thread to send a query to the server.  When sending large
	 * amounts of data to a server, the output buffer of the underlying
	 * communication socket may overflow.  In such case the sending
	 * process blocks.  In order to prevent deadlock, it might be
	 * desirable that the driver as a whole does not block.  This thread
	 * facilitates the prevention of such 'full block', because this
	 * separate thread only will block.<br />
	 * This thread is designed for reuse, as thread creation costs are
	 * high.
	 */
	// {{{ SendThread class implementation
	static class SendThread extends Thread {
		/** The state WAIT represents this thread to be waiting for
		 *  something to do */
		private final static int WAIT = 0;
		/** The state QUERY represents this thread to be executing a query */
		private final static int QUERY = 1;
		/** The state SHUTDOWN is the final state that ends this thread */
		private final static int SHUTDOWN = -1;

		private String[] templ;
		private String query;
		private BufferedMCLWriter out;
		private String error;
		private int state = WAIT;
		
		final Lock sendLock = new ReentrantLock();
		final Condition queryAvailable = sendLock.newCondition(); 
		final Condition waiting = sendLock.newCondition();

		/**
		 * Constructor which immediately starts this thread and sets it
		 * into daemon mode.
		 *
		 * @param monet the socket to write to
		 */
		public SendThread(BufferedMCLWriter out) {
			super("SendThread");
			setDaemon(true);
			this.out = out;
			start();
		}

		@Override
		public void run() {
			sendLock.lock();
			try {
				while (true) {
					while (state == WAIT) {
						try {
							queryAvailable.await();
						} catch (InterruptedException e) {
							// woken up, eh?
						}
					}
					if (state == SHUTDOWN)
						break;

					// state is QUERY here
					try {
						out.writeLine(
								(templ[0] == null ? "" : templ[0]) +
								query +
								(templ[1] == null ? "" : templ[1]));
					} catch (IOException e) {
						error = e.getMessage();
					}

					// update our state, and notify, maybe someone is waiting
					// for us in throwErrors
					state = WAIT;
					waiting.signal();
				}
			} finally {
				sendLock.unlock();
			}
		}

		/**
		 * Starts sending the given query over the given socket.  Beware
		 * that the thread should be finished (can be assured by calling
		 * throwErrors()) before this method is called!
		 *
		 * @param templ the query template
		 * @param query the query itself
		 * @throws SQLException if this SendThread is already in use
		 */
		public void runQuery(String[] templ, String query) throws SQLException {
			sendLock.lock();
			try {
				if (state != WAIT) 
					throw new SQLException("SendThread already in use or shutting down!", "M0M03");

				this.templ = templ;
				this.query = query;

				// let the thread know there is some work to do
				state = QUERY;
				queryAvailable.signal();
			} finally {
				sendLock.unlock();
			}
		}

		/**
		 * Returns errors encountered during the sending process.
		 *
		 * @return the errors or null if none
		 */
		public String getErrors() {
			sendLock.lock();
			try {
				// make sure the thread is in WAIT state, not QUERY
				while (state == QUERY) {
					try {
						waiting.await();
					} catch (InterruptedException e) {
						// just try again
					}
				}
				if (state == SHUTDOWN)
					error = "SendThread is shutting down";
			} finally {
				sendLock.unlock();
			}
			return error;
		}

		/**
		 * Requests this SendThread to stop. 
		 */
		public void shutdown() {
			sendLock.lock();
			state = SHUTDOWN;
			sendLock.unlock();
			this.interrupt();  // break any wait conditions
		}
	}
	// }}}
}

// vim: foldmethod=marker:
