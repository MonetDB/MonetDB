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
 * Portions created by CWI are Copyright (C) 1997-2005 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.jdbc;

import java.sql.*;
import java.util.*;
import java.io.*;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;

/**
 * A Connection suitable for the MonetDB database.
 * <br /><br />
 * This connection represents a connection (session) to a MonetDB database. SQL
 * statements are executed and results are returned within the context of a
 * connection. This Connection object holds a physical connection to the MonetDB
 * database.
 * <br /><br />
 * A Connection object's database should able to provide information describing
 * its tables, its supported SQL grammar, its stored procedures, the
 * capabilities of this connection, and so on. This information is obtained with
 * the getMetaData method.<br />
 * Note: By default a Connection object is in auto-commit mode, which means that
 * it automatically commits changes after executing each statement. If
 * auto-commit mode has been disabled, the method commit must be called
 * explicitly in order to commit changes; otherwise, database changes will not
 * be saved.
 * <br /><br />
 * The current state of this connection is that it nearly implements the
 * whole Connection interface.<br />
 * Be aware that this Connection is a thread that reads from the socket
 * independent from what the client requests (pre-fetching strategy).
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 0.9
 */
public class MonetConnection extends Thread implements Connection {
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
	/** The language which is used */
	private final int lang;
	/** Whether to use server-side (native) or Java emulated
	 * PreparedStatements */
	private final boolean nativePreparedStatements;
	/** A connection to Mserver using a TCP socket */
	private final MonetSocket monet;

	/** Whether this Connection is closed (and cannot be used anymore) */
	private boolean closed;

	/** The stack of warnings for this Connection object */
	private SQLWarning warnings = null;
	/** The Connection specific mapping of user defined types to Java
	 * types (not used) */
	private Map typeMap = new HashMap();

	// See javadoc for documentation about WeakHashMap if you don't know what
	// it does !!!NOW!!! (only when you deal with it of course)
	/** A Map containing all (active) Statements created from this Connection */
	private Map statements = new WeakHashMap();

	/** The number of results we receive from the server at once */
	private int curReplySize = -1;	// the server by default uses -1 (all)

	/* only parse the date patterns once, use multiple times */
	/** Format of a timestamp */
	final static SimpleDateFormat mTimestamp =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	/** Format of a timestamp with RFC822 time zone */
	final static SimpleDateFormat mTimestampZ =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
	/** Format of a time */
	final static SimpleDateFormat mTime =
		new SimpleDateFormat("HH:mm:ss.SSS");
	/** Format of a time with RFC822 time zone */
	final static SimpleDateFormat mTimeZ =
		new SimpleDateFormat("HH:mm:ss.SSSZ");
	/** Format of a date used by Mserver */
	final static SimpleDateFormat mDate =
		new SimpleDateFormat("yyyy-MM-dd");

	/** the SQL language */
	final static int LANG_SQL = 0;
	/** the XQuery language */
	final static int LANG_XQUERY = 1;
	/** the MIL language (officially *NOT* supported) */
	final static int LANG_MIL = 2;
	/** an unknown language */
	final static int LANG_UNKNOWN = -1;

	/** a simple sequence counter for MonetConnections */
	private static int sequence = 0;

	/**
	 * Constructor of a Connection for MonetDB. At this moment the current
	 * implementation limits itself to storing the given host, database,
	 * username and password for later use by the createStatement() call.
	 * This constructor is only accessible to classes from the jdbc package.
	 *
	 * @param props a Property hashtable holding the properties needed for
	 *              connecting
	 * @throws SQLException if a database error occurs
	 * @throws IllegalArgumentException is one of the arguments is null or empty
	 */
	MonetConnection(
		Properties props)
		throws SQLException, IllegalArgumentException
	{
		// set the name for this `thread' (mainly for debugging purposes)
		super("MonetConnection-CachingThread-" + sequence++);

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
		this.nativePreparedStatements = Boolean.valueOf(props.getProperty("native_prepared_statements")).booleanValue();

		String language = props.getProperty("language");
		boolean blockMode = Boolean.valueOf(props.getProperty("blockmode")).booleanValue();
		boolean debug = Boolean.valueOf(props.getProperty("debug")).booleanValue();

		// check input arguments
		if (hostname == null || hostname.trim().equals(""))
			throw new IllegalArgumentException("hostname should not be null or empty");
		if (port == 0)
			throw new IllegalArgumentException("port should not be 0");
		if (database == null || database.trim().equals(""))
			throw new IllegalArgumentException("database should not be null or empty");
		if (username == null || username.trim().equals(""))
			throw new IllegalArgumentException("user should not be null or empty");
		if (password == null || password.trim().equals(""))
			throw new IllegalArgumentException("password should not be null or empty");
		if (language == null || language.trim().equals("")) {
			language = "sql";
			addWarning("No language given, defaulting to 'sql'");
		}

		try {
			// make connection to MonetDB
			if (blockMode) {
				int blocksize;
				try {
					blocksize = Integer.parseInt(props.getProperty("blockmode_blocksize"));
				} catch (NumberFormatException e) {
					blocksize = 0;
				}
				monet = new MonetSocketBlockMode(hostname, port, blocksize);
			} else {
				monet = new MonetSocket(hostname, port);
			}

			/*
			 * There is no need for a lock on the monet object here.
			 * Since we just created the object, and the reference to
			 * this object has not yet been returned to the caller,
			 * noone can (in a legal way) know about the object.
			 */

			// we're debugging here... uhm, should be off in real life
			if (debug) {
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

				monet.debug(f.getAbsolutePath());
			}

			// log in
			if (blockMode) {
				// convenience cast shortcut
				MonetSocketBlockMode blkmon = (MonetSocketBlockMode)monet;
				String challenge = null;

				// read the challenge from the server
				byte[] chal = new byte[2];
				blkmon.read(chal);
				int len = 0;
				try {
					len = Integer.parseInt(new String(chal, "UTF-8"));
				} catch (NumberFormatException e) {
					throw new SQLException("Server challenge length unparsable (" + new String(chal, "UTF-8") + ")");
				}
				// read the challenge string
				chal = new byte[len];
				blkmon.read(chal);
				
				challenge = new String(chal, "UTF-8");

				// mind the newline at the end
				blkmon.write(getChallengeResponse(
					challenge,
					username,
					password,
					language,
					true,
					database
				) + "\n");

				// We need to send the server our byte order.  Java by itself
				// uses network order.
				// A short with value 1234 will be sent to indicate our
				// byte-order.
				/*
				short x = 1234;
				byte high = (byte)(x >>> 8);	// = 0x04
				byte low = (byte)x;				// = 0xD2
				*/
				final byte[] bigEndian = {(byte)0x04, (byte)0xD2};
				blkmon.write(bigEndian);
				blkmon.flush();

				// now read the byte-order of the server
				byte[] byteorder = new byte[2];
				if (blkmon.read(byteorder) != 2)
					throw new SQLException("The server sent an incomplete byte-order sequence");
				if (byteorder[0] == (byte)0x04) {
					// set our connection to big-endian mode
					blkmon.setByteOrder(ByteOrder.BIG_ENDIAN);
				} else if (byteorder[0] == (byte)0xD2) {
					// set our connection to litte-endian mode
					blkmon.setByteOrder(ByteOrder.LITTLE_ENDIAN);
				}
			} else {
				monet.writeln(getChallengeResponse(
					monet.readLine(),
					username,
					password,
					language,
					false,
					database
				));
			}

			// read monet response till prompt
			String err;
			if ((err = monet.waitForPrompt()) != null) {
				monet.disconnect();
				throw new SQLException(err);
			}

			// we seem to have managed to log in, let's store the
			// language used
			if ("sql".equals(language)) {
				lang = LANG_SQL;
			} else if ("xquery".equals(language)) {
				lang = LANG_XQUERY;
			} else if ("mil".equals(language)) {
				lang = LANG_MIL;
			} else {
				lang = LANG_UNKNOWN;
			}
			
			// we're ready for commands!
		} catch (IOException e) {
			throw new SQLException("Unable to connect (" + hostname + ":" + port + "): " + e.getMessage());
		}

		// make ourselves a little more important
		setPriority(getPriority() + 1);
		// quit the VM if it's waiting for this thread to end
		setDaemon(true);
		start();

		// the following initialisers are only valid when the language
		// is SQL...
		if (lang == LANG_SQL) {
			// enable auto commit
			setAutoCommit(true);
			// set our time zone on the server
			Calendar cal = Calendar.getInstance();
			int offset = (cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET)) / (60 * 1000);
			String tz = offset < 0 ? "-" : "+";
			tz += (Math.abs(offset) / 60 < 10 ? "0" : "") + (Math.abs(offset) / 60) + ":";
			offset -= (offset / 60) * 60;
			tz += (offset < 10 ? "0" : "") + offset;
			sendIndependantCommand("SET TIME ZONE INTERVAL '" + tz + "' HOUR TO MINUTE");
		}

		// we're absolutely not closed, since we're brand new
		closed = false;
	}

	/**
	 * A little helper function that processes a challenge string, and
	 * returns a response string for the server.  If the challenge string
	 * is null, a challengeless response is returned.
	 *
	 * @param chalstr the challenge string
	 * @param username the username to use
	 * @param password the password to use
	 * @param language the language to use
	 * @param blocked whether to use blocked protocol
	 * @param database the database to connect to
	 */
	private String getChallengeResponse(
		String chalstr,
		String username,
		String password,
		String language,
		boolean blocked,
		String database
	) throws SQLException {
		int version = 0;
		String response;
		
		// parse the challenge string, split it on ':'
		String[] chaltok = chalstr.split(":");
		if (chaltok.length != 4) throw
			new SQLException("Server challenge string unusable!");

		// challenge string use as salt/key in future
		String challenge = chaltok[1];
		// chaltok[2]; // server type, not needed yet 
		try {
			version = Integer.parseInt(chaltok[3].trim());	// protocol version
		} catch (NumberFormatException e) {
			throw new SQLException("Protocol version unparseable: " + chaltok[3]);
		}

		/**
		 * do something with the challenge to salt the password hash here!!!
		 */
		response = username + ":" + password + ":" + language;
		if (blocked) {
			response += ":blocked";
		} else if (version >= 5) {
			response += ":line";
		}
		if (version < 5) {
			// don't use database
			addWarning("database specifier not supported on this server (" + chaltok[2].trim() + "), protocol version " + chaltok[3].trim());
		} else {
			response += ":" + database;
		}

		return(response);
	}

	//== methods of interface Connection

	/**
	 * Clears all warnings reported for this Connection object. After a call to
	 * this method, the method getWarnings returns null until a new warning is
	 * reported for this Connection object.
	 */
	public void clearWarnings() {
		warnings = null;
	}

	/**
	 * Releases this Connection object's database and JDBC resources immediately
	 * instead of waiting for them to be automatically released. All Statements
	 * created from this Connection will be closed when this method is called.
	 * <br /><br />
	 * Calling the method close on a Connection object that is already closed is
	 * a no-op.
	 */
	public void close() {
		Iterator it = statements.keySet().iterator();
		while (it.hasNext()) {
			try {
				((Statement)it.next()).close();
			} catch (SQLException e) {
				// better luck next time!
			}
		}
		// terminate the thread
		shutdown();
		// close the socket
		monet.disconnect();
		// report ourselves as closed
		closed = true;
	}

	/**
	 * Makes all changes made since the previous commit/rollback permanent and
	 * releases any database locks currently held by this Connection object.
	 * This method should be used only when auto-commit mode has been disabled.
	 *
	 * @throws SQLException if a database access error occurs or this Connection
	 *         object is in auto-commit mode
	 * @see #setAutoCommit(boolean)
	 */
	public void commit() throws SQLException {
		// send commit to the server (note the s in front is a protocol issue)
		sendIndependantCommand("COMMIT");
	}

	/**
	 * Creates a Statement object for sending SQL statements to the database.
	 * SQL statements without parameters are normally executed using Statement
	 * objects. If the same SQL statement is executed many times, it may be more
	 * efficient to use a PreparedStatement object.<br />
	 * <br /><br />
	 * Result sets created using the returned Statement object will by default
	 * be type TYPE_FORWARD_ONLY and have a concurrency level of
	 * CONCUR_READ_ONLY.
	 *
	 * @return a new default Statement object
	 * @throws SQLException if a database access error occurs
	 */
	public Statement createStatement() throws SQLException {
		return(createStatement(
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY));
	}

	/**
	 * Creates a Statement object that will generate ResultSet objects with
	 * the given type and concurrency. This method is the same as the
	 * createStatement method above, but it allows the default result set type
	 * and concurrency to be overridden.
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
	public Statement createStatement(
		int resultSetType,
		int resultSetConcurrency)
		throws SQLException
	{
		try {
			Statement ret =
				new MonetStatement(
					this, resultSetType, resultSetConcurrency
				);
			// store it in the map for when we close...
			statements.put(ret, null);
			return(ret);
		} catch (IllegalArgumentException e) {
			throw new SQLException(e.toString());
		}
		// we don't have to catch SQLException because that is declared to
		// be thrown
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {return(null);}

	/**
	 * Retrieves the current auto-commit mode for this Connection object.
	 *
	 * @return the current state of this Connection object's auto-commit mode
	 * @see #setAutoCommit(boolean)
	 */
	public boolean getAutoCommit() throws SQLException {
		// get it from the database
		Statement stmt;

		stmt = createStatement();
		ResultSet rs =
			stmt.executeQuery("SELECT \"value\" FROM \"sessions\" WHERE \"name\"='auto_commit'");
		if (rs.next()) {
			boolean ret = rs.getBoolean(1);
			rs.close();
			stmt.close();
			return(ret);
		} else {
			rs.close();
			stmt.close();
			throw new SQLException("Driver Panic!!! MonetDB doesn't want to tell us what we need! BAAAAAAAAAAAD MONET!");
		}
	}

	/**
	 * Retrieves this Connection object's current catalog name.
	 *
	 * @return the current catalog name or null if there is none
	 * @throws SQLException if a database access error occurs or the
	 *         current language is not SQL
	 */
	public String getCatalog() throws SQLException {
		if (lang != LANG_SQL)
			throw new SQLException("This method is only supported in SQL mode");

		// this is a dirty hack, but it works as long as MonetDB
		// only handles one catalog (dbfarm) at a time
		ResultSet rs = getMetaData().getCatalogs();
		if (rs.next()) {
			String ret = rs.getString(1);
			rs.close();
			return(ret);
		} else {
			return(null);
		}
	}
	
	public int getHoldability() {return(-1);}

	/**
	 * Retrieves a DatabaseMetaData object that contains metadata about the
	 * database to which this Connection object represents a connection. The
	 * metadata includes information about the database's tables, its supported
	 * SQL grammar, its stored procedures, the capabilities of this connection,
	 * and so on.
	 *
	 * @throws SQLException if the current language is not SQL
	 * @return a DatabaseMetaData object for this Connection object
	 */
	public DatabaseMetaData getMetaData() throws SQLException {
		if (lang != LANG_SQL)
			throw new SQLException("This method is only supported in SQL mode");

		return(new MonetDatabaseMetaData(this));
	}

	/**
	 * Retrieves this Connection object's current transaction isolation level.
	 *
	 * @return the current transaction isolation level, which will be
	 *         Connection.TRANSACTION_SERIALIZABLE
	 */
	public int getTransactionIsolation() {
		return(TRANSACTION_SERIALIZABLE);
	}

	/**
	 * Retrieves the Map object associated with this Connection object. Unless
	 * the application has added an entry, the type map returned will be empty.
	 *
	 * @return the java.util.Map object associated with this Connection object
	 */
	public Map getTypeMap() {
		return(typeMap);
	}

	/**
	 * Retrieves the first warning reported by calls on this Connection object.
	 * If there is more than one warning, subsequent warnings will be chained to
	 * the first one and can be retrieved by calling the method
	 * SQLWarning.getNextWarning on the warning that was retrieved previously.
	 * <br /><br />
	 * This method may not be called on a closed connection; doing so will cause
	 * an SQLException to be thrown.
	 * <br /><br />
	 * Note: Subsequent warnings will be chained to this SQLWarning.
	 *
	 * @return the first SQLWarning object or null if there are none
	 * @throws SQLException if a database access error occurs or this method is
	 *         called on a closed connection
	 */
	public SQLWarning getWarnings() throws SQLException {
		if (closed) throw new SQLException("Cannot call on closed Connection");

		// if there are no warnings, this will be null, which fits with the
		// specification.
		return(warnings);
	}

	/**
	 * Retrieves whether this Connection object has been closed. A connection is
	 * closed if the method close has been called on it or if certain fatal
	 * errors have occurred. This method is guaranteed to return true only when
	 * it is called after the method Connection.close has been called.
	 * <br /><br />
	 * This method generally cannot be called to determine whether a connection
	 * to a database is valid or invalid. A typical client can determine that a
	 * connection is invalid by catching any exceptions that might be thrown
	 * when an operation is attempted.
	 *
	 * @return true if this Connection object is closed; false if it is still
	 *         open
	 */
	public boolean isClosed() {
		return(closed);
	}

	/**
	 * Retrieves whether this Connection object is in read-only mode.  MonetDB
	 * currently doesn't support updateable result sets.
	 *
	 * @return true if this Connection object is read-only; false otherwise
	 */
	public boolean isReadOnly() {
		return(true);
	}

	public String nativeSQL(String sql) {return(null);}
	public CallableStatement prepareCall(String sql) {return(null);}
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {return(null);}
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {return(null);}

	/**
	 * Creates a PreparedStatement object for sending parameterized SQL
	 * statements to the database.
	 * <br /><br />
	 * A SQL statement with or without IN parameters can be pre-compiled and
	 * stored in a PreparedStatement object. This object can then be used to
	 * efficiently execute this statement multiple times.
	 * <br /><br />
	 * Note: This method is optimized for handling parametric SQL statements
	 * that benefit from precompilation. If the driver supports precompilation,
	 * the method prepareStatement will send the statement to the database for
	 * precompilation. Some drivers may not support precompilation. In this
	 * case, the statement may not be sent to the database until the
	 * PreparedStatement object is executed. This has no direct effect on
	 * users; however, it does affect which methods throw certain SQLException
	 * objects.
	 * <br /><br />
	 * Result sets created using the returned PreparedStatement object will by
	 * default be type TYPE_FORWARD_ONLY and have a concurrency level of
	 * CONCUR_READ_ONLY.
	 *
	 * @param sql an SQL statement that may contain one or more '?' IN parameter
	 *            placeholders
	 * @return a new default PreparedStatement object containing the pre-compiled
	 *         SQL statement
	 * @throws SQLException if a database access error occurs
	 */
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return(
			prepareStatement(
					sql,
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY
			)
		);
	}

	/**
	 * Creates a PreparedStatement object that will generate ResultSet objects
	 * with the given type and concurrency. This method is the same as the
	 * prepareStatement method above, but it allows the default result set type
	 * and concurrency to be overridden.
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
	public PreparedStatement prepareStatement(
		String sql,
		int resultSetType,
		int resultSetConcurrency)
		throws SQLException
	{
		try {
			PreparedStatement ret;
			if (nativePreparedStatements) {
				// use a server-side PreparedStatement
				ret = new MonetPreparedStatement(
					this, resultSetType, resultSetConcurrency, sql
				);
			} else {
				// use a Java implementation of a PreparedStatement
				ret = new MonetPreparedStatementJavaImpl(
					this, resultSetType, resultSetConcurrency, sql
				);
			}
			// store it in the map for when we close...
			statements.put(ret, null);
			return(ret);
		} catch (IllegalArgumentException e) {
			throw new SQLException(e.toString());
		}
		// we don't have to catch SQLException because that is declared to
		// be thrown
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {return(null);}
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {return(null);}
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {return(null);}
	public PreparedStatement prepareStatement(String sql, String[] columnNames) {return(null);}

	/**
	 * Removes the given Savepoint object from the current transaction. Any
	 * reference to the savepoint after it have been removed will cause an
	 * SQLException to be thrown.
	 *
	 * @param savepoint the Savepoint object to be removed
	 * @throws SQLException if a database access error occurs or the given
	 *         Savepoint object is not a valid savepoint in the current
	 *         transaction
	 */
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		if (!(savepoint instanceof MonetSavepoint)) throw
			new SQLException("This driver can only handle savepoints it created itself");

		MonetSavepoint sp = (MonetSavepoint)savepoint;

		// send the appropriate query string to the database
		sendIndependantCommand("RELEASE SAVEPOINT " + sp.getName());
	}

	/**
	 * Undoes all changes made in the current transaction and releases any
	 * database locks currently held by this Connection object. This method
	 * should be used only when auto-commit mode has been disabled.
	 *
	 * @throws SQLException if a database access error occurs or this
	 *         Connection object is in auto-commit mode
	 * @see #setAutoCommit(boolean)
	 */
	public void rollback() throws SQLException {
		// send commit to the server (note the s in front is a protocol issue)
		sendIndependantCommand("ROLLBACK");
	}

	/**
	 * Undoes all changes made after the given Savepoint object was set.
	 * <br /><br />
	 * This method should be used only when auto-commit has been disabled.
	 *
	 * @param savepoint the Savepoint object to roll back to
	 * @throws SQLException if a database access error occurs, the Savepoint
	 *         object is no longer valid, or this Connection object is currently
	 *         in auto-commit mode
	 */
	public void rollback(Savepoint savepoint) throws SQLException {
		if (!(savepoint instanceof MonetSavepoint)) throw
			new SQLException("This driver can only handle savepoints it created itself");

		MonetSavepoint sp = (MonetSavepoint)savepoint;

		// send the appropriate query string to the database
		sendIndependantCommand("ROLLBACK TO SAVEPOINT " + sp.getName());
	}

	/**
	 * Sets this connection's auto-commit mode to the given state. If a
	 * connection is in auto-commit mode, then all its SQL statements will be
	 * executed and committed as individual transactions. Otherwise, its SQL
	 * statements are grouped into transactions that are terminated by a call
	 * to either the method commit or the method rollback. By default, new
	 * connections are in auto-commit mode.
	 * <br /><br />
	 * The commit occurs when the statement completes or the next execute
	 * occurs, whichever comes first. In the case of statements returning a
	 * ResultSet object, the statement completes when the last row of the
	 * ResultSet object has been retrieved or the ResultSet object has been
	 * closed. In advanced cases, a single statement may return multiple
	 * results as well as output parameter values. In these cases, the commit
	 * occurs when all results and output parameter values have been retrieved.
	 * <br /><br />
	 * NOTE: If this method is called during a transaction, the transaction is
	 * committed.
	 *
 	 * @param autoCommit true to enable auto-commit mode; false to disable it
	 * @throws SQLException if a database access error occurs
	 * @see #getAutoCommit()
	 */
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		sendIndependantCommand("SET auto_commit = " + autoCommit);
	}

	public void setCatalog(String catalog) {}
	public void setHoldability(int holdability) {}
	public void setReadOnly(boolean readOnly) {}

	/**
	 * Creates an unnamed savepoint in the current transaction and returns the
	 * new Savepoint object that represents it.
	 *
	 * @return the new Savepoint object
	 * @throws SQLException if a database access error occurs or this Connection
	 *         object is currently in auto-commit mode
	 */
	public Savepoint setSavepoint() throws SQLException {
		// create a new Savepoint object
		MonetSavepoint sp = new MonetSavepoint();
		// send the appropriate query string to the database
		sendIndependantCommand("SAVEPOINT " + sp.getName());

		return(sp);
	}

	/**
	 * Creates a savepoint with the given name in the current transaction and
	 * returns the new Savepoint object that represents it.
	 *
	 * @param name a String containing the name of the savepoint
	 * @return the new Savepoint object
	 * @throws SQLException if a database access error occurs or this Connection
	 *         object is currently in auto-commit mode
	 */
	public Savepoint setSavepoint(String name) throws SQLException {
		// create a new Savepoint object
		MonetSavepoint sp;
		try {
			sp = new MonetSavepoint(name);
		} catch (IllegalArgumentException e) {
			throw new SQLException(e.getMessage());
		}
		// send the appropriate query string to the database
		sendIndependantCommand("SAVEPOINT " + sp.getName());

		return(sp);
	}

	/**
	 * Attempts to change the transaction isolation level for this Connection
	 * object to the one given. The constants defined in the interface
	 * Connection are the possible transaction isolation levels.
	 *
	 * @param level one of the following Connection constants:
	 *        Connection.TRANSACTION_READ_UNCOMMITTED,
	 *        Connection.TRANSACTION_READ_COMMITTED,
	 *        Connection.TRANSACTION_REPEATABLE_READ, or
	 *        Connection.TRANSACTION_SERIALIZABLE.
	 */
	public void setTransactionIsolation(int level) {
		if (level != TRANSACTION_SERIALIZABLE) {
			addWarning("MonetDB only supports fully serializable transactions, continuing with transaction level raised to TRANSACTION_SERIALIZABLE");
		}
	}

	/**
	 * Installs the given TypeMap object as the type map for this Connection
	 * object. The type map will be used for the custom mapping of SQL
	 * structured types and distinct types.
	 *
	 * @param map the java.util.Map object to install as the replacement for
	 *        this Connection  object's default type map
	 */
	public void setTypeMap(Map map) {
		typeMap = map;
	}

	//== end methods of interface Connection

	/**
	 * Sends the given string to MonetDB, making sure there is a prompt before
	 * and after the command has sent. All possible returned information is
	 * discarded.
	 *
	 * @param command the exact string to send to MonetDB
	 * @throws SQLException if an IO exception or a database error occurs
	 */
	void sendIndependantCommand(String command) throws SQLException {
		HeaderList hdrl =
			addQuery(command, 0, 0, 0, 0);
		
		while (hdrl.getNextHeader() != null);
	}

	/**
	 * Adds a warning to the pile of warnings this Connection object has. If
	 * there were no warnings (or clearWarnings was called) this warning will
	 * be the first, otherwise this warning will get appended to the current
	 * warning.
	 *
	 * @param reason the warning message
	 */
	private void addWarning(String reason) {
		if (warnings == null) {
			warnings = new SQLWarning(reason);
		} else {
			warnings.setNextWarning(new SQLWarning(reason));
		}
	}



	//=== CacheThread methods


	/**
	 * The CacheThread represents a pseudo array holding all results.  For real
	 * only a part of the complete result set is cached, but upon request for
	 * a result outside the actual cache, the cache is shuffled so the result
	 * comes available.
	 */

	/** A queue of queries that need to be executed by this Statement */
	private List queryQueue = new LinkedList();

	/** The state WAIT represents this thread to be waiting for something to do */
	private final static int WAIT = 0;
	/** The state QUERY represents this thread to be executing a query */
	private final static int QUERY = 1;
	/** The state DEAD represents this thread to be dead and unable to do anything */
	private final static int DEAD = 2;
	/** the default number of rows that are (attempted to) read at once */
	private final static int DEF_FETCHSIZE = 250;
	/** The sequence counter */
	private static int seqCounter = 0;

	/** An optional thread that is used for sending large queries */
	SendThread sendThread = null;
	/** Whether this CacheThread is still running, executing or waiting */
	private int state = WAIT;

	public void run() {
		try {
			while(state != DEAD) {
				Object cur;
				synchronized(queryQueue) {
					cur = null;
					if (queryQueue.size() == 0) {
						try {
							state = WAIT;
							queryQueue.wait();
						} catch (InterruptedException e) {
							// possible shutdown of this thread?
							// next condition check will act appropriately
						}
						continue;
					} else {
						cur = queryQueue.remove(0);
					}
				}

				// at this point we have a valid cur, since the wait continues
				// and skips this part
				if (cur instanceof HeaderList) {
					processQuery((HeaderList)cur);
				} else if (cur instanceof RawResults) {
					fetchBlock((RawResults)cur);
				}
			}
		} catch (Throwable t) {	// we catch EVERYTHING!
			// this thread will die, so before doing so,
			// set it's state appropriately
			state = DEAD;
			// because we cannot tell this to the user in any normal
			// way, the best we can do is dump the stack trace to
			// standard err.
			t.printStackTrace(System.err);
		}
	}

	/**
	 * Lets this thread terminate (die) so it turns into a normal object and
	 * can be garbage collected.
	 */
	void shutdown() {
		state = DEAD;
		// if the thread is blocking on a wait, break it out
		synchronized(queryQueue) {
			queryQueue.notify();
		}
	}

	/**
	 * Adds a new query to the queue of queries that can and should be
	 * executed.  A HeaderList object is returned which is notified when a
	 * new Header is added to it.
	 *
	 * @param query the query to execute
	 * @param cacheSize the size of the cache to use for this query
	 * @param maxRows the maximum number of results for this query
	 * @param rsType the type of the ResultSets to produce
	 * @param rsConcur the concurrency of the ResultSets to produce
	 * @return a HeaderList object which will get filled with Headers
	 * @throws IllegalStateException if this thread is not alive
	 * @see MonetConnection.HeaderList
	 */
	HeaderList addQuery(String query, int cacheSize, int maxRows, int rsType, int rsConcur)
		throws IllegalStateException
	{
		if (state == DEAD) throw
			new IllegalStateException("CacheThread shutting down or not running");

		HeaderList hdrl;
		synchronized(queryQueue) {
			if (lang == LANG_SQL) {
				query = "s" + query + ";";
			} else if (lang == LANG_XQUERY) {
				query = "xml-seq-mapi\n" + query;
			} else if (lang == LANG_MIL) {
				query = query + ";";
			}
			hdrl = new HeaderList(query, cacheSize, maxRows, rsType, rsConcur);
			queryQueue.add(hdrl);
			queryQueue.notify();
		}

		return(hdrl);
	}

	/**
	 * Adds a new query result block request to the queue of queries that
	 * can and should be executed.  A RawResults object is returned which
	 * will be filled as soon as the query request is processed.
	 *
	 * @param hdr the Header this query block is part of
	 * @param block the block number to fetch, index starts at 0
	 * @return a RawResults object which will get filled as soon as the
	 *         query is processed
	 * @throws IllegalStateException if this thread is not alive
	 * @see RawResults
	 */
	RawResults addBlock(Header hdr, int block) throws IllegalStateException {
		if (state == DEAD) throw
			new IllegalStateException("CacheThread shutting down or not running");

		RawResults rawr;
		synchronized(queryQueue) {
			int cacheSize = hdr.getCacheSize();
			// get number of results to fetch
			int size = Math.min(cacheSize, hdr.getTupleCount() - ((block * cacheSize) + hdr.getBlockOffset()));

			if (size == 0) throw
				new IllegalStateException("Should not fetch empty block!");

			rawr = new RawResults(size,
				"Xexport " + hdr.getID() + " " + ((block * cacheSize) + hdr.getBlockOffset()) + " " + size,
				hdr.getRSType() == ResultSet.TYPE_FORWARD_ONLY);

			queryQueue.add(rawr);
			queryQueue.notify();
		}

		return(rawr);
	}

	/**
	 * Adds a result set close command to the head of the queue of queries
	 * that can and should be executed.  Close requests are given maximum
	 * priority because it are small quick terminating queries and release
	 * resources on the server backend.
	 *
	 * @param id the table id of the result set to close
	 * @throws IllegalStateException if this thread is not alive
	 */
	void closeResult(int id) throws IllegalStateException {
		if (state == DEAD) throw
			new IllegalStateException("CacheThread shutting down or not running");

		synchronized(queryQueue) {
			queryQueue.add(0, new RawResults(0, "Xclose " + id, true));
			queryQueue.notify();
		}
	}

	/**
	 * Executes the query contained in the given HeaderList, and stores the
	 * Headers resulting from this query in the HeaderList.
	 * There is no need for an exclusive lock on the monet object here.
	 * Since there is a queue system, queries are executed only by on
	 * specialised thread.  The monet object is not accessible for any
	 * other object (ok, not entirely true) so this specialised thread
	 * is the only one accessing it.
	 *
	 * @param hdrl a HeaderList which contains the query to execute
	 */
	private void processQuery(HeaderList hdrl) {
		boolean sendThreadInUse = false;
		
		try {
			// make sure we're ready to send query; read data till we have the
			// prompt it is possible (and most likely) that we already have
			// the prompt and do not have to skip any lines.  Ignore errors from
			// previous result sets.
			monet.waitForPrompt();

			int size;
			try {
				/**
				 * Change the reply size of the server.  If the given
				 * value is the same as the current value known to use,
				 * then ignore this call.  If it is set to 0 we get a
				 * prompt after the server sent it's header.
				 */
				size = hdrl.cachesize == 0 ? DEF_FETCHSIZE : hdrl.cachesize;
				size = hdrl.maxrows != 0 ? Math.min(hdrl.maxrows, size) : size;
				// don't do work if it's not needed
				if (lang == LANG_SQL && size != 0 && size != curReplySize) {
					monet.writeln("SSET reply_size = " + size + ";");

					String error = monet.waitForPrompt();
					if (error != null) throw new SQLException(error);
					
					// store the reply size after a successful change
					curReplySize = size;
				}
			} catch (SQLException e) {
				hdrl.addError(e.getMessage());
				hdrl.setComplete();
				return;
			}

			// send the query
			// If the query is larger than the TCP buffer size, use a special
			// send thread to avoid deadlock with the server due to blocking
			// behaviour when the buffer is full.  Because the server will be
			// writing back results to us, it will eventually block as well
			// when its TCP buffer gets full, as we are blocking an not
			// consuming from it.  The result is a state where both client and
			// server want to write, but block.
			if (hdrl.query.length() > monet.writecapacity) {
				// get a reference to the send thread
				if (sendThread == null) sendThread = new SendThread();
				// tell it to do some work!
				sendThread.runQuery(hdrl.query, monet);
				sendThreadInUse = true;
			} else {
				// this is a simple call, which is a lot cheaper and will
				// always succeed for small queries
				monet.writeln(hdrl.query);
			}

			// go for new results
			String tmpLine;
			Header hdr = null;
			RawResults rawr = null;
			int lastState = MonetSocket.UNKNOWN;
			do {
				tmpLine = monet.readLine();
				if (monet.getLineType() == MonetSocket.ERROR) {
					// store the error message in the Header object
					hdrl.addError(tmpLine.substring(1));
				} else if (monet.getLineType() == MonetSocket.SOHEADER) {
					// close previous if set
					if (hdr != null) {
						hdr.complete();
						hdrl.addHeader(hdr);
					}

					// create new header
					hdr = new Header(
						this,
						hdrl.cachesize,
						hdrl.maxrows,
						hdrl.rstype,
						hdrl.rsconcur,
						hdrl.seqnr
					);
					if (rawr != null) rawr.finish();
					rawr = null;
				} else if (monet.getLineType() == MonetSocket.HEADER) {
					if (hdr == null) throw
						new SQLException("Protocol violation: header sent before start of header was issued!");
					hdr.addHeader(tmpLine);
				} else if (monet.getLineType() == MonetSocket.RESULT) {
					// complete the header info and add to list
					if (lastState == MonetSocket.HEADER) {
						hdr.complete();
						rawr = new RawResults(size != 0 ? Math.min(size, hdr.getTupleCount()) : hdr.getTupleCount(), null, hdr.getRSType() == ResultSet.TYPE_FORWARD_ONLY);
						hdr.addRawResults(0, rawr);
						// a RawResults must be in hdr at this point!!!
						hdrl.addHeader(hdr);
						hdr = null;
					}
					if (rawr == null) throw
						new SQLException("Protocol violation: result sent before header!");
					rawr.addRow(tmpLine);
				} else if (monet.getLineType() == MonetSocket.UNKNOWN) {
					// unknown, will mean a protocol violation
					addWarning("Protocol violation: unknown linetype.  Ignoring line: " + tmpLine);
				}
			} while ((lastState = monet.getLineType()) != MonetSocket.PROMPT1);
			// Tell the RawResults object there is nothing going to be
			// added right now.  We need to do this because MonetDB
			// sometimes plays games with us and just doesn't send what
			// it promises.
			if (rawr != null) rawr.finish();
			// catch resultless headers
			if (hdr != null) {
				hdr.complete();
				hdrl.addHeader(hdr);
			}
			// if we used the sendThread, make sure it has finished
			if (sendThreadInUse) sendThread.throwErrors();
		} catch (SQLException e) {
			hdrl.addError(e.getMessage());
			// if MonetDB sent us an incomplete or malformed header, we have
			// big problems, thus discard the whole bunch and quit processing
			// this one
			try {
				monet.waitForPrompt();
			} catch (IOException ioe) {
				hdrl.addError(e.toString());
			}
		} catch (IOException e) {
			closed = true;
			hdrl.addError(e.getMessage() + " (Mserver still alive?)");
		}
		// close the header list, no more headers will follow
		hdrl.setComplete();
	}

	/**
	 * Retrieves a continuation block of a previously (partly) fetched
	 * result.  The data is stored in the given RawResults which also
	 * holds the Xeport query to issue on the server.
	 *
	 * @param rawr a RawResults containing the Xexport to execute
	 */
	private void fetchBlock(RawResults rawr) {
		synchronized (monet) {
			try {
				// make sure we're ready to send query; read data till we have the
				// prompt it is possible (and most likely) that we already have
				// the prompt and do not have to skip any lines. Ignore errors from
				// previous result sets.
				monet.waitForPrompt();

				// send the query
				monet.writeln(rawr.getXexport());

				// go for new results, everything should be result (or error :( )
				String tmpLine;
				do {
					tmpLine = monet.readLine();
					if (monet.getLineType() == MonetSocket.RESULT) {
						rawr.addRow(tmpLine);
					} else if (monet.getLineType() == MonetSocket.ERROR) {
						rawr.addError(tmpLine.substring(1));
					} else if (monet.getLineType() == MonetSocket.HEADER) {
						rawr.addError("Unexpected header found");
					} else if (monet.getLineType() == MonetSocket.UNKNOWN) {
						addWarning("Protocol violation, unknown line type for line: " + tmpLine);
					}
				} while (monet.getLineType() != MonetSocket.PROMPT1);
				// Tell the RawResults object there is nothing going to be
				// added right now.  We need to do this because MonetDB
				// sometimes plays games with us and just doesn't send what
				// it promises.
				rawr.finish();
			} catch (IOException e) {
				closed = true;
				rawr.addError("Unexpected end of stream, Mserver still alive? " + e.toString());
			}
		}
	}


	/**
	 * Inner class which holds the raw data as read from the server, and
	 * the associated header information, in a parsed manor, ready for easy
	 * retrieval.
	 * <br /><br />
	 * This object is not intended to be queried by multiple threads
	 * synchronously. It is designed to work for one thread retrieving rows
	 * from it. When multiple threads will retrieve rows from this object, it
	 * is likely for some threads to get locked forever.
	 */
	class RawResults {
		/** The String array to keep the data in */
		private String[] data;
		/** The Xexport query that results in this block */
		private String export;

		/** The counter which keeps the current position in the data array */
		private int pos;
		/** The line to watch for and notify upon when added */
		private int watch;
		/** The errors generated for this ResultBlock */
		private String error;
		/** Whether we can discard lines as soon as we have read them */
		private boolean forwardOnly;

		/**
		 * Constructs a RawResults object
		 * @param size the size of the data array to create
		 * @param export the Xexport query
		 * @param forward whether this is a forward only result
		 */
		RawResults(int size, String export, boolean forward) {
			pos = -1;
			data = new String[size];
			// a newly set watch will always be smaller than size
			watch = data.length;
			this.export = export;
			this.forwardOnly = forward;
			error = "";
		}


		/**
		 * addRow adds a String of data to this object's data array.
		 * Note that an IndexOutOfBoundsException can be thrown when an
		 * attempt is made to add more than the original construction size
		 * specified.
		 *
		 * @param line the String of data to add
		 */
		synchronized void addRow(String line) {
			data[++pos] = line;
			if (pos >= watch) {
				// reset the watch
				watch = data.length;
				// notify listener for our lock object; we got it!
				this.notify();
			}
		}

		/**
		 * finish marks this RawResult as complete.  In most cases this
		 * is a redundant operation because the data array is full.
		 * However... it can happen that this is NOT the case!
		 */
		void finish() {
			if ((pos + 1) != data.length) {
				addError("Inconsistent state detected!  Current block capacity: " + data.length + ", block usage: " + (pos + 1) + ".  Did MonetDB sent what it promised to send?");
			}
		}

		/**
		 * Retrieves the required row. If the row is not present, this method
		 * blocks until the row is available. <br />
		 *
		 * @param line the row to retrieve
		 * @return the requested row as String
		 * @throws IllegalArgumentException if the row to watch for is not
		 *         within the possible range of values (0 - (size - 1))
		 */
		synchronized String getRow(int line)
			throws IllegalArgumentException, SQLException
		{
			if (error != "") throw new SQLException(error);

			if (line >= data.length || line < 0)
				throw new IllegalArgumentException("Row index out of bounds: " + line);

			while (setWatch(line)) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// re-check if we got the desired row
				}
				// re-check for errors
				if (error != "") throw new SQLException(error);
			}
			if (forwardOnly) {
				String ret = data[line];
				data[line] = null;
				return(ret);
			} else {
				return(data[line]);
			}
		}

		/**
		 * Returns the Xexport query associated with this RawResults block.
		 *
		 * @return the Xexport query
		 */
		String getXexport() {
			return(export);
		}

		/**
		 * Adds an error to this object's error queue
		 *
		 * @param error the error string to add
		 */
		synchronized void addError(String error) {
			this.error += error + "\n";
			// notify listener for our lock object; maybe this is bad news
			// that must be heard...
			this.notify();
		}


		/**
		 * Sets a watch for a certain row. When the row gets added, a notify
		 * will be performed on this object itself. The behaviour of this method
		 * is that it returns whether the row is already in the cache or not, so
		 * it can for example be used as:
		 * <pre>
		 *   while (rawr.setWatch(row)) {
		 *     try {
		 *       rawr.wait();
		 *     } catch (InterruptedException e) {
		 *       // recheck if we got the desired row
		 *     }
		 *   }
		 *   rawr.getLine(row);
		 * </pre>
		 *
		 * @param line the row to set the watch for
		 * @return true when the watch was set, false when the row is already
		 *         fetched and no need for wait/notify is there
		 */
		private synchronized boolean setWatch(int line) {
			boolean ret;
			if (line <= pos) {
				ret = false;
			} else {
				watch = line;
				ret = true;
			}
			return(ret);
		}
	}

	/**
	 * A Header represents a Mapi SQL header which looks like:
	 *
	 * <pre>
	 * # name,     value # name
	 * # varchar,  varchar # type
	 * # 28,    # tuplecount
	 * # 1,     # id
	 * </pre>
	 * .
	 * This class does not check all arguments for null, size, etc. for
	 * performance's sake!
	 */
	class Header {
		/** The names of the columns in this result */
		private String[] name;
		/** The types of the columns in this result */
		private String[] type;
		/** The number of rows this result has */
		private int tuplecount;
		/** The table ID of this result */
		private int id;
		/** The query type of this `result' */
		private int queryType;
		/** The query sequence number */
		private final int seqnr;
		/** The max string length for each column in this result */
		private int[] columnLengths;
		/** The table for each column in this result */
		private String[] tableNames;
		/** A Map of result blocks (chunks of size fetchSize/cacheSize) */
		private Map resultBlocks;

		/** A bitmap telling whether the headers are set or not */
		private boolean[] isSet;
		/** Whether this Header is closed */
		private boolean closed;

		/** The Connection that we should use when requesting a new block */
		private MonetConnection cachethread;
		/** A local copy of fetchSize, so its protected from changes made by
		 *  the Statement parent */
		private int cacheSize;
		/** Whether the fetchSize was explitly set by the user */
		private boolean cacheSizeSetExplicitly = false;
		/** A local copy of maxRows, so its protected from changes made by
		 *  the Statement parent */
		private int maxRows;
		/** A local copy of resultSetType, so its protected from changes made
		 *  by the Statement parent */
		private int rstype;
		/** A local copy of resultSetConcurrency, so its protected from changes
		 *  made by the Statement parent */
		private int rsconcur;
		/** Whether we should send an Xclose command to the server
		 *  if we close this Header */
		private boolean destroyOnClose;
		/** the offset to be used on Xexport queries */
		private int blockOffset = 0;


		/**
		 * Sole constructor, which requires a CacheThread parent to be given.
		 *
		 * @param parent the CacheThread that created this Header and will
		 *               supply new result blocks
		 * @param cs the cache size to use
		 * @param mr the maximum number of results to return
		 * @param rst the ResultSet type to use
		 * @param rsc the ResultSet concurrency to use
		 * @param seq the query sequence number
		 */
		Header(
			MonetConnection parent,
			int cs,
			int mr,
			int rst,
			int rsc,
			int seq)
		{
			isSet = new boolean[7];
			resultBlocks = new HashMap();
			cachethread = parent;
			if (cs == 0) {
				cacheSize = MonetConnection.DEF_FETCHSIZE;
				cacheSizeSetExplicitly = false;
			} else {
				cacheSize = cs;
				cacheSizeSetExplicitly = true;
			}
			maxRows = mr;
			rstype = rst;
			rsconcur = rsc;
			seqnr = seq;
			closed = false;
			destroyOnClose = false;
		}

		/**
		 * Parses the given string and changes the value of the matching
		 * header appropriately.
		 *
		 * @param tmpLine the string that contains the header
		 * @throws SQLException if the header cannot be parsed or is unknown
		 */
		void addHeader(String tmpLine) throws SQLException {
			int len = tmpLine.length();
			char[] chrLine = new char[len];
			tmpLine.getChars(0, len, chrLine, 0);

			String name = null;
			int pos = 0;
			boolean foundChar = false;
			// find header name
			for (int i = len - 1; i >= 0; i--) {
				switch (chrLine[i]) {
					case ' ':
					case '\n':
					case '\t':
					case '\r':
						if (!foundChar) {
							len = i - 1;
						} else {
							pos = i + 1;
						}
					break;
					case '#':
						// found!
						if (pos == 0) pos = i + 1;
						name = new String(chrLine, pos, len - pos);
						len = i - 1;	// " #"
						i = 0;	// force the loop to terminate
					break;
					default:
						foundChar = true;
						pos = 0;
					break;
				}
			}
			if (name == null)
				throw new SQLException("Illegal header: " + tmpLine);

			// depending on the name of the header, we continue
			if (name.equals("name")) {
				setNames(getValues(chrLine, 2, len));
			} else if (name.equals("type")) {
				setTypes(getValues(chrLine, 2, len));
			} else if (name.equals("table_name")) {
				setTableNames(getValues(chrLine, 2, len));
			} else if (name.equals("length")) {
				setColumnLengths(getValues(chrLine, 2, len));
			} else if (name.equals("tuplecount")) {
				setTupleCount(getValue(chrLine, 2, len));
			} else if (name.equals("id")) {
				setID(getValue(chrLine, 2, len));
			} else if (name.equals("querytype")) {
				setQueryType(getValue(chrLine, 2, len));
			} else {
				throw new SQLException("Unknown header: " + name);
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
			int elem = 0, capacity = 25;
			String[] values = new String[capacity];	// first guess
			
			for (int i = start; i < stop; i++) {
				if (chrLine[i] == '\t' && chrLine[i - 1] == ',') {
					if (elem == capacity) {
						// increase the capacity, assume it's now or never
						capacity *= 10;
						String[] tmp = new String[capacity];
						for (int j = 0; j < elem; j++) tmp[j] = values[j];
						values = tmp;
					}
					values[elem++] =
						new String(chrLine, start, i - 1 - start);
					start = i + 1;
				}
			}
			// at the left over part
			if (elem == capacity) {
				// increase the capacity just with one
				capacity++;
				String[] tmp = new String[capacity];
				for (int j = 0; j < elem; j++) tmp[j] = values[j];
				values = tmp;
			}
			values[elem++] = new String(chrLine, start, stop - start);

			// if the array is already to the ideal size, return it
			// straight away
			if (capacity == elem) return(values);
			
			// otherwise truncate the array to it's real size
			String[] ret = new String[elem];
			for (int i = 0; i < elem; i++) ret[i] = values[i];
			return(ret);
		}

		/**
		 * Returns an the first String that appears before the first
		 * occurrence of the ',\t' separator.
		 *
		 * @param chrLine a character array holding the input data
		 * @param start where the relevant data starts
		 * @param stop where the relevant data stops
		 * @return the first String found
		 */
		private final String getValue(char[] chrLine, int start, int stop) {
			for (int i = start; i < stop; i++) {
				if (chrLine[i] == '\t' && chrLine[i - 1] == ',') {
					return(new String(chrLine, start, i - 1 - start));
				}
			}
			return(new String(chrLine, start, stop - start));
		}

		/**
		 * Sets the name header and updates the bitmask
		 *
		 * @param name an array of Strings holding the column names
		 */
		private void setNames(String[] name) {
			this.name = name;
			isSet[0] = true;
		}

		/**
		 * Sets the type header and updates the bitmask
		 *
		 * @param type an array of Strings holding the column types
		 */
		private void setTypes(String[] type) {
			this.type = type;
			isSet[1] = true;
		}

		/**
		 * Sets the tuplecount header and updates the bitmask
		 *
		 * @param tuplecount a string representing the tuple count of
		 *                   this result
		 * @throws SQLException if the given string is not a parseable
		 *                      number
		 */
		private void setTupleCount(String tuplecount) throws SQLException {
			try {
				this.tuplecount = Integer.parseInt(tuplecount);
			} catch (NumberFormatException e) {
				throw new SQLException("tuplecount " + tuplecount + " is not a number!");
			}
			isSet[2] = true;
			// The server does not save results that are smaller than the
			// reply size, so we don't have to attempt to clean it up also.
			if (maxRows > 0 &&
				this.tuplecount > curReplySize) destroyOnClose = true;
		}
		
		/**
		 * Sets the id header and updates the bitmask
		 *
		 * @param id a string representing the table id of
		 *           this result
		 * @throws SQLException if the given string is not a parseable
		 *                      number
		 */
		void setID(String id) throws SQLException {
			try {
				this.id = Integer.parseInt(id);
			} catch (NumberFormatException e) {
				throw new SQLException("ID " + id + " is not a number!");
			}
			isSet[3] = true;
		}

		/**
		 * Sets the querytype header and updates the bitmask
		 *
		 * @param queryType a string representing the query type of
		 *                  this `result'
		 * @throws SQLException if the given string is not a parseable
		 *                      number
		 */
		void setQueryType(String queryType) throws SQLException {
			try {
				this.queryType = Integer.parseInt(queryType);
			} catch (NumberFormatException e) {
				throw new SQLException("QueryType " + queryType + " is not a number!");
			}
			isSet[4] = true;
		}

		/**
		 * Sets the table_name header and updates the bitmask
		 *
		 * @param name an array of Strings holding the column's table names
		 */
		private void setTableNames(String[] name) {
			this.tableNames = name;
			isSet[5] = true;
		}

		/**
		 * Sets the length header and updates the bitmask
		 *
		 * @param len an array of Strings holding the column lengths
		 */
		private void setColumnLengths(String[] len) {
			// convert each string to an int
			this.columnLengths = new int[len.length];
			for (int i = 0; i < len.length; i++) {
				this.columnLengths[i] = 0;
				try {
					this.columnLengths[i] = Integer.parseInt(len[i]);
				} catch (NumberFormatException e) {
					// too bad
				}
			}

			isSet[6] = true;
		}

		/**
		 * Adds the given RawResults to this Header at the given block
		 * position.
		 *
		 * @param block the result block the RawResults object represents
		 * @param rr the RawResults to add
		 */
		void addRawResults(int block, RawResults rr) {
			resultBlocks.put("" + block, rr);
		}

		/**
		 * Marks this Header as being completed.  A complete Header needs
		 * to be consistent with regard to its internal data.
		 *
		 * @throws SQLException if the data currently in this Header is not
		 *                      sufficient to be consistant
		 */
		void complete() throws SQLException {
			if (isSet[0] || isSet[1] || isSet[2] || isSet[3] ||
				isSet[4] || isSet[5] || isSet[6]
			) {
				String error = "";
				if (!isSet[4]) error += "querytype header missing\n";
				if (
						queryType == MonetStatement.Q_TABLE ||
						queryType == MonetStatement.Q_PREPARE
				) {
					if (!isSet[0]) error += "name header missing\n";
					if (!isSet[1]) error += "type header missing\n";
					if (!isSet[5]) error += "table name header missing\n";
					if (!isSet[6]) error += "column width header missing\n";
					if (!isSet[2]) error += "tuplecount header missing\n";
					if (!isSet[3]) error += "id header missing\n";
					if (isSet[0] && isSet[1] && isSet[5] && isSet[6]) {
						int cols = name.length;
						if (
								cols != type.length ||
								cols != tableNames.length ||
								cols != columnLengths.length
						) {
							error += "name (" + cols + "), type (" + type.length + "), table (" + tableNames.length + ") and length (" + columnLengths.length + ") do not have the same number of columns\n";
						}
					}
				} else if (
					queryType == MonetStatement.Q_UPDATE ||
					queryType == MonetStatement.Q_TRANS
				) {
					// update count has one result: the count
					tuplecount = 1;
				}
				if (error != "") throw new SQLException(error);

				if (maxRows != 0)
					tuplecount = Math.min(tuplecount, maxRows);

				// make sure the cache size is minimal to
				// reduce overhead and memory usage
				if (cacheSize == 0) {
					cacheSize = tuplecount;
				} else {
					cacheSize = Math.min(tuplecount, cacheSize);
				}
			} else {
				// a no header query (sigh, yes that can happen)
				// make sure there is NO RawResults
				resultBlocks.clear();
			}
		}


		/**
		 * Returns the names of the columns
		 *
		 * @return the names of the columns
		 */
		String[] getNames() {
			return(name);
		}

		/**
		 * Returns the types of the columns
		 *
		 * @return the types of the columns
		 */
		String[] getTypes() {
			return(type);
		}

		/**
		 * Returns the number of results for this result
		 *
		 * @return the number of results for this result
		 */
		int getTupleCount() {
			return(tuplecount);
		}

		/**
		 * Returns the table id for this result
		 *
		 * @return the table id for this result
		 */
		int getID() {
			return(id);
		}

		/**
		 * Returns the query type for this `result'
		 *
		 * @return the query type for this `result'
		 */
		int getQueryType() {
			return(queryType);
		}

		/**
		 * Returns the tables of the columns
		 *
		 * @return the tables of the columns
		 */
		String[] getTableNames() {
			return(tableNames);
		}

		/**
		 * Returns the lengths of the columns
		 *
		 * @return the lengths of the columns
		 */
		int[] getColumnLengths() {
			return(columnLengths);
		}

		/**
		 * Returns the cache size used within this Header
		 *
		 * @return the cache size
		 */
		int getCacheSize() {
			return(cacheSize);
		}

		/**
		 * Returns the result set type used within this Header
		 *
		 * @return the result set type
		 */
		int getRSType() {
			return(rstype);
		}

		/**
		 * Returns the result set concurrency used within this Header
		 *
		 * @return the result set concurrency
		 */
		int getRSConcur() {
			return(rsconcur);
		}

		/**
		 * Returns the current block offset
		 *
		 * @return the current block offset
		 */
		int getBlockOffset() {
			return(blockOffset);
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
			if (row >= tuplecount || row < 0) return null;

			int block = (row - blockOffset) / cacheSize;
			int blockLine = (row - blockOffset) % cacheSize;

			// do we have the right block loaded? (optimistic try)
			RawResults rawr = (RawResults)(resultBlocks.get("" + block));
			// if not, try again and load if appropriate
			if (rawr == null) synchronized(resultBlocks) {
				rawr = (RawResults)(resultBlocks.get("" + block));
				if (rawr == null) {
					/// TODO: ponder about a maximum number of blocks to keep
					///       in memory when dealing with random access to
					///       reduce memory blow-up

					// if we're running forward only, we can discard the old
					// block loaded
					if (rstype == ResultSet.TYPE_FORWARD_ONLY) {
						resultBlocks.clear();

						if (MonetConnection.seqCounter - 1 == seqnr) {
							// there has no query been issued after this
							// one, so we can consider this a uninterrupted
							// continuation request.  Let's increase the
							// blocksize if it was not explicitly set,
							// as the chances are high that we won't bother
							// anyone else by doing so, and just gaining
							// some performance.
							if (!cacheSizeSetExplicitly) {
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
					}
					
					// ok, need to fetch cache block first
					rawr = cachethread.addBlock(this, block);
					resultBlocks.put("" + block, rawr);
				}
			}

			try {
				return(rawr.getRow(blockLine));
			} catch (IllegalArgumentException e) {
				throw new SQLException(e.getMessage());
			}
		}

		/**
		 * Closes this Header by sending an Xclose to the server indicating
		 * that the result can be closed at the server side as well.
		 */
		void close() {
			if (closed) return;
			try {
				// send command to server indicating we're done with this
				// result only if we had an ID in the header and this result
				// was larger than the reply size
				if (destroyOnClose) {
					// since it is not really critical `when' this command is
					// executed, we put it on the CacheThread's queue. If we
					// would want to do it ourselves here, a deadlock situation
					// may occur if the HeaderList calls us.
					cachethread.closeResult(id);
				}
			} catch (IllegalStateException e) {
				// too late, cache thread is gone or shutting down
			}
			closed = true;
		}

		/**
		 * Returns whether this Header is closed
		 *
		 * @return whether this Header is closed
		 */
		boolean isClosed() {
			return(closed);
		}


		protected void finalize() throws Throwable {
			close();
			super.finalize();
		}
	}

	/**
	 * A list of Header objects.  Headers are added to this list till the
	 * setComplete() method is called.  This allows users of this HeaderList
	 * to determine whether more Headers can be added or not.  Upon add or
	 * completion, the object itself is notified, so a user can use this object
	 * to wait on when figuring out whether a new Header is available.
	 */
	class HeaderList {
		/** The query that resulted in this HeaderList */
		final String query;
		/** The cache size (number of rows in a RawResults object) */
		final int cachesize;
		/** The maximum number of results for this query */
		final int maxrows;
		/** The ResultSet type to produce */
		final int rstype;
		/** The ResultSet concurrency to produce */
		final int rsconcur;
		/** The sequence number of this HeaderList */
		final int seqnr;
		/** Whether there are more Headers to come or not */
		private boolean complete;
		/** A list of the Headers associated with the query,
		 *  in the right order */
		private List headers;

		/** The current header returned by getNextHeader() */
		private int curHeader;
		/** The errors produced by the query */
		private String error;

		/**
		 * Main constructor.
		 *
		 * @param query the query that is the 'cause' of this HeaderList
		 * @param cachesize overall cachesize to use
		 * @param maxrows maximum number of rows to allow in the set
		 * @param rstype the type of result sets to produce
		 * @param rsconcur the concurrency of result sets to produce
		 */
		HeaderList(String query, int cachesize, int maxrows, int rstype, int rsconcur) {
			this.query = query;
			this.cachesize = cachesize;
			this.maxrows = maxrows;
			this.rstype = rstype;
			this.rsconcur = rsconcur;
			complete = false;
			headers = new ArrayList();
			curHeader = -1;
			error = "";
			seqnr = MonetConnection.seqCounter++;
		}


		/** Sets the complete flag to true and notifies this object. */
		synchronized void setComplete() {
			complete = true;
			this.notify();
		}

		/** Adds a Header to this object and notifies this object. */
		synchronized void addHeader(Header header) {
			headers.add(header);
			this.notify();
		}

		/**
		 * Retrieves the number of Headers currently in this list.
		 *
		 * @return the number of Header objects in this list
		 */
		synchronized int getSize() {
			return(headers.size());
		}

		/**
		 * Returns whether this HeaderList is completed.
		 *
		 * @return whether this HeaderList is completed
		 */
		synchronized boolean isCompleted() {
			return(complete);
		}

		/**
		 * Retrieves the requested Header.
		 *
		 * @return the Header in this list at position i
		 */
		private synchronized Header getHeader(int i) {
			return((Header)(headers.get(i)));
		}

		/**
		 * Retrieves the next available header, or null if there are no next
		 * headers to come.
		 *
		 * @return the next Header available or null
		 */
		synchronized Header getNextHeader() throws SQLException {

			curHeader++;
			while(curHeader >= getSize() && !isCompleted()) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// hmmm... recheck to see why we were woken up
				}
			}

			if (error != "") throw new SQLException(error);

			if (curHeader >= getSize()) {
				// header is obviously completed so, there are no more headers
				return(null);
			} else {
				// return this header
				return(getHeader(curHeader));
			}
		}

		/** Adds an error to the pile of errors for this object */
		synchronized void addError(String error) {
			this.error += error + "\n";
		}

		/**
		 * Closes the Header at index i, if not null
		 *
		 * @param i the index position of the header to close
		 */
		private synchronized void closeHeader(int i) {
			if (i < 0 || i >= getSize()) return;
			Header tmp = getHeader(i);
			if (tmp != null) tmp.close();
		}

		/**
		 * Closes the current header
		 */
		synchronized void closeCurrentHeader() {
			closeHeader(curHeader);
		}

		/**
		 * Closes the current and previous headers
		 */
		synchronized void closeCurOldHeaders() {
			for (int i = curHeader; i >= 0; i--) {
				closeHeader(i);
			}
		}

		/**
		 * Closes this HeaderList by closing all the Headers in this
		 * HeaderList.
		 */
		synchronized void close() {
			for (int i = 0; i < headers.size(); i++) {
				getHeader(i).close();
			}
		}


		protected void finalize() throws Throwable {
			close();
			super.finalize();
		}
	}
}

/**
 * A thread to send a query to the server.  When sending large amounts of data
 * to a server, the output buffer of the underlying communication socket may
 * overflow.  In such case the sending process blocks.  In order to prevent
 * deadlock, it might be desirable that the driver as a whole does not block.
 * This thread facilitates the prevention of such 'full block', because this
 * separate thread only will block.<br />
 * This thread is designed for reuse, as thread creation costs are high.<br />
 * <br />
 * NOTE: This thread is neither thread safe nor synchronised.  The reason for
 * this is that program wise only one thread (the CacheThread) will use this
 * thread, so costly locking mechanisms can be avoided.
 */
class SendThread extends Thread {
	private String query;
	private MonetSocket conn;
	private String error;

	/**
	 * Constructor which immediately starts this thread and sets it into
	 * daemon mode.
	 */
	public SendThread() {
		super("SendThread");
		setDaemon(true);
		start();
	}

	public synchronized void run() {
		while (true) {
			while (query == null) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// woken up, eh?
				}
			}

			try {
				conn.writeln(query);
			} catch (IOException e) {
				error = e.getMessage();
			}
			
			conn = null;
			query = null;
		}
	}

	/**
	 * Starts sending the given query over the given socket.  Beware that
	 * the thread should be finished (assured by calling throwErrors()) before
	 * this method is called!
	 *
	 * @param query a String containing the query to send
	 * @param monet the socket to write to
	 */
	public synchronized void runQuery(String query, MonetSocket monet) {
		this.query = query;
		conn = monet;
	
		this.notify();
	}

	/**
	 * Throws errors encountered during the sending process or about the
	 * current state of the thread.
	 *
	 * @throws AssertionError (note: Error) if this thread is not finished
	 *         at the time of calling this method (should theoretically never
	 *         happen)
	 * @throws SQLException in case an (IO) error occurred while sending the
	 *         query to the server.
	 */
	public void throwErrors() throws SQLException {
		if (query != null) throw new AssertionError("Aaaiiiiiiii!!! SendThread not finished :(");
		if (error != null) throw new SQLException(error);
	}
}
