package nl.cwi.monetdb.jdbc;

import java.sql.*;
import java.util.*;
import java.io.*;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.regex.*;

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
 * @version 0.6
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
	/** A connection to Mserver using a TCP socket */
	private final MonetSocket monet;

	/** Whether this Connection is closed (and cannot be used anymore) */
	private boolean closed;

	/** The stack of warnings for this Connection object */
	private SQLWarning warnings = null;
	/** The Connection specific mapping of user defined types to Java types (not used) */
	private Map typeMap = new HashMap();

	// See javadoc for documentation about WeakHashMap if you don't know what
	// it does !!!NOW!!! (only when you deal with it of course)
	/** A Map containing all (active) Statements created from this Connection */
	private Map statements = new WeakHashMap();

	/** The number of results we receive from the server at once */
	private int curReplySize = -1;	// the server by default uses -1 (all)

	/* only parse the date patterns once, use multiple times */
	/** Format of a timestamp used by Mserver */
	final static SimpleDateFormat mTimestamp =
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	/** Format of a time used by Mserver */
	final static SimpleDateFormat mTime =
		new SimpleDateFormat("HH:mm:ss.SSS");
	/** Format of a date used by Mserver */
	final static SimpleDateFormat mDate =
		new SimpleDateFormat("yyyy-MM-dd");

	static {
		// make sure strict parsing of time fields is used, such
		// that partial matches are not allowed
		mTimestamp.setLenient(false);
		mTime.setLenient(false);
		mDate.setLenient(false);
	}

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
		/** check and use the database name here... */

		boolean blockMode = Boolean.valueOf(props.getProperty("blockmode")).booleanValue();
		boolean debug = Boolean.valueOf(props.getProperty("debug")).booleanValue();
		/** language ?!? */

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

			// make sure we own the lock on monet during the whole login
			// procedure
			synchronized (monet) {
				// we're debugging here... uhm, should be off in real life
				if (debug) {
					String fname = props.getProperty("logfile", "monet_" +
						(new java.util.Date()).getTime() + ".log");
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
					//-- \begin{reverse engineered mode}

					// convenience cast shortcut
					MonetSocketBlockMode blkmon = (MonetSocketBlockMode)monet;

					// mind the newline at the end
					blkmon.write(username + ":" + password + ":blocked\n");
					// We need to send the server our byte order.  Java by itself
					// uses network order, however for performance reasons it
					// is nice when we can use native byte buffers.  Therefore
					// we send the machine native byte-order to the server here.
					// A short with value 1234 will be sent to indicate our
					// byte-order.
					/*
					short x = 1234;
					byte high = (byte)(x >>> 8);	// = 0x04
					byte low = (byte)x;				// = 0xD2
					*/
					final byte[] bigEndian = {(byte)0x04, (byte)0xD2};
					final byte[] littleEndian = {(byte)0xD2, (byte)0x04};
					ByteOrder nativeOrder = ByteOrder.nativeOrder();
					if (nativeOrder == ByteOrder.BIG_ENDIAN) {
						blkmon.write(bigEndian);
					} else if (nativeOrder == ByteOrder.LITTLE_ENDIAN) {
						blkmon.write(littleEndian);
					} else {
						throw new AssertionError("Machine byte-order unknown!!!");
					}
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

					//-- \end
				} else {
					monet.writeln(username + ":" + password);
				}
				// read monet response till prompt
				String err;
				if ((err = monet.waitForPrompt()) != null) {
					monet.disconnect();
					throw new SQLException(err);
				}

				// we're logged in and ready for commands!
			}
		} catch (IOException e) {
			throw new SQLException("Unable to connect (" + hostname + ":" + port + "): " + e.getMessage());
		}

		setAutoCommit(true);
		closed = false;

		// make ourselves a little more important
		setPriority(getPriority() + 1);
		// quit the VM if it's waiting for this thread to end
		setDaemon(true);
		start();
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
		sendCommit();
	}

	/**
	 * Creates a Statement object for sending SQL statements to the database.
	 * SQL statements without parameters are normally executed using Statement
	 * objects. If the same SQL statement is executed many times, it may be more
	 * efficient to use a PreparedStatement object.<br />
	 * <b>A PreparedStatement is not (yet) implemented in this Connection</b>
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
					monet, this, resultSetType, resultSetConcurrency
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

	public String getCatalog() {return(null);}
	public int getHoldability() {return(-1);}

	/**
	 * Retrieves a DatabaseMetaData object that contains metadata about the
	 * database to which this Connection object represents a connection. The
	 * metadata includes information about the database's tables, its supported
	 * SQL grammar, its stored procedures, the capabilities of this connection,
	 * and so on.
	 *
	 * @return a DatabaseMetaData object for this Connection object
	 */
	public DatabaseMetaData getMetaData() {
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
			PreparedStatement ret =
				new MonetPreparedStatement(
					monet, this, resultSetType, resultSetConcurrency, sql
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
		String error =
			sendIndependantCommand("sRELEASE SAVEPOINT " + sp.getName() + ";");
		if (error != null) throw new SQLException(error);
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
		sendRollback();
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
		String error =
			sendIndependantCommand("sROLLBACK TO SAVEPOINT " + sp.getName() + ";");
		if (error != null) throw new SQLException(error);
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
		String error =
			sendIndependantCommand("SSET auto_commit = " + autoCommit + ";");
		if (error != null) throw new SQLException(error);
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
		String error =
			sendIndependantCommand("sSAVEPOINT " + sp.getName() + ";");
		if (error != null) throw new SQLException(error);

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
		String error =
			sendIndependantCommand("sSAVEPOINT " + sp.getName() + ";");
		if (error != null) throw new SQLException(error);

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
	 * Changes the reply size of the server to the given value. If the given
	 * value is the same as the current value, the call is ignored and this
	 * method will immediately return.
	 *
	 * @param size the new reply size to use
	 * @throws SQLException if a database (access) error occurs
	 */
	void setReplySize(int size) throws SQLException {
		synchronized(monet) {
			// don't do work if it's not needed
			if (size == curReplySize) return;

			String error =
				sendIndependantCommand("SSET reply_size = " + size + ";");
			if (error != null) throw new SQLException(error);

			// store the reply size after a successful change
			curReplySize = size;
		}
	}

	/**
	 * Makes all changes made since the previous commit/rollback permanent and
	 * releases any database locks currently held by this Connection object.
	 * This method sends the actual command to the server to make it effective.
	 *
	 * @throws SQLException if a database access error occurs
	 */
	void sendCommit() throws SQLException {
		// send commit to the server (note the s in front is a protocol issue)
		String error = sendIndependantCommand("sCOMMIT;");
		// I don't know why and how an error could be produced, but you never
		// know with MonetDB
		if (error != null) throw new SQLException(error);
	}

	/**
	 * Undoes all changes made in the current transaction and releases any
	 * database locks currently held by this Connection object. This method
	 * sends the actual command to the server to make it effective.
	 *
	 * @throws SQLException if a database access error occurs
	 */
	void sendRollback() throws SQLException {
		// send commit to the server (note the s in front is a protocol issue)
		String error = sendIndependantCommand("sROLLBACK;");
		// I don't know why and how an error could be produced, but you never
		// know with MonetDB
		if (error != null) throw new SQLException(error);
	}

	/**
	 * Sends the given string to MonetDB, making sure there is a prompt before
	 * and after the command has sent. All possible returned information is
	 * discarded.
	 *
	 * @param command the exact string to send to MonetDB
	 * @return a string containing errors that occurred after the command was
	 *         executed, or null if no errors occurred
	 * @throws SQLException if an IO exception occurs
	 */
	String sendIndependantCommand(String command) throws SQLException {
		String error;
		// get lock on MonetDB
		synchronized(monet) {
			try {
				// make sure we have the prompt
				monet.waitForPrompt();
				// send the command
				monet.writeln(command);
				// wait for the prompt again
				error = monet.waitForPrompt();
			} catch (IOException e) {
				throw new SQLException(e.toString());
			}
		}
		return(error);
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

	/** Whether this CacheThread is still running, executing or waiting */
	private int state = WAIT;

	public void run() {
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
	}

	/**
	 * Lets this thread terminate (die) so it turns into a normal object and
	 * can be garbage collected.
	 * A call to this function should be made when the parent Statement
	 * closes to let this thread disappear.
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
	 * @see MonetStatement.RawResults
	 */
	RawResults addBlock(Header hdr, int block) throws IllegalStateException {
		if (state == DEAD) throw
			new IllegalStateException("CacheThread shutting down or not running");

		RawResults rawr;
		synchronized(queryQueue) {
			int cacheSize = hdr.getCacheSize();
			// get number of results to fetch
			int size = Math.min(cacheSize, hdr.getTupleCount() - (block * cacheSize));

			if (size == 0) throw
				new IllegalStateException("Should not fetch empty block!");

			rawr = new RawResults(size,
				"Xexport " + hdr.getID() + " " + block * cacheSize + " " + size);

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
			queryQueue.add(0, new RawResults(0, "Xclose " + id));
			queryQueue.notify();
		}
	}

	/**
	 * Executes the query contained in the given HeaderList, and stores the
	 * Headers resulting from this query in the HeaderList.
	 *
	 * @param hdrl a HeaderList which contains the query to execute
	 */
	private void processQuery(HeaderList hdrl) {

		synchronized (monet) {
			try {
				// make sure we're ready to send query; read data till we have the
				// prompt it is possible (and most likely) that we already have
				// the prompt and do not have to skip any lines.  Ignore errors from
				// previous result sets.
				monet.waitForPrompt();

				// set the reply size for this query.  If it is set to 0 we get a
				// prompt after the server sent it's header
				try {
					setReplySize(
						hdrl.maxrows != 0 ? Math.min(hdrl.maxrows, hdrl.cachesize) : hdrl.cachesize);
				} catch (SQLException e) {
					hdrl.addError(e.getMessage());
					hdrl.setComplete();
					return;
				}

				// send the query
				monet.writeln(hdrl.query);

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
							hdrl.rsconcur
						);
						rawr = null;
					} else if (monet.getLineType() == MonetSocket.HEADER) {
						if (hdr == null) throw new SQLException("Protocol violation: header sent before start of header was issued!");
						hdr.addHeader(tmpLine);
					} else if (monet.getLineType() == MonetSocket.RESULT) {
						// complete the header info and add to list
						if (lastState == MonetSocket.HEADER) {
							hdr.complete();
							rawr = new RawResults(Math.min(hdrl.cachesize, hdr.getTupleCount()), null);
							hdr.addRawResults(0, rawr);
							// a RawResults must be in hdr at this point!!!
							hdrl.addHeader(hdr);
							hdr = null;
						}
						rawr.addRow(tmpLine);
					} else if (monet.getLineType() == MonetSocket.UNKNOWN) {
						// unknown, will mean a protocol violation
						hdrl.addError("Protocol violation: unknown linetype for line: " + tmpLine);
					}
				} while ((lastState = monet.getLineType()) != MonetSocket.PROMPT1);
				// catch resultless headers
				if (hdr != null) {
					hdr.complete();
					hdrl.addHeader(hdr);
				}
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
				hdrl.addError(e.getMessage() + " (Mserver still alive?)");
			}
			// close the header list, no more headers will follow
			hdrl.setComplete();
		}
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
						rawr.addError("Protocol violation, unknown line type for line: " + tmpLine);
					}
				} while (monet.getLineType() != MonetSocket.PROMPT1);
			} catch (IOException e) {
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

		/**
		 * Constructs a RawResults object
		 * @param size the size of the data array to create
		 */
		RawResults(int size, String export) {
			pos = -1;
			data = new String[size];
			// a newly set watch will always be smaller than size
			watch = data.length;
			this.export = export;
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
		 * Retrieves the required row. If the row is not present, this method will
		 * block until the row is available. <br />
		 * <b>Do *NOT* use multiple threads synchronously on this method</b>
		 *
		 * @param line the row to retrieve
		 * @return the requested row as String
		 * @throws IllegalArgumentException if the row to watch for is not
		 *         within the possible range of values (0 - (size - 1))
		 */
		synchronized String getRow(int line) throws IllegalArgumentException, SQLException {
			if (error != "") throw new SQLException(error);

			if (line >= data.length || line < 0)
				throw new IllegalArgumentException("Cannot get row outside data range (" + line + ")");

			while (setWatch(line)) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// recheck if we got the desired row
				}
			}
			return(data[line]);
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
		void addError(String error) {
			this.error += error + "\n";
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
	 *
	 * This class does not check all arguments for null, size, etc. for
	 * performance sake!
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
		/** A local copy of maxRows, so its protected from changes made by
		 *  the Statement parent */
		private int maxRows;
		/** A local copy of resultSetType, so its protected from changes made
		 *  by the Statement parent */
		private int rstype;
		/** A local copy of resultSetConcurrency, so its protected from changes
		 *  made by the Statement parent */
		private int rsconcur;

		/** a regular expression that we often use to split
		 *  the headers into an array (compile them once) */
		private final Pattern splitPattern = Pattern.compile(",\t");

		/**
		 * Sole constructor, which requires a CacheThread parent to be given.
		 *
		 * @param parent the CacheThread that created this Header and will
		 *               supply new result blocks
		 * @param cs the cache size to use
		 * @param mr the maximum number of results to return
		 * @param rst the ResultSet type to use
		 * @param rsc the ResultSet concurrency to use
		 */
		Header(MonetConnection parent, int cs, int mr, int rst, int rsc) {
			isSet = new boolean[7];
			resultBlocks = new HashMap();
			cachethread = parent;
			cacheSize = cs;
			maxRows = mr;
			rstype = rst;
			rsconcur = rsc;
			closed = false;
		}

		/**
		 * Parses the given string and changes the value of the matching
		 * header appropriately.
		 *
		 * @param tmpLine the string that contains the header
		 * @throws SQLException if the header cannot be parsed or is unknown
		 */
		void addHeader(String tmpLine) throws SQLException {
			int pos = tmpLine.indexOf("#", 1);
			if (pos == -1) {
				throw new SQLException("Illegal header: " + tmpLine);
			}

			// split the header line into an array
			String[] values =
				splitPattern.split(tmpLine.substring(1, pos - 1));
			// remove whitespace from all the results
			for (int i = 0; i < values.length; i++) {
				values[i] = values[i].trim();
			}
			// add the header
			String name = tmpLine.substring(pos + 1).trim();
			if (name.equals("name")) {
				setNames(values);
			} else if (name.equals("type")) {
				setTypes(values);
			} else if (name.equals("tuplecount")) {
				setTupleCount(values[0]);
			} else if (name.equals("id")) {
				setID(values[0]);
			} else if (name.equals("querytype")) {
				setQueryType(values[0]);
			} else if (name.equals("table_name")) {
				setTableNames(values);
			} else if (name.equals("length")) {
				setColumnLengths(values);
			} else {
				throw new SQLException("Unknown header: " + name);
			}
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
		 * @param id a string representing the query type of
		 *           this `result'
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
		 * @param name an array of Strings holding the column lengths
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
		 * @param rawr the RawResults to add
		 */
		void addRawResults(int block, RawResults rr) {
			resultBlocks.put("" + block, rr);
		}

		/**
		 * Marks this Header as being completed.  A complete Header needs
		 * to be consistant with regards to its internal data.
		 *
		 * @throws SQLException if the data currently in this Header is not
		 *                      sufficient to be consistant
		 */
		void complete() throws SQLException {
			boolean hasSome = isSet[0] || isSet[1] || isSet[2] || isSet[3];
			if (hasSome) {
				String error = "";
				if (!isSet[4]) error += "querytype header missing\n";
				if (queryType == MonetStatement.Q_TABLE ||
					queryType == MonetStatement.Q_UPDATE)
				{
					if (!isSet[0]) error += "name header missing\n";
					if (!isSet[1]) error += "type header missing\n";
					if (!isSet[2]) error += "tuplecount header missing\n";
					if (!isSet[3]) error += "id header missing\n";
				}
				if (error != "") throw new SQLException(error);

				if (maxRows != 0)
					tuplecount = Math.min(tuplecount, maxRows);

				// make sure the cache size is minimal to
				// reduce overhead and memory usage
				cacheSize = Math.min(tuplecount, cacheSize);
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

			int block = row / cacheSize;
			int blockLine = row % cacheSize;

			// do we have the right block (still) loaded?
			RawResults rawr;
			synchronized(resultBlocks) {
				rawr = (RawResults)(resultBlocks.get("" + block));
				if (rawr == null) {
					// if we're running forward only, we can discard the old
					// block loaded
					if (rstype == ResultSet.TYPE_FORWARD_ONLY) {
						resultBlocks.clear();
					}

					/// todo: ponder about a maximum number of blocks to keep
					///       in memory when dealing with random access to
					///       reduce memory blow-up

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
				// result only if we had an ID in the header... Currently
				// on updates, inserts and deletes there is no header at all
				if (isSet[3]) {
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
		/** Whether there are more Headers to come or not */
		private boolean complete;
		/** A list of the Headers associated with the query, in the right order */
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
				// return this header, and increment the counter
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
