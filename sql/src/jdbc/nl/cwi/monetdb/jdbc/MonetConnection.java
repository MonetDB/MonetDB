package nl.cwi.monetdb.jdbc;

import java.sql.*;
import java.util.*;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * A Connection suitable for the Monet database
 * <br /><br />
 * This connection represents a connection (session) to a Monet database. SQL
 * statements are executed and results are returned within the context of a
 * connection. This Connection object holds a physical connection to the Monet
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
 * whole Connection interface.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 0.4 (beta release)
 */
public class MonetConnection implements Connection {
	private final String host;
	private final int port;
	private final String database;
	private final String username;
	private final String password;
	private final MonetSocket monet;

	private static boolean debug;
	private boolean closed;

	private SQLWarning warnings = null;
	private Map typeMap = new HashMap();

	// See javadoc for documentation about WeakHashMap if you don't know what
	// it does !!!NOW!!! (only when you deal with it of course)
	private Map statements = new WeakHashMap();

	private int curReplySize = -1;	// the server by default uses -1 (all)

	/**
	 * Constructor of a Connection for MonetDB. At this moment the current
	 * implementation limits itself to storing the given host, database,
	 * username and password for later use by the createStatement() call.
	 * This constructor is only accessible to classes from the jdbc package.
	 *
	 * @param host the hostname to connect to
	 * @param port the port to connect on the host to
	 * @param database the database to use then connected
	 * @param username the username to use to identify
	 * @param password the password to use to identify
	 * @throws SQLException if a database error occurs
	 * @throws IllegalArgumentException is one of the arguments is null or empty
	 */
	MonetConnection(
		String hostname,
		int port,
		boolean blockMode,
		String database,
		String username,
		String password)
		throws SQLException, IllegalArgumentException
	{
		// check arguments
		if (!(hostname != null && !hostname.trim().equals("")))
			throw new IllegalArgumentException("hostname should not be null or empty");
		if (port == 0)
			throw new IllegalArgumentException("port should not be 0");
		if (!(username != null && !username.trim().equals("")))
			throw new IllegalArgumentException("user should not be null or empty");
		if (!(password != null && !password.trim().equals("")))
			throw new IllegalArgumentException("host should not be null or empty");
		/** @todo check and use the database name here... */

		this.host = hostname;
		this.port = port;
		this.database = database;
		this.username = username;
		this.password = password;

		try {
			// make connection to Monet
			if (blockMode) {
				monet = new MonetSocketBlockMode(hostname, port);
			} else {
				monet = new MonetSocket(hostname, port);
			}

			// make sure we own the lock on monet during the whole login
			// procedure
			synchronized (monet) {
				// we're debugging here... uhm, should be off in real life
				if (debug)
					monet.debug("monet_" +
						(new java.util.Date()).getTime()+".log");

				// log in
				if (blockMode) {
					//-- \begin{reverse engineered mode}

					// convenience cast shortcut
					MonetSocketBlockMode blkmon = (MonetSocketBlockMode)monet;

					// mind the newline at the end
					blkmon.write(username + ":" + password + ":blocked\n");
					// We need to send the server our byte order. Java by itself
					// uses network order, however for performance reasons it
					// is nice when we can use native byte buffers. Therefore
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
			throw new SQLException("IO Exception: " + e.getMessage());
		}

		setAutoCommit(true);
		closed = false;
	}

	/**
	 * Sets whether Connections should produce debug information.<br />
	 * Call this method before creating a new Connection!
	 *
	 * @param debug a boolean flag indicating wether to debug or not
	 */
	public static void setDebug(boolean dbug) {
		debug = dbug;
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
		// close the socket
		monet.disconnect();
		// report ourselve as closed
		closed = true;
	}

	/**
	 * Makes all changes made since the previous commit/rollback permanent and
	 * releases any database locks currently held by this Connection object.
	 * This method should be used only when auto-commit mode has been disabled.
	 *
	 * @throws SQLException if a database access error occurs or this Connection
	 *         object is in auto-commit mode
	 * @see setAutoCommit(boolean), getAutoCommit()
	 */
	public void commit() throws SQLException {
		if (getAutoCommit()) throw new SQLException("Currently in auto-commit mode");

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
	 * @see setAutoCommit(boolean)
	 */
	public boolean getAutoCommit() throws SQLException {
		// get it from the database
		Statement stmt;

		stmt = createStatement();
		ResultSet rs =
			stmt.executeQuery("SELECT \"value\" FROM \"session\" WHERE \"name\"='auto_commit'");
		if (rs.next()) {
			boolean ret = rs.getBoolean(1);
			rs.close();
			stmt.close();
			return(ret);
		} else {
			rs.close();
			stmt.close();
			throw new SQLException("Driver Panic!!! Monet doesn't want to tell us what we need! BAAAAAAAAAAAD MONET!");
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

	public boolean isReadOnly() {return(false);}
	public String nativeSQL(String sql) {return(null);}
	public CallableStatement prepareCall(String sql) {return(null);}
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {return(null);}
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {return(null);}
	public PreparedStatement prepareStatement(String sql) {return(null);}
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {return(null);}
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) {return(null);}
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) {return(null);}
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
		if (getAutoCommit()) throw new SQLException("Currently in auto-commit mode");
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
	 * @see setAutoCommit(boolean), getAutoCommit()
	 */
	public void rollback() throws SQLException {
		if (getAutoCommit()) throw new SQLException("Currently in auto-commit mode");

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
		if (getAutoCommit()) throw new SQLException("Currently in auto-commit mode");
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
	 * @see getAutoCommit()
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
	 * @returns the new Savepoint object
	 * @throws SQLException if a database access error occurs or this Connection
	 *         object is currently in auto-commit mode
	 */
	public Savepoint setSavepoint() throws SQLException {
		if (getAutoCommit()) throw new SQLException("Currently in auto-commit mode");

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
	 * @returns the new Savepoint object
	 * @throws SQLException if a database access error occurs or this Connection
	 *         object is currently in auto-commit mode
	 */
	public Savepoint setSavepoint(String name) throws SQLException {
		if (getAutoCommit()) throw new SQLException("Currently in auto-commit mode");

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
	 * is the same as the current value, the call is ignored and this method
	 * will immediately return.
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
		// know with Monet
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
		// know with Monet
		if (error != null) throw new SQLException(error);
	}

	/**
	 * Sends the given string to Monet, making sure there is a prompt before
	 * and after the command has sent. All possible returned information is
	 * discarded.
	 *
	 * @param command the exact string to send to Monet
	 * @return a string containing errors that occurred after the command was
	 *         executed, or null if no errors occurred
	 * @throws SQLException if an IO exception occurs
	 */
	private String sendIndependantCommand(String command) throws SQLException {
		String error;
		// get lock on Monet
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
}
