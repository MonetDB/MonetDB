package nl.cwi.monetdb.jdbc;

import java.sql.*;
import java.util.*;
import java.io.IOException;

/**
 * A Connection suitable for the Monet database
 * <br /><br />
 * This connection represents a connection (session) to a Monet database. SQL
 * statements are executed and results are returned within the context of a
 * connection. <b>In this implementation there is no connection to Monet at this
 * object.</b> Physical connections to Monet are made in a MonetStatement object
 * as returned by createStatement().
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
 * The current state of this connection is that it only implements the
 * createStatement() method with default transactions and cursors.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 0.3 (beta release)
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
	private boolean autoCommit = true;
	
	private SQLWarning warnings = null;
	
	// See javadoc for documentation about WeakHashMap if you don't know what
	// it does !!!NOW!!! (only when you deal with it of course)
	private Map statements = new WeakHashMap();

	private int curReplySize = 100;

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
			monet = new MonetSocket(hostname, port);

			// make sure we own the lock on monet during the whole login
			// procedure
			synchronized (monet) {
				// we're debugging here... uhm, should be off in real life
				if (debug)
					monet.debug("monet_" + 
						(new java.util.Date()).getTime()+".log");

				// log in
				monet.writeln(username + ":" + password);		
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
		if (autoCommit) throw new SQLException("Currently in auto-commit mode");
		
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
	public boolean getAutoCommit() {
		return(autoCommit);
	}
	
	public String getCatalog() {return(null);}
	public int getHoldability() {return(-1);}
	public DatabaseMetaData getMetaData() {return(null);}
	public int getTransactionIsolation() {return(-1);}
	public Map getTypeMap() {return(null);}
	
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
	public void releaseSavepoint(Savepoint savepoint) {}
	
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
		if (autoCommit) throw new SQLException("Currently in auto-commit mode");
		
		sendRollback();
	}
	
	public void rollback(Savepoint savepoint) {}
	
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
	 * @see getAutoCommit()
	 */
	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}
	
	public void setCatalog(String catalog) {}
	public void setHoldability(int holdability) {}
	public void setReadOnly(boolean readOnly) {}
	public Savepoint setSavepoint() {return(null);}
	public Savepoint setSavepoint(String name) {return(null);}
	public void setTransactionIsolation(int level) {}
	public void setTypeMap(Map map) {}
	
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
			
			try {
				monet.writeln("Xreply_size " + size);
				// and wait for the server to be ready again
				String err;
				if ((err = monet.waitForPrompt()) != null)
					throw new SQLException(err);
			} catch (IOException e) {
				throw new SQLException(e.toString());
			}
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
		// send commit to the server
		synchronized(monet) {
			try {
				// make sure we have the prompt
				monet.waitForPrompt();
				// send the commit command (note the S in front is a protocol issue)
				monet.writeln("SCOMMIT;");
				// wait for the prompt again
				monet.waitForPrompt();
			} catch (IOException e) {
				throw new SQLException(e.toString());
			}
		}
	}

	/**
	 * Undoes all changes made in the current transaction and releases any
	 * database locks currently held by this Connection object. This method
	 * sends the actual command to the server to make it effective.
	 *
	 * @throws SQLException if a database access error occurs
	 */
	void sendRollback() throws SQLException {	
		// send commit to the server
		synchronized(monet) {
			try {
				// make sure we have the prompt
				monet.waitForPrompt();
				// send the commit command (note the S in front is a protocol issue)
				monet.writeln("SROLLBACK;");
				// wait for the prompt again
				monet.waitForPrompt();
			} catch (IOException e) {
				throw new SQLException(e.toString());
			}
		}
	}
	
	/**
	 * Adds a warning to the pile of warnings this Connection object has. If
	 * there were no warnings (or clearWarnings was called) this warning will
	 * be the first, otherwise this warning will get appended to the current
	 * warning.
	 *
	 * @param reason the warning message
	 */
	void addWarning(String reason) {
		if (warnings == null) {
			warnings = new SQLWarning(reason);
		} else {
			warnings.setNextWarning(new SQLWarning(reason));
		}
	}
}
