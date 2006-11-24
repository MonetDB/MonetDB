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
 * Portions created by CWI are Copyright (C) 1997-2006 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.jdbc;

import java.sql.*;
import java.util.*;
import java.io.*;
import java.nio.*;
import java.security.*;
import nl.cwi.monetdb.jdbc.util.*;

/**
 * A Connection suitable for the MonetDB database.
 * <br /><br />
 * This connection represents a connection (session) to a MonetDB
 * database. SQL statements are executed and results are returned within
 * the context of a connection. This Connection object holds a physical
 * connection to the MonetDB database.
 * <br /><br />
 * A Connection object's database should able to provide information
 * describing its tables, its supported SQL grammar, its stored
 * procedures, the capabilities of this connection, and so on. This
 * information is obtained with the getMetaData method.<br />
 * Note: By default a Connection object is in auto-commit mode, which
 * means that it automatically commits changes after executing each
 * statement. If auto-commit mode has been disabled, the method commit
 * must be called explicitly in order to commit changes; otherwise,
 * database changes will not be saved.
 * <br /><br />
 * The current state of this connection is that it nearly implements the
 * whole Connection interface.<br />
 * Additionally, the static method getEmbeddedInstanceConnection()
 * provides a Connection for embedded situations, where an embedded
 * Mserver is started and used.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 1.2
 */
public class MonetConnection implements Connection {
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
	private final MonetSocketBlockMode monet;

	/** Whether this Connection is closed (and cannot be used anymore) */
	private boolean closed;

	/** Whether this Connection is in autocommit mode */
	private boolean autoCommit = true;

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

	/** A template to apply to each query (like pre and post fixes) */
	String[] queryTempl;
	/** A template to apply to each command (like pre and post fixes) */
	String[] commandTempl;

	/** the SQL language */
	final static int LANG_SQL = 0;
	/** the XQuery language */
	final static int LANG_XQUERY = 1;
	/** the MIL language (officially *NOT* supported) */
	final static int LANG_MIL = 2;
	/** an unknown language */
	final static int LANG_UNKNOWN = -1;
	/** The language which is used */
	final int lang;

	/** Query types (copied from sql_query.mx) */
	final static int Q_PARSE	= '0';
	final static int Q_TABLE	= '1';
	final static int Q_UPDATE	= '2';
	final static int Q_SCHEMA	= '3';
	final static int Q_TRANS	= '4';
	final static int Q_PREPARE	= '5';
	final static int Q_BLOCK	= '6';

	/** Embedded instance */
	private static MonetEmbeddedInstance embeddedInstance = null;
	/** Embedded properties */
	private static Properties embeddedProps = null;


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
	MonetConnection(
		Properties props)
		throws SQLException, IllegalArgumentException, MonetRedirectException
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

		// initialise query templates (filled later, but needed below)
		queryTempl = new String[3]; // pre, post, sep
		commandTempl = new String[3]; // pre, post, sep

		try {
			monet = new MonetSocketBlockMode(hostname, port);

			/*
			 * There is no need for a lock on the monet object here.
			 * Since we just created the object, and the reference to
			 * this object has not yet been returned to the caller,
			 * noone can (in a legal way) know about the object.
			 */

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

					monet.debug(f.getAbsolutePath());
				} catch (IOException ex) {
					throw new SQLException("Opening logfile failed: " + ex.getMessage());
				}
			}

			// log in
			String challenge = null;

			// read the challenge from the server
			byte[] chal = new byte[2];
			monet.read(chal);
			int len = 0;
			try {
				len = Integer.parseInt(new String(chal, "UTF-8"));
			} catch (NumberFormatException e) {
				throw new SQLException("Server challenge length unparsable " +
						"(" + new String(chal, "UTF-8") + ")");
			}
			// read the challenge string
			chal = new byte[len];
			monet.read(chal);

			challenge = new String(chal, "UTF-8");

			sendChallengeResponse(
					monet,
					challenge,
					username,
					password,
					language,
					database,
					hash
				);

			// read monet response till prompt
			List redirects = null;
			String err = "", tmp;
			int lineType = 0;
			while (lineType != MonetSocketBlockMode.PROMPT1) {
				if ((tmp = monet.readLine()) == null)
					throw new IOException("Connection to server lost!");
				if ((lineType = monet.getLineType()) == MonetSocketBlockMode.ERROR) {
					err += "\n" + tmp.substring(1);
				} else if (lineType == MonetSocketBlockMode.REDIRECT) {
					if (redirects == null)
						redirects = new ArrayList();
					redirects.add(tmp.substring(1));
				}
			}
			if (err != "") {
				monet.disconnect();
				throw new SQLException(err.trim());
			}
			if (redirects != null) {
				monet.disconnect();
				throw new MonetRedirectException(redirects);
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

		// fill the query templates
		if (lang == LANG_SQL) {
			queryTempl[0] = "s";		// pre
			queryTempl[1] = ";";		// post
			queryTempl[2] = ";\n";		// separator

			commandTempl[0] = "X";		// pre
			commandTempl[1] = null;		// post
			commandTempl[2] = "\nX";	// separator
		} else if (lang == LANG_XQUERY) {
			queryTempl[0] = "s";
			queryTempl[1] = null;
			queryTempl[2] = ",";

			commandTempl[0] = "X";		// pre
			commandTempl[1] = null;		// post
			commandTempl[2] = "\nX";	// separator
		} else if (lang == LANG_MIL) {
			queryTempl[0] = null;
			queryTempl[1] = ";";
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
			int offset = (cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET)) / (60 * 1000);
			String tz = offset < 0 ? "-" : "+";
			tz += (Math.abs(offset) / 60 < 10 ? "0" : "") + (Math.abs(offset) / 60) + ":";
			offset -= (offset / 60) * 60;
			tz += (offset < 10 ? "0" : "") + offset;
			sendIndependantCommand("SET TIME ZONE INTERVAL '" + tz + "' HOUR TO MINUTE");
		}
		// the following initialisers are only valid when the language
		// is XQUERY...
		if (lang == LANG_XQUERY) {
			// output format 
			sendControlCommand("output seq");
		}

		// we're absolutely not closed, since we're brand new
		closed = false;
	}

	/**
	 * A little helper function that processes a challenge string, and
	 * returns a response string for the server.  If the challenge
	 * string is null, a challengeless response is returned.
	 *
	 * @param monet the MonetSocketBlockMode stream to write to
	 * @param chalstr the challenge string
	 * @param username the username to use
	 * @param password the password to use
	 * @param language the language to use
	 * @param database the database to connect to
	 * @param hash the hash method(s) to use, or NULL for all supported
	 *             hashes
	 */
	private void sendChallengeResponse(
			MonetSocketBlockMode monet,
			String chalstr,
			String username,
			String password,
			String language,
			String database,
			String hash
	) throws SQLException, IOException {
		int version = 0;
		String response;
		
		// parse the challenge string, split it on ':'
		String[] chaltok = chalstr.trim().split(":");
		if (chaltok.length < 4) throw
			new SQLException("Server challenge string unusable!");

		// challenge string to use as salt/key
		String challenge = chaltok[1];
		// chaltok[2]; // server type, not needed yet 
		try {
			version = Integer.parseInt(chaltok[3].trim());	// protocol version
		} catch (NumberFormatException e) {
			throw new SQLException("Protocol version unparseable: " + chaltok[3]);
		}

		// handle the challenge according to the version it is
		switch (version) {
			default:
				throw new SQLException("Unsupported protocol version: " + version);
			case 6:
				response = username + ":" + password + ":" + language;
				response += ":blocked" + ":" + database;

				monet.write(response + "\n");

				// We need to send the server our byte order.  Java by
				// itself uses network order.
				// A short with value 1234 will be sent to indicate our
				// byte-order.
				/*
				short x = 1234;
				byte high = (byte)(x >>> 8);	// = 0x04
				byte low = (byte)x;				// = 0xD2
				*/
				final byte[] bigEndian = {(byte)0x04, (byte)0xD2};
				monet.write(bigEndian);
				monet.flush();

				// now read the byte-order of the server
				byte[] byteorder = new byte[2];
				if (monet.read(byteorder) != 2)
					throw new SQLException("The server sent an incomplete byte-order sequence");
				if (byteorder[0] == (byte)0x04) {
					// set our connection to big-endian mode
					monet.setByteOrder(ByteOrder.BIG_ENDIAN);
				} else if (byteorder[0] == (byte)0xD2) {
					// set our connection to litte-endian mode
					monet.setByteOrder(ByteOrder.LITTLE_ENDIAN);
				} else if (byteorder[0] == '!') {
					// we have an error message, try to read a reasonable
					// chunk and spit it out
					byte[] buf = new byte[1024];
					int len = monet.read(buf);
					monet.disconnect();
					throw new SQLException(new String(byteorder, 1, 1, "UTF-8") +
							new String(buf, 0, len, "UTF-8"));
				} else {
					// we have something else then a handshake here
					throw new SQLException("The server sent an invalid byte-order sequence");
				}
			break;
			case 7:
				// proto 7 (finally) uses the challenge and works with a
				// password hash.  The supported implementations come
				// from the server challenge.  We chose the best hash
				// we can find, in the order SHA1, MD5, plain.  Also,
				// the byte-order is reported in the challenge string,
				// which makes sense, since only blockmode is supported.
				String hashes = (hash == null ? chaltok[4] : hash);
				String pwhash;
				if (hashes.indexOf("SHA1") != -1) {
					try {
						MessageDigest md = MessageDigest.getInstance("SHA-1");
						md.update(password.getBytes("UTF-8"));
						md.update(challenge.getBytes("UTF-8"));
						byte[] digest = md.digest();
						pwhash = "{SHA1}" + toHex(digest);
					} catch (NoSuchAlgorithmException e) {
						throw new AssertionError("internal error: " + e.toString());
					} catch (UnsupportedEncodingException e) {
						throw new AssertionError("internal error: " + e.toString());
					}
				} else if (hashes.indexOf("MD5") != -1) {
					try {
						MessageDigest md = MessageDigest.getInstance("MD5");
						md.update(password.getBytes("UTF-8"));
						md.update(challenge.getBytes("UTF-8"));
						byte[] digest = md.digest();
						pwhash = "{MD5}" + toHex(digest);
					} catch (NoSuchAlgorithmException e) {
						throw new AssertionError("internal error: " + e.toString());
					} catch (UnsupportedEncodingException e) {
						throw new AssertionError("internal error: " + e.toString());
					}
				} else if (hashes.indexOf("plain") != -1) {
					pwhash = "{plain}" + password + challenge;
				} else {
					throw new SQLException("no supported password hashes in " + hashes);
				}
				if (chaltok[5].equals("1234")) {
					// byte-order of server is big-endian
					monet.setByteOrder(ByteOrder.BIG_ENDIAN);
				} else if (chaltok[5].equals("3412")) {
					// byte-order of server is little-endian
					monet.setByteOrder(ByteOrder.LITTLE_ENDIAN);
				} else {
					throw new SQLException("Invalid byte-order: " + chaltok[5]);
				}

				// generate response
				response = "1234:";	// JVM byte-order is big-endian
				response += username + ":" + pwhash + ":" + language;
				response += ":" + database;

				monet.write(response + "\n");
				monet.flush();
			break;
		}
	}

	/**
	 * Small helper method to convert a byte string to a hexadecimal
	 * string representation.
	 *
	 * @param digest the byte array to convert
	 * @return the byte array as hexadecimal string
	 */
	private static String toHex(byte[] digest) {
		StringBuffer r = new StringBuffer(digest.length * 2);
		for (int i = 0; i < digest.length; i++) {
			// zero out higher bits to get unsigned conversion
			int b = digest[i] << 24 >>> 24;
			if (b < 16) r.append("0");
			r.append(Integer.toHexString(b));
		}
		return(r.toString());
	}

	//== methods of interface Connection

	/**
	 * Clears all warnings reported for this Connection object. After a
	 * call to this method, the method getWarnings returns null until a
	 * new warning is reported for this Connection object.
	 */
	public void clearWarnings() {
		warnings = null;
	}

	/**
	 * Releases this Connection object's database and JDBC resources
	 * immediately instead of waiting for them to be automatically
	 * released. All Statements created from this Connection will be
	 * closed when this method is called.
	 * <br /><br />
	 * Calling the method close on a Connection object that is already
	 * closed is a no-op.
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
		// report ourselves as closed
		closed = true;
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
	 * Creates a Statement object for sending SQL statements to the
	 * database.  SQL statements without parameters are normally
	 * executed using Statement objects. If the same SQL statement is
	 * executed many times, it may be more efficient to use a
	 * PreparedStatement object.
	 * <br /><br />
	 * Result sets created using the returned Statement object will by
	 * default be type TYPE_FORWARD_ONLY and have a concurrency level of
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
	 * Retrieves the current auto-commit mode for this Connection
	 * object.
	 *
	 * @return the current state of this Connection object's auto-commit
	 *         mode
	 * @see #setAutoCommit(boolean)
	 */
	public boolean getAutoCommit() throws SQLException {
		return(autoCommit);
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
	
	/**
	 * Retrieves the current holdability of ResultSet objects created
	 * using this Connection object.
	 *
	 * @return the holdability, one of
	 *         ResultSet.HOLD_CURSORS_OVER_COMMIT or
	 *         ResultSet.CLOSE_CURSORS_AT_COMMIT
	 * @throws SQLException is a database access error occors
	 */
	public int getHoldability() {
		// TODO: perhaps it is better to have the server implement
		//       CLOSE_CURSORS_AT_COMMIT
		return(ResultSet.HOLD_CURSORS_OVER_COMMIT);
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
	public DatabaseMetaData getMetaData() throws SQLException {
		if (lang != LANG_SQL)
			throw new SQLException("This method is only supported in SQL mode");

		return(new MonetDatabaseMetaData(this));
	}

	/**
	 * Retrieves this Connection object's current transaction isolation
	 * level.
	 *
	 * @return the current transaction isolation level, which will be
	 *         Connection.TRANSACTION_SERIALIZABLE
	 */
	public int getTransactionIsolation() {
		return(TRANSACTION_SERIALIZABLE);
	}

	/**
	 * Retrieves the Map object associated with this Connection object.
	 * Unless the application has added an entry, the type map returned
	 * will be empty.
	 *
	 * @return the java.util.Map object associated with this Connection
	 *         object
	 */
	public Map getTypeMap() {
		return(typeMap);
	}

	/**
	 * Retrieves the first warning reported by calls on this Connection
	 * object.  If there is more than one warning, subsequent warnings
	 * will be chained to the first one and can be retrieved by calling
	 * the method SQLWarning.getNextWarning on the warning that was
	 * retrieved previously.
	 * <br /><br />
	 * This method may not be called on a closed connection; doing so
	 * will cause an SQLException to be thrown.
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
	 * Retrieves whether this Connection object has been closed.  A
	 * connection is closed if the method close has been called on it or
	 * if certain fatal errors have occurred.  This method is guaranteed
	 * to return true only when it is called after the method
	 * Connection.close has been called.
	 * <br /><br />
	 * This method generally cannot be called to determine whether a
	 * connection to a database is valid or invalid.  A typical client
	 * can determine that a connection is invalid by catching any
	 * exceptions that might be thrown when an operation is attempted.
	 *
	 * @return true if this Connection object is closed; false if it is
	 *         still open
	 */
	public boolean isClosed() {
		return(closed);
	}

	/**
	 * Retrieves whether this Connection object is in read-only mode.
	 * MonetDB currently doesn't support updateable result sets.
	 *
	 * @return true if this Connection object is read-only; false otherwise
	 */
	public boolean isReadOnly() {
		return(true);
	}

	public String nativeSQL(String sql) {return(sql);}
	public CallableStatement prepareCall(String sql) {return(null);}
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {return(null);}
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {return(null);}

	/**
	 * Creates a PreparedStatement object for sending parameterized SQL
	 * statements to the database.
	 * <br /><br />
	 * A SQL statement with or without IN parameters can be pre-compiled
	 * and stored in a PreparedStatement object. This object can then be
	 * used to efficiently execute this statement multiple times.
	 * <br /><br />
	 * Note: This method is optimized for handling parametric SQL
	 * statements that benefit from precompilation. If the driver
	 * supports precompilation, the method prepareStatement will send
	 * the statement to the database for precompilation. Some drivers
	 * may not support precompilation. In this case, the statement may
	 * not be sent to the database until the PreparedStatement object is
	 * executed. This has no direct effect on users; however, it does
	 * affect which methods throw certain SQLException objects.
	 * <br /><br />
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
	public PreparedStatement prepareStatement(
		String sql,
		int resultSetType,
		int resultSetConcurrency)
		throws SQLException
	{
		try {
			PreparedStatement ret;
			// use a server-side PreparedStatement
			ret = new MonetPreparedStatement(
					this, resultSetType, resultSetConcurrency, sql
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
	 * Removes the given Savepoint object from the current transaction.
	 * Any reference to the savepoint after it have been removed will
	 * cause an SQLException to be thrown.
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
	 * <br /><br />
	 * This method should be used only when auto-commit has been
	 * disabled.
	 *
	 * @param savepoint the Savepoint object to roll back to
	 * @throws SQLException if a database access error occurs, the
	 *         Savepoint object is no longer valid, or this Connection
	 *         object is currently in auto-commit mode
	 */
	public void rollback(Savepoint savepoint) throws SQLException {
		if (!(savepoint instanceof MonetSavepoint)) throw
			new SQLException("This driver can only handle savepoints it created itself");

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
	 * <br /><br />
	 * The commit occurs when the statement completes or the next
	 * execute occurs, whichever comes first. In the case of statements
	 * returning a ResultSet object, the statement completes when the
	 * last row of the ResultSet object has been retrieved or the
	 * ResultSet object has been closed. In advanced cases, a single
	 * statement may return multiple results as well as output parameter
	 * values. In these cases, the commit occurs when all results and
	 * output parameter values have been retrieved.
	 * <br /><br />
	 * NOTE: If this method is called during a transaction, the
	 * transaction is committed.
	 *
 	 * @param autoCommit true to enable auto-commit mode; false to disable it
	 * @throws SQLException if a database access error occurs
	 * @see #getAutoCommit()
	 */
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
	public void setCatalog(String catalog) throws SQLException {
		// silently ignore this request
	}

	public void setHoldability(int holdability) {}

	/**
	 * Puts this connection in read-only mode as a hint to the driver to
	 * enable database optimizations.  MonetDB doesn't support writable
	 * ResultSets, hence an SQLException is thrown if attempted to set
	 * to false here.
	 *
	 * @param readOnly true enables read-only mode; false disables it
	 * @throws SQLException if a database access error occurs or this
	 *         method is called during a transaction or set to false.
	 */
	public void setReadOnly(boolean readOnly) throws SQLException {
		if (autoCommit == false) throw
			new SQLException("changing read-only setting not allowed during transactions");
		if (readOnly == false) throw
			new SQLException("writable mode not supported");
	}

	/**
	 * Creates an unnamed savepoint in the current transaction and
	 * returns the new Savepoint object that represents it.
	 *
	 * @return the new Savepoint object
	 * @throws SQLException if a database access error occurs or this Connection
	 *         object is currently in auto-commit mode
	 */
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

		return(sp);
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
	public Savepoint setSavepoint(String name) throws SQLException {
		// create a new Savepoint object
		MonetSavepoint sp;
		try {
			sp = new MonetSavepoint(name);
		} catch (IllegalArgumentException e) {
			throw new SQLException(e.getMessage());
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

		return(sp);
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
	public void setTransactionIsolation(int level) {
		if (level != TRANSACTION_SERIALIZABLE) {
			addWarning("MonetDB only supports fully serializable " +
					"transactions, continuing with transaction level " +
					"raised to TRANSACTION_SERIALIZABLE");
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
	public void setTypeMap(Map map) {
		typeMap = map;
	}

	/**
	 * Returns a string identifying this Connection to the MonetDB
	 * server.
	 *
	 * @return a String representing this Object
	 */
	public String toString() {
		String language = "";
		if (lang == LANG_XQUERY) language = "?language=xquery";
		else if (lang == LANG_MIL) language = "?language=mil";
		return("MonetDB Connection (jdbc:monetdb://" + hostname +
				":" + port + "/" + database + language + ") " + 
				(closed ? "connected" : "disconnected"));
	}

	//== end methods of interface Connection

	/**
	 * Sends the given string to MonetDB as regular statement, making
	 * sure there is a prompt after the command is sent.  All possible
	 * returned information is discarded.  Encountered errors are
	 * reported.
	 *
	 * @param command the exact string to send to MonetDB
	 * @throws SQLException if an IO exception or a database error occurs
	 */
	void sendIndependantCommand(String command) throws SQLException {
		synchronized (monet) {
			try {
				monet.writeLine(queryTempl, command);
				String error = monet.waitForPrompt();
				if (error != null) throw new SQLException(error);
			} catch (IOException e) {
				throw new SQLException(e.getMessage());
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
		synchronized (monet) {
			try {
				monet.writeLine(commandTempl, command);
				String error = monet.waitForPrompt();
				if (error != null) throw new SQLException(error);
			} catch (IOException e) {
				throw new SQLException(e.getMessage());
			}
		}
	}

	/**
	 * Sends the given string to MonetDB as data; it is sent using a
	 * special Xcopy mode of the server, allowing not to make any
	 * changes to the given in String.  A Response is returned, just
	 * like for a normal query.
	 *
	 * @param in the exact string to send to MonetDB
	 * @param name the name to associate to the data in in
	 * @return A Response object, or null if no response
	 * @throws SQLException if a database error occurs
	 */
	Response copyToServer(String in, String name) throws SQLException {
		synchronized (monet) {
			// TODO: maybe in the future add support for a second
			// name which is a convenience "alias" (e.g. in XQuery)
			// tell server we're going to "copy" data over to be
			// stored under the given name, make up something if the
			// caller doesn't know it too
			if (name == null) {
				name = "doc_" + System.currentTimeMillis() + ".xml";
				addWarning("adding new document with name: " + name);
			}
			sendControlCommand("copy " + name);
			// the server is waiting for data to come
			String[] templ = new String[3];	// empty on everything
			ResponseList l = new ResponseList(
					0,
					0,
					ResultSet.FETCH_FORWARD,
					ResultSet.CONCUR_READ_ONLY
			);
			try {
				l.executeQuery(templ, in);
				return(l.getNextResponse()); // don't you love Java?
			} finally {
				l.close();
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
	void addWarning(String reason) {
		if (warnings == null) {
			warnings = new SQLWarning(reason);
		} else {
			warnings.setNextWarning(new SQLWarning(reason));
		}
	}

	/** the default number of rows that are (attempted to) read at once */
	private final static int DEF_FETCHSIZE = 250;
	/** The sequence counter */
	private static int seqCounter = 0;

	/** An optional thread that is used for sending large queries */
	private SendThread sendThread = null;

	/**
	 * Returns the numeric value in the given CharBuffer.  The value is
	 * considered to end at the end of the CharBuffer or at a space.  If
	 * a non-numeric character is encountered a ParseException is
	 * thrown.
	 *
	 * @param str CharBuffer to read from
	 * @throws java.text.ParseException if no numeric value could be
	 * read
	 */
	private final int parseNumber(CharBuffer str)
		throws java.text.ParseException
	{
		if (!str.hasRemaining()) throw
			new java.text.ParseException("Unexpected end of string", str.position() - 1);
		int tmp = MonetResultSet.getIntrinsicValue(str.get(), str.position() - 1);
		char chr;
		while (str.hasRemaining() && (chr = str.get()) != ' ') {
			tmp *= 10;
			tmp += MonetResultSet.getIntrinsicValue(chr, str.position() - 1);
		}

		return(tmp);
	}

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
				cacheSize = MonetConnection.DEF_FETCHSIZE;
				cacheSizeSetExplicitly = false;
			} else {
				cacheSize = parent.cachesize;
				cacheSizeSetExplicitly = true;
			}
			seqnr = seq;
			closed = false;
			destroyOnClose = false;

			this.id = id;
			this.tuplecount = tuplecount;
			this.columncount = columncount;
			this.resultBlocks =
				new DataBlockResponse[(tuplecount / cacheSize) + 1];

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
		public String addLine(String tmpLine, int linetype) {
			if (isSet[LENS] && isSet[TYPES] &&
					isSet[TABLES] && isSet[NAMES])
			{
				return(resultBlocks[0].addLine(tmpLine, linetype));
			}

			if (linetype != MonetSocketBlockMode.HEADER)
				return("header expected, got: " + tmpLine);

			char[] chrLine = tmpLine.toCharArray();
			int len = chrLine.length;

			int pos = 0;
			boolean foundChar = false;
			boolean nameFound = false;
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
						nameFound = true;
						if (pos == 0) pos = i + 1;
						i = 0;	// force the loop to terminate
					break;
					default:
						foundChar = true;
						pos = 0;
					break;
				}
			}
			if (!nameFound)
				return("illegal header: " + tmpLine);

			// depending on the name of the header, we continue
			switch (chrLine[pos]) {
				default:
					return("protocol violation: unknown header: " +
							(new String(chrLine, pos, len - pos)));
				case 'n':
					if (len - pos == 4 &&
							tmpLine.regionMatches(pos + 1, "name", 1, 3))
					{
						name = getValues(chrLine, 2, pos - 3);
						isSet[NAMES] = true;
					} else {
						return("protocol violation: unknown header: " +
								(new String(chrLine, pos, len - pos)));
					}
				break;
				case 'l':
					if (len - pos == 6 &&
							tmpLine.regionMatches(pos + 1, "length", 1, 5))
					{
						try {
							columnLengths = getIntValues(chrLine, 2, pos - 3);
							isSet[LENS] = true;
						} catch (SQLException e) {
							return(e.getMessage());
						}
					} else {
						return("protocol violation: unknown header: " +
								(new String(chrLine, pos, len - pos)));
					}
				break;
				case 't':
					if (len - pos == 4 &&
							tmpLine.regionMatches(pos + 1, "type", 1, 3))
					{
						type = getValues(chrLine, 2, pos - 3);
						isSet[TYPES] = true;
					} else if (len - pos == 10 &&
							tmpLine.regionMatches(pos + 1, "table_name", 1, 9))
					{
						tableNames = getValues(chrLine, 2, pos - 3);
						isSet[TABLES] = true;
					} else {
						return("protocol violation: unknown header: " +
								(new String(chrLine, pos, len - pos)));
					}
				break;
			}

			// all is well
			return(null);
		}
		// }}}

		/**
		 * Returns whether this ResultSetResponse needs more lines.
		 * This method returns true if not all headers are set, or the
		 * first DataBlockResponse reports to want more.
		 */
		public boolean wantsMore() {
			if (isSet[LENS] && isSet[TYPES] &&
					isSet[TABLES] && isSet[NAMES])
			{
				return(resultBlocks[0].wantsMore());
			} else {
				return(true);
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

			return(values);
		}

		/**
		 * Returns an array of ints containing the values between
		 * ',\t' separators.
		 *
		 * @param chrLine a character array holding the input data
		 * @param start where the relevant data starts
		 * @param stop where the relevant data stops
		 * @return an array of ints
		 */
		final private int[] getIntValues(
				char[] chrLine,
				int start,
				int stop
			) throws SQLException
		{
			int elem = 0;
			int tmp = 0;
			int[] values = new int[columncount];

			try {
				for (int i = start; i < stop; i++) {
					if (chrLine[i] == ',' && chrLine[i + 1] == '\t') {
						values[elem++] = tmp;
						tmp = 0;
						start = ++i;
					} else {
						tmp *= 10;
						tmp += MonetResultSet.getIntrinsicValue(chrLine[i], i);
					}
				}
				// at the left over part
				values[elem++] = tmp;
			} catch (java.text.ParseException e) {
				throw new SQLException(e.getMessage() +
						" found: '" + chrLine[e.getErrorOffset()] + "'" +
						" in: " + new String(chrLine) +
						" at pos: " + e.getErrorOffset());
			}

			return(values);
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
		public void complete() throws SQLException {
			String error = "";
			if (!isSet[NAMES]) error += "name header missing\n";
			if (!isSet[TYPES]) error += "type header missing\n";
			if (!isSet[TABLES]) error += "table name header missing\n";
			if (!isSet[LENS]) error += "column width header missing\n";
			if (error != "") throw new SQLException(error);
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
		 * Returns the cache size used within this Response
		 *
		 * @return the cache size
		 */
		int getCacheSize() {
			return(cacheSize);
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
		 * Returns the ResultSet type, FORWARD_ONLY or not.
		 *
		 * @return the ResultSet type
		 */
		int getRSType() {
			return(parent.rstype);
		}

		/**
		 * Returns the concurrency of the ResultSet.
		 *
		 * @return the ResultSet concurrency
		 */
		int getRSConcur() {
			return(parent.rsconcur);
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

							// Java string are UTF-16, right?
							long free = Runtime.getRuntime().freeMemory() / 2;
							// we run the risk that we kill ourselves
							// here, but maybe that's better to continue
							// as long as possible, than asking too much
							// too soon
							if (cacheSize > free)
								cacheSize = (int)free;	// it must fit

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
				parent.executeQuery(
						commandTempl, 
						"export " + id + " " + ((block * cacheSize) + blockOffset) + " " + cacheSize 
				);
				rawr = resultBlocks[block];
				if (rawr == null) throw
					new AssertionError("block " + block + " should have been fetched by now :(");
			}

			return(rawr.getRow(blockLine));
		}

		/**
		 * Closes this Response by sending an Xclose to the server indicating
		 * that the result can be closed at the server side as well.
		 */
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
			return(closed);
		}


		protected void finalize() throws Throwable {
			close();
			super.finalize();
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
	 * <br /><br />
	 * This object is not intended to be queried by multiple threads
	 * synchronously. It is designed to work for one thread retrieving
	 * rows from it.  When multiple threads will retrieve rows from this
	 * object, it is possible for threads to get the same data.
	 */
	// {{{ DataBlockResponse class implementation
	class DataBlockResponse implements Response {
		/** The String array to keep the data in */
		private String[] data;

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
		public String addLine(String line, int linetype) {
			if (linetype != MonetSocketBlockMode.RESULT)
				return("protocol violation: unexpected line in data block: " + line);
			// add to the backing array
			data[++pos] = line;

			// all is well
			return(null);
		}

		/**
		 * Returns whether this Reponse expects more lines to be added
		 * to it.
		 *
		 * @return true if a next line should be added, false otherwise
		 */
		public boolean wantsMore() {
			// remember: pos is the value already stored
			return(pos + 1 < data.length);
		}

		/**
		 * Indicates that no more header lines will be added to this
		 * Response implementation.  In most cases this is a redundant
		 * operation because the data array is full.  However... it can
		 * happen that this is NOT the case!
		 *
		 * @throws SQLException if not all rows are filled
		 */
		public void complete() throws SQLException {
			if ((pos + 1) != data.length) throw
				new SQLException("Inconsistent state detected!  Current block capacity: " + data.length + ", block usage: " + (pos + 1) + ".  Did MonetDB send what it promised to?");
		}

		/**
		 * Instructs the Response implementation to close and do the
		 * necessary clean up procedures.
		 *
		 * @throws SQLException
		 */
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
				return(ret);
			} else {
				return(data[line]);
			}
		}
	}
	// }}}

	/**
	 * The AffectedRowsResponse represents an update or schema message.
	 * It keeps an additional count field that represents the affected
	 * rows for update statements, and a success flag (negative numbers,
	 * actually) for schema messages.<br />
	 * <tt>&amp;2 af</tt>
	 */
	// {{{ AffectedRowsResponse class implementation
	class AffectedRowsResponse implements Response {
		public final int count;
		
		public AffectedRowsResponse(int cnt) {
			// fill the blank final
			this.count = cnt;
		}

		public String addLine(String line, int linetype) {
			return("Header lines are not supported for an AffectedRowsResponse");
		}

		public boolean wantsMore() {
			return(false);
		}

		public void complete() {
			// we're empty, because we don't need to check anything...
		}

		public void close() {
			// nothing to do here...
		}
	}
	// }}}

	/**
	 * The AutoCommitResponse represents a transaction message.  It
	 * stores (a change in) the server side auto commit mode.<br />
	 * <tt>&amp;3 (t|f)</tt>
	 */
	// {{{ AutoCommitResponse class implementation
	class AutoCommitResponse extends AffectedRowsResponse {
		public final boolean autocommit;
		
		public AutoCommitResponse(boolean ac) {
			super(Statement.SUCCESS_NO_INFO);
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
		private List responses;
		/** A map of ResultSetResponses, used for additional
		 *  DataBlockResponse mapping */
		private Map rsresponses;

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
			responses = new ArrayList();
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
			curResponse++;
			if (curResponse >= responses.size()) {
				// ResponseList is obviously completed so, there are no
				// more responses
				return(null);
			} else {
				// return this response
				return((Response)(responses.get(curResponse)));
			}
		}

		/**
		 * Closes the Reponse at index i, if not null.
		 *
		 * @param i the index position of the header to close
		 */
		void closeResponse(int i) {
			if (i < 0 || i >= responses.size()) return;
			Response tmp = (Response)(responses.get(i));
			if (tmp != null) tmp.close();
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

		protected void finalize() throws Throwable {
			close();
			super.finalize();
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
				// make sure we're ready to send query; read data till we
				// have the prompt it is possible (and most likely) that we
				// already have the prompt and do not have to skip any
				// lines.  Ignore errors from previous result sets.
				monet.waitForPrompt();

				int size;
				// {{{ set reply size
				/**
				 * Change the reply size of the server.  If the given
				 * value is the same as the current value known to use,
				 * then ignore this call.  If it is set to 0 we get a
				 * prompt after the server sent it's header.
				 */
				size = cachesize == 0 ? DEF_FETCHSIZE : cachesize;
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
				if (query.length() > MonetSocketBlockMode.BLOCK) {
					// get a reference to the send thread
					if (sendThread == null) sendThread = new SendThread(monet);
					// tell it to do some work!
					sendThread.runQuery(templ, query);
					sendThreadInUse = true;
				} else {
					// this is a simple call, which is a lot cheaper and will
					// always succeed for small queries.
					monet.writeLine(templ, query);
				}

				// go for new results
				String tmpLine = monet.readLine();
				int linetype = monet.getLineType();
				Response res = null;
				while (linetype != MonetSocketBlockMode.PROMPT1) {
					// each response should start with a start of header
					// (or error)
					switch (linetype) {
						case MonetSocketBlockMode.SOHEADER:
							// make the response object, and fill it
							// {{{ soh line parsing
							// parse the start of header line
							CharBuffer soh = CharBuffer.wrap(tmpLine);
							soh.get();	// skip the &
							try {
								switch (soh.get()) {
									default:
										throw new java.text.ParseException("protocol violation: unknown header", 1);
									case Q_PARSE:
										throw new java.text.ParseException("Q_PARSE header not allowed here", 1);
									case Q_TABLE:
									case Q_PREPARE: {
										soh.get();	// skip space
										int id = parseNumber(soh);
										int tuplecount = parseNumber(soh);
										int columncount = parseNumber(soh);
										int rowcount = parseNumber(soh);
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
										// the hashmap if can possibly
										// have an additional datablock
										if (rowcount < tuplecount) {
											if (rsresponses == null)
												rsresponses = new HashMap();
											rsresponses.put(
													new Integer(id),
													res
											);
										}
									} break;
									case Q_UPDATE:
										soh.get();	// skip space
										res = new AffectedRowsResponse(
												parseNumber(soh)	// count
												);
										break;
									case Q_SCHEMA:
										res = new AffectedRowsResponse(
												Statement.SUCCESS_NO_INFO
												);
										break;
									case Q_TRANS:
										soh.get();	// skip space
										if (soh.position() == soh.length()) throw
											new java.text.ParseException("unexpected end of string", soh.position() - 1);
										boolean ac = soh.get() == 't' ? true : false;
										if (autoCommit && ac) {
											addWarning("Server enabled auto commit " +
													"mode while local state " +
													"already was auto commit."
													);
										}
										autoCommit = ac;
										// note: the use of a special header
										// here is not really clear.  Maybe
										// ditch it and use
										// AffectedRowsResponse (which
										// is a superclass of
										// AutoCommitResponse anyway).
										res = new AutoCommitResponse(
												ac
										);
									break;
									case Q_BLOCK: {
										// a new block of results for a
										// response...
										soh.get();	// skip space
										int id = parseNumber(soh); 
										parseNumber(soh);	// columncount
										int rowcount = parseNumber(soh);
										int offset = parseNumber(soh);
										ResultSetResponse t =
											(ResultSetResponse)rsresponses.get(new Integer(id));
										if (t == null) {
											error = "No ResultSetResponse with id " + id + " found";
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
							} catch (java.text.ParseException e) {
								error = "error while parsing start of header:\n" +
									e.getMessage() +
									" found: '" + soh.get(e.getErrorOffset()) + "'" +
									" in: \"" + tmpLine + "\"" +
									" at pos: " + e.getErrorOffset();
								// flush all the rest
								monet.waitForPrompt();
								linetype = monet.getLineType();
								break;
							}
							// }}} soh line parsing

							// immediately handle errors after parsing
							// the header (res may be null)
							if (error != null) {
								monet.waitForPrompt();
								linetype = monet.getLineType();
								break;
							}

							// here we have a res object, which
							// we can start filling
							while (res.wantsMore()) {
								error = res.addLine(
										monet.readLine(),
										monet.getLineType()
								);
								if (error != null) {
									// right, some protocol violation,
									// skip the rest of the result
									monet.waitForPrompt();
									linetype = monet.getLineType();
									break;
								}
							}
							if (error != null) break;
							responses.add(res);

							// read the next line (can be prompt, new
							// result, error, etc.) before we start the
							// loop over
							tmpLine = monet.readLine();
							linetype = monet.getLineType();
						break;
						case MonetSocketBlockMode.INFO:
							addWarning(tmpLine.substring(1));

							// read the next line (can be prompt, new
							// result, error, etc.) before we start the
							// loop over
							tmpLine = monet.readLine();
							linetype = monet.getLineType();
						break;
						default:	// Yeah... in Java this is correct!
							// we have something we don't
							// expect/understand, let's make it an error
							// message
							tmpLine = "!protocol violation, unexpected line: " + tmpLine;
						case MonetSocketBlockMode.ERROR:
							// read everything till the prompt (should be
							// error) we don't know if we ignore some
							// garbage here... but the log should reveal
							// that
							error = monet.waitForPrompt();
							linetype = monet.getLineType();
							if (error != null) {
								error = tmpLine.substring(1) + "\n" + error;
							} else {
								error = tmpLine.substring(1);
							}
						break;
					}
				}

				// if we used the sendThread, make sure it has finished
				if (sendThreadInUse) {
					String tmp = sendThread.getErrors();
					if (tmp != null) {
						if (error == null) {
							error = tmp;
						} else {
							error += tmp;
						}
					}
				}
				if (error != null) throw new SQLException(error.trim());
			} catch (IOException e) {
				closed = true;
				throw new SQLException(e.getMessage() + " (Mserver still alive?)");
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
	 * high.<br />
	 * <br />
	 * NOTE: This thread is neither thread safe nor synchronised.  The
	 * reason for this is that program wise only one thread (the
	 * CacheThread) will use this thread, so costly locking mechanisms
	 * can be avoided.
	 */
	// {{{ SendThread class implementation
	class SendThread extends Thread {
		/** The state WAIT represents this thread to be waiting for
		 *  something to do */
		private final static int WAIT = 0;
		/** The state QUERY represents this thread to be executing a query */
		private final static int QUERY = 1;

		private String[] templ;
		private String query;
		private MonetSocketBlockMode conn;
		private String error;
		private int state = WAIT;

		/**
		 * Constructor which immediately starts this thread and sets it
		 * into daemon mode.
		 *
		 * @param monet the socket to write to
		 */
		public SendThread(MonetSocketBlockMode conn) {
			super("SendThread");
			setDaemon(true);
			this.conn = conn;
			start();
		}

		public synchronized void run() {
			while (true) {
				while (state == WAIT) {
					try {
						// wait requires the object to be exclusive
						// (synchronized)
						this.wait();
					} catch (InterruptedException e) {
						// woken up, eh?
					}
				}

				// state is QUERY here
				try {
					// we issue notify here, so incase we get blocked on IO
					// the thread that waits on us in runQuery can continue
					this.notify();
					conn.writeLine(templ, query);
				} catch (IOException e) {
					error = e.getMessage();
				}

				// update our state, and notify, maybe someone is waiting
				// for us in throwErrors
				state = WAIT;
				this.notify();
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
		public synchronized void runQuery(String[] templ, String query) 
			throws SQLException
		{
			if (state != WAIT) throw
				new SQLException("SendThread already in use!");

			this.templ = templ;
			this.query = query;

			// let the thread know there is some work to do
			state = QUERY;
			this.notify();

			// implement the following behaviour:
			// - let the SendThread first try to send whatever it can over
			//   the socket
			// - return as soon as the SendThread gets blocked
			// the effect is a relatively high chance of data waiting to be
			// read when returning from this method.
			try {
				this.wait();
			} catch (InterruptedException e) {
				// Woken up, eh?  Let's hope it's all good
			}
		}

		/**
		 * Returns errors encountered during the sending process.
		 *
		 * @return the errors or null if none
		 */
		public synchronized String getErrors() {
			// make sure the thread is in WAIT state, not QUERY
			while (state != WAIT) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					// just try again
				}
			}
			return (error);
		}
	}
	// }}}

	//=== embedded stuff (shouldn't hurt normal use)
	
	/**
	 * Sets the properties for the embedded connection.  The properties
	 * can only be set as long as there is no embedded connection made,
	 * afterwards calling this method will result in an SQLException.
	 *
	 * @param props the properties for the embedded server
	 * @throws SQLException if the embedded connection has already been
	 *         started
	 */
	public static synchronized void setEmbeddedProperties(Properties props)
		throws SQLException
	{
		if (embeddedInstance != null) throw
			new SQLException("embedded connection has already been started");

		embeddedProps = props;
	}

	/**
	 * Returns a Connection to an embedded Mserver instance.  The
	 * embedded instance is started if not running yet.  The
	 * specification for the to be started embedded Mserver instance can
	 * given via the setEmbeddedProperties() method.  Note that this
	 * method is synchronized to avoid race conditions.
	 *
	 * @return a Connection to an embedded instance
	 * @throws SQLException if a problem occurred while making the
	 *         connection
	 */
	public static synchronized Connection getEmbeddedInstanceConnection()
		throws SQLException
	{
		if (embeddedInstance == null || !embeddedInstance.isAlive())
			embeddedInstance = new MonetEmbeddedInstance(embeddedProps);

		return(embeddedInstance.getConnection());
	}

}

/**
 * The MonetEmbeddedInstance is a Thread wrapper around an Mserver
 * process, that is started by this Thread.  When the process dies, the
 * thread dies as well, printing the reason for dying to the standard
 * error stream.
 */
// {{{
class MonetEmbeddedInstance extends Thread {
	private String error;
	private final String[] cmdarray;
	private final String dbname;
	private final Process mserver;

	/**
	 * Constructor of a MonetEmbeddedInstance that requires a property
	 * value map to be given.  The properties are used to start the
	 * Mserver, and required.  The following two properties need to be
	 * available in the Properties map: <tt>executable</tt> and
	 * <tt>dbname</tt>.  The first one should be an (absolute) path to
	 * the Mserver executable, while the latter one should be a database
	 * name that will be used.  Optionally, the <tt>dbfarm</tt> and
	 * <tt>dbinit</tt> arguments can be present and will be passed to
	 * the Mserver invocation.
	 *
	 * @param props the Properties map
	 * @throws SQLException if executable or dbname is not present
	 */
	public MonetEmbeddedInstance(Properties props) throws SQLException {
		// check we get what we want
		String executable = props.getProperty("executable");
		if (executable == null) throw
			new SQLException("executable missing");
		dbname = props.getProperty("dbname");
		if (dbname == null) throw
			new SQLException("dbname missing");

		// see what we got more
		int cnt = 2;
		String dbfarm = props.getProperty("dbfarm");
		if (dbfarm != null) cnt++;
		String dbinit = props.getProperty("dbinit");
		if (dbinit != null) cnt++;

		cmdarray = new String[cnt];
		switch (cnt) {
			case 4:
				cmdarray[3] = "--dbinit=" + dbinit;
			case 3:	
				if (dbfarm != null) {
					cmdarray[2] = "--dbfarm=" + dbfarm;
				} else {
					cmdarray[2] = "--dbinit=" + dbinit;
				}
			default:
				cmdarray[1] = "--dbname=" + dbname;
				cmdarray[0] = executable;
		}

		// start the server and wait for its hello
		try {
			mserver = Runtime.getRuntime().exec(cmdarray);
			BufferedReader in =
				new BufferedReader(
						new InputStreamReader(mserver.getInputStream()));
			if (in.readLine() == null) throw
				new SQLException("embedded process died immediately");
		} catch (Exception e) {
			throw new SQLException(e.toString());
		}
		setDaemon(true);
		start();
		try {
			// hopefully this enough for the server to set up its socket
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// ok, too bad
		}
	}

	/**
	 * Main execution.  This function starts the Mserver using the given
	 * command array, and waits for it to terminate.  If there are
	 * errors written to the standard error pipe, they are written to
	 * the standard error channel of the console.
	 */
	public void run() {
		try {
			BufferedReader err =
				new BufferedReader(
						new InputStreamReader(mserver.getErrorStream()));
			
			// wait for the beast to die
			mserver.waitFor();
			String msg;
			while ((msg = err.readLine()) != null) {
				if (error == null) {
					error = msg + "\n";
				} else {
					error += msg + "\n";
				}
			}
		} catch (InterruptedException e) {
			// waitFor
			error = "caught interrupt while waiting for Mserver to crash";
		} catch (Exception e) {
			if (error == null) error = "";
			error += e.toString();
		}

		// Where to go with the error?
		System.err.println("embedded Mserver died:\n" + error);
	}

	/**
	 * Returns a Connection for the embedded Mserver instance.  This
	 * method "just" creates a TCP connection to the running Mserver,
	 * using administrator privileges.
	 *
	 * @return a Connection for the embedded Mserver instance
	 * @throws SQLException if making a connection fails
	 */
	public Connection getConnection() throws SQLException {
		try {
			Properties conProps = new Properties();
			conProps.setProperty("host", "localhost");
			conProps.setProperty("port", "50000");
			conProps.setProperty("language", "sql");
			conProps.setProperty("database", dbname);
			conProps.setProperty("user", "monetdb");
			conProps.setProperty("password", "monetdb");

			return(new MonetConnection(conProps));
		} catch (SQLException e) {
			// this is just to avoid the SQLException to be
			// rewrapped into another SQLException
			throw e;
		} catch (Exception e) {
			throw new SQLException(e.toString());
		}
	}
}
// }}}

// vim: foldmethod=marker:
