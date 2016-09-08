/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.client;

import nl.cwi.monetdb.util.*;
import java.sql.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.net.*;

/**
 * This program acts like an extended client program for MonetDB. Its
 * look and feel is very much like PostgreSQL's interactive terminal
 * program.  Although it looks like this client is designed for MonetDB,
 * it demonstrates the power of the JDBC interface since it built on top
 * of JDBC only.
 *
 * @author Fabian Groffen, Martin van Dinther
 * @version 1.3
 */

public final class JdbcClient {

	private static Connection con;
	private static Statement stmt;
	private static BufferedReader in;
	private static PrintWriter out;
	private static Exporter exporter;
	private static DatabaseMetaData dbmd;

	public final static void main(String[] args) throws Exception {
		CmdLineOpts copts = new CmdLineOpts();

		// arguments which take exactly one argument
		copts.addOption("h", "host", CmdLineOpts.CAR_ONE, "localhost",
				"The hostname of the host that runs the MonetDB database.  " +
				"A port number can be supplied by use of a colon, i.e. " +
				"-h somehost:12345.");
		copts.addOption("p", "port", CmdLineOpts.CAR_ONE, "50000",
				"The port number to connect to.");
		// todo make it CAR_ONE_MANY
		copts.addOption("f", "file", CmdLineOpts.CAR_ONE, null,
				"A file name to use either for reading or writing.  The " +
				"file will be used for writing when dump mode is used " +
				"(-D --dump).  In read mode, the file can also be an URL " +
				"pointing to a plain text file that is optionally gzip " +
				"compressed.");
		copts.addOption("u", "user", CmdLineOpts.CAR_ONE, System.getProperty("user.name"),
				"The username to use when connecting to the database.");
		// this one is only here for the .monetdb file parsing, it is
		// removed before the command line arguments are parsed
		copts.addOption(null, "password", CmdLineOpts.CAR_ONE, null, null);
		copts.addOption("d", "database", CmdLineOpts.CAR_ONE, "",
				"Try to connect to the given database (only makes sense " +
				"if connecting to monetdbd).");
		copts.addOption("l", "language", CmdLineOpts.CAR_ONE, "sql",
				"Use the given language, defaults to 'sql'.");

		// arguments which have no argument(s)
		copts.addOption(null, "help", CmdLineOpts.CAR_ZERO, null,
				"This help screen.");
		copts.addOption(null, "version", CmdLineOpts.CAR_ZERO, null,
				"Display driver version and exit.");
		copts.addOption("e", "echo", CmdLineOpts.CAR_ZERO, null,
				"Also outputs the contents of the input file, if any.");
		copts.addOption("q", "quiet", CmdLineOpts.CAR_ZERO, null,
				"Suppress printing the welcome header.");

		// arguments which have zero to many arguments
		copts.addOption("D", "dump", CmdLineOpts.CAR_ZERO_MANY, null,
				"Dumps the given table(s), or the complete database if " +
				"none given.");

		// extended options
		copts.addOption(null, "Xoutput", CmdLineOpts.CAR_ONE, null,
				"The output mode when dumping.  Default is sql, xml may " +
				"be used for an experimental XML output.");
		copts.addOption(null, "Xhash", CmdLineOpts.CAR_ONE, null,
				"Use the given hash algorithm during challenge response.  " +
				"Supported algorithm names: SHA1, MD5, plain.");
		// arguments which can have zero or one argument(s)
		copts.addOption(null, "Xdebug", CmdLineOpts.CAR_ZERO_ONE, null,
				"Writes a transmission log to disk for debugging purposes.  " +
				"If a file name is given, it is used, otherwise a file " +
				"called monet<timestamp>.log is created.  A given file " +
				"never be overwritten; instead a unique variation of the " +
				"file is used.");
		copts.addOption(null, "Xbatching", CmdLineOpts.CAR_ZERO_ONE, null,
				"Indicates that a batch should be used instead of direct " +
				"communication with the server for each statement.  If a " +
				"number is given, it is used as batch size.  i.e. 8000 " +
				"would execute the contents on the batch after each 8000 " +
				"statements read.  Batching can greatly speedup the " +
				"process of restoring a database dump.");

		// we store user and password in separate variables in order to
		// be able to properly act on them like forgetting the password
		// from the user's file if the user supplies a username on the
		// command line arguments
		String pass = null;
		String user = null;

		// look for a file called .monetdb in the current dir or in the
		// user's homedir and read its preferences
		File pref = new File(".monetdb");
		if (!pref.exists())
			pref = new File(System.getProperty("user.home"), ".monetdb");
		if (pref.exists()) {
			try {
				copts.processFile(pref);
			} catch (OptionsException e) {
				System.err.println("Error in " + pref.getAbsolutePath() + ": " + e.getMessage());
				System.exit(1);
			}
			user = copts.getOption("user").getArgument();
			pass = copts.getOption("password").getArgument();
		}

		// process the command line arguments, remove password option
		// first, and save the user we had at this point
		copts.removeOption("password");
		try {
			copts.processArgs(args);
		} catch (OptionsException e) {
			System.err.println("Error: " + e.getMessage());
			System.exit(1);
		}
		// we can actually compare pointers (objects) here
		if (user != copts.getOption("user").getArgument()) pass = null;

		if (copts.getOption("help").isPresent()) {
			System.out.print(
				"Usage java -jar jdbcclient.jar\n" +
				"                  [-h host[:port]] [-p port] [-f file] [-u user]\n" +
				"                  [-l language] [-d database] [-e] [-D [table]]\n" +
				"                  [-X<opt>]\n" +
				"or using long option equivalents --host --port --file --user --language\n" +
				"--dump --echo --database.\n" +
				"Arguments may be written directly after the option like -p50000.\n" +
				"\n" +
				"If no host and port are given, localhost and 50000 are assumed.  An .monetdb\n" +
				"file may exist in the user's home directory.  This file can contain\n" +
				"preferences to use each time JdbcClient is started.  Options given on the\n" +
				"command line override the preferences file.  The .monetdb file syntax is\n" +
				"<option>=<value> where option is one of the options host, port, file, mode\n" +
				"debug, or password.  Note that the last one is perilous and therefore not\n" +
				"available as command line option.\n" +
				"If no input file is given using the -f flag, an interactive session is\n" +
				"started on the terminal.\n" +
				"\n" +
				"OPTIONS\n" +
				copts.produceHelpMessage()
				);
			System.exit(0);
		} else if (copts.getOption("version").isPresent()) {
			// We cannot use the DatabaseMetaData here, because we
			// cannot get a Connection.  So instead, we just get the
			// values we want out of the Driver directly.
			System.out.println("JDBC Driver: v" + nl.cwi.monetdb.jdbc.MonetDriver.getDriverVersion());
			System.exit(0);
		}

		in = new BufferedReader(new InputStreamReader(System.in));
		out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)));

		// whether the semi-colon at the end of a String terminates the
		// query or not (default = yes => SQL)
		boolean scolonterm = true;
		boolean xmlMode = "xml".equals(copts.getOption("Xoutput").getArgument());

		// we need the password from the user, fetch it with a pseudo
		// password protector
		if (pass == null) {
			char[] tmp = System.console().readPassword("password: ");
			if (tmp == null) {
				System.err.println("Invalid password!");
				System.exit(1);
			}
			pass = String.valueOf(tmp);
		}

		user = copts.getOption("user").getArgument();

		// build the hostname
		String host = copts.getOption("host").getArgument();
		if (host.indexOf(":") == -1) {
			host = host + ":" + copts.getOption("port").getArgument();
		}

		// make sure the driver is loaded
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");

		// build the extra arguments of the JDBC connect string
		String attr = "?";
		CmdLineOpts.OptionContainer oc = copts.getOption("language");
		String lang = oc.getArgument();
		if (oc.isPresent())
			attr += "language=" + lang + "&";
		// set some behaviour based on the language XQuery
		if (lang.equals("xquery")) {
			scolonterm = false;	// no ; to end a statement
			if (!copts.getOption("Xoutput").isPresent())
				xmlMode = true; // the user will like xml results, most probably
		}
		oc = copts.getOption("Xdebug");
		if (oc.isPresent()) {
			attr += "debug=true&";
			if (oc.getArgumentCount() == 1)
				attr += "logfile=" + oc.getArgument() + "&";
		}
		oc = copts.getOption("Xhash");
		if (oc.isPresent())
			attr += "hash=" + oc.getArgument() + "&";

		// request a connection suitable for MonetDB from the driver
		// manager note that the database specifier is only used when
		// connecting to a proxy-like service, since MonetDB itself
		// can't access multiple databases.
		con = null;
		String database = copts.getOption("database").getArgument();
		try {
			con = DriverManager.getConnection(
					"jdbc:monetdb://" + host + "/" + database + attr,
					user,
					pass
			);
			SQLWarning warn = con.getWarnings();
			while (warn != null) {
				System.err.println("Connection warning: " + warn.getMessage());
				warn = warn.getNextWarning();
			}
			con.clearWarnings();
		} catch (SQLException e) {
			System.err.println("Database connect failed: " + e.getMessage());
			System.exit(1);
		}

		try {
			dbmd = con.getMetaData();
		} catch (SQLException e) {
			// we ignore this because it's probably because we don't use
			// SQL language
			dbmd = null;
		}
		stmt = con.createStatement();
		
		// see if we will have to perform a database dump (only in SQL mode)
		if ("sql".equals(lang) && copts.getOption("dump").isPresent()) {
			ResultSet tbl;

			// use the given file for writing
			oc = copts.getOption("file");
			if (oc.isPresent())
				out = new PrintWriter(new BufferedWriter(new FileWriter(oc.getArgument())));

			// we only want user tables and views to be dumped, unless a specific
			// table is requested
			String[] types = {"TABLE", "VIEW"};
			if (copts.getOption("dump").getArgumentCount() > 0)
				types = null;
			// request the tables available in the current schema in the database
			tbl = dbmd.getTables(null, con.getSchema(), null, types);

			List<Table> tables = new LinkedList<Table>();
			while (tbl.next()) {
				tables.add(new Table(
					tbl.getString("TABLE_SCHEM"),
					tbl.getString("TABLE_NAME"),
					tbl.getString("TABLE_TYPE")));
			}
			tbl.close();
			tbl = null;

			if (xmlMode) {
				exporter = new XMLExporter(out);
				exporter.setProperty(XMLExporter.TYPE_NIL, XMLExporter.VALUE_XSI);
			} else {
				exporter = new SQLExporter(out);
				// stick with SQL INSERT INTO commands for now
				// in the future we might do COPY INTO's here using VALUE_COPY
				exporter.setProperty(SQLExporter.TYPE_OUTPUT, SQLExporter.VALUE_INSERT);
			}
			exporter.useSchemas(true);

			// start SQL output
			if (!xmlMode)
				out.println("START TRANSACTION;\n");

			// dump specific table(s) or not?
			if (copts.getOption("dump").getArgumentCount() > 0) { // yes we do
				String[] dumpers = copts.getOption("dump").getArguments();
				for (int i = 0; i < tables.size(); i++) {
					Table ttmp = tables.get(i);
					for (int j = 0; j < dumpers.length; j++) {
						if (ttmp.getName().equalsIgnoreCase(dumpers[j].toString()) ||
							ttmp.getFqname().equalsIgnoreCase(dumpers[j].toString()))
						{
							// dump the table
							doDump(out, ttmp);
						}
					}
				}
			} else {
				/* this returns everything, so including SYSTEM TABLE
				 * constraints */
				tbl = dbmd.getImportedKeys(null, null, null);
				while (tbl.next()) {
					// find FK table object
					Table fk = Table.findTable(tbl.getString("FKTABLE_SCHEM") + "." + tbl.getString("FKTABLE_NAME"), tables);

					// find PK table object
					Table pk = Table.findTable(tbl.getString("PKTABLE_SCHEM") + "." + tbl.getString("PKTABLE_NAME"), tables);

					// this happens when a system table has referential
					// constraints
					if (fk == null || pk == null)
						continue;

					// add PK table dependancy to FK table
					fk.addDependancy(pk);
				}
				tbl.close();
				tbl = null;

				// search for cycles of type a -> (x ->)+ b probably not
				// the most optimal way, but it works by just scanning
				// every table for loops in a recursive manor
				for (Table t : tables) {
					Table.checkForLoop(t, new ArrayList<Table>());
				}

				// find the graph, at this point we know there are no
				// cycles, thus a solution exists
				for (int i = 0; i < tables.size(); i++) {
					List<Table> needs = tables.get(i).requires(tables.subList(0, i + 1));
					if (needs.size() > 0) {
						tables.removeAll(needs);
						tables.addAll(i, needs);

						// re-evaluate this position, for there is a new
						// table now
						i--;
					}
				}

				// we now have the right order to dump tables
				for (Table t : tables) {
					// dump the table
					doDump(out, t);
				}
			}

			if (!xmlMode)
				out.println("COMMIT;");
			out.flush();

			// free resources, close the statement
			stmt.close();
			// close the connection with the database
			con.close();
			// completed database dump
			System.exit(0);
		}

		if (xmlMode) {
			exporter = new XMLExporter(out);
			exporter.setProperty(XMLExporter.TYPE_NIL, XMLExporter.VALUE_XSI);
		} else {
			exporter = new SQLExporter(out);
			// we want nice table formatted output
			exporter.setProperty(SQLExporter.TYPE_OUTPUT, SQLExporter.VALUE_TABLE);
		}
		exporter.useSchemas(false);

		try {
			// use the given file for reading
			boolean hasFile = copts.getOption("file").isPresent();
			boolean doEcho = hasFile && copts.getOption("echo").isPresent();
			if (hasFile) {
				String tmp = copts.getOption("file").getArgument();
				int batchSize = 0;
				try {
					in = getReader(tmp);
				} catch (Exception e) {
					System.err.println("Error: " + e.getMessage());
					System.exit(1);
				}

				// check for batch mode
				oc = copts.getOption("Xbatching");
				if (oc.isPresent()) {
					if (oc.getArgumentCount() == 1) {
						// parse the number
						try {
							batchSize = Integer.parseInt(oc.getArgument());
						} catch (NumberFormatException ex) {
							// complain to the user
							throw new IllegalArgumentException("Illegal argument for Xbatching: " + oc.getArgument() + " is not a parseable number!");
						}
					}
					processBatch(batchSize);
				} else {
					processInteractive(true, doEcho, scolonterm, user);
				}
			} else {
				if (!copts.getOption("quiet").isPresent()) {
					// print welcome message
					out.println("Welcome to the MonetDB interactive JDBC terminal!");
					if (dbmd != null) {
						out.println("Database Server: " + dbmd.getDatabaseProductName() +
							" v" + dbmd.getDatabaseProductVersion());
						out.println("JDBC Driver: " + dbmd.getDriverName() +
							" v" + dbmd.getDriverVersion());
					}
					out.println("Current Schema: " + con.getSchema());
					out.println("Type \\q to quit, \\h for a list of available commands");
					out.flush();
				}
				processInteractive(false, doEcho, scolonterm, user);
			}

			// free resources, close the statement
			stmt.close();
			// close the connection with the database
			con.close();
			// close the file (if we used a file)
			in.close();
		} catch (Exception e) {
			System.err.println("A fatal exception occurred: " + e.toString());
			e.printStackTrace(System.err);
			// at least try to close the connection properly, since it will
			// close all statements associated with it
			try {
				con.close();
			} catch (SQLException ex) {
				// ok... nice try
			}
			System.exit(1);
		}
	}

	/**
	 * Tries to interpret the given String as URL or file.  Returns the
	 * assigned BufferedReader, or throws an Exception if the given
	 * string couldn't be identified as a valid URL or file.
	 *
	 * @param uri URL or filename as String
	 * @return a BufferedReader for the uri
	 * @throws Exception if uri cannot be identified as a valid URL or
	 *         file
	 */
	static BufferedReader getReader(String uri) throws Exception {
		BufferedReader ret = null;
		URL u = null;

		// Try and parse as URL first
		try {
			u = new URL(uri);
		} catch (MalformedURLException e) {
			// no URL, try as file
			try {
				ret = new BufferedReader(new FileReader(uri));
			} catch (FileNotFoundException fnfe) {
				// the message is descriptive enough, adds "(No such file
				// or directory)" itself.
				throw new Exception(fnfe.getMessage());
			}
		}

		if (ret == null) try {
			HttpURLConnection.setFollowRedirects(true);
			HttpURLConnection con =
				(HttpURLConnection)u.openConnection();
			con.setRequestMethod("GET");
			String ct = con.getContentType();
			if ("application/x-gzip".equals(ct)) {
				// open gzip stream
				ret = new BufferedReader(new InputStreamReader(
							new GZIPInputStream(con.getInputStream())));
			} else {
				// text/plain otherwise just attempt to read as is
				ret = new BufferedReader(new InputStreamReader(
							con.getInputStream()));
			}
		} catch (IOException e) {
			// failed to open the url
			throw new Exception("No such host/file: " + e.getMessage());
		} catch (Exception e) {
			// this is an exception that comes from deep ...
			throw new Exception("Invalid URL: " + e.getMessage());
		}

		return ret;
	}

	/**
	 * Starts an interactive processing loop, where output is adjusted to an
	 * user session.  This processing loop is not suitable for bulk processing
	 * as in executing the contents of a file, since processing on the given
	 * input is done after each row that has been entered.
	 *
	 * @param hasFile a boolean indicating whether a file is used as input
	 * @param doEcho a boolean indicating whether to echo the given input
	 * @param user a String representing the username of the current user
	 * @throws IOException if an IO exception occurs
	 * @throws SQLException if a database related error occurs
	 */
	public static void processInteractive(
		boolean hasFile,
		boolean doEcho,
		boolean scolonterm,
		String user
	)
		throws IOException, SQLException
	{
		// an SQL stack keeps track of ( " and '
		SQLStack stack = new SQLStack();
		// a query part is a line of an SQL query
		QueryPart qp = null;

		String query = "", curLine;
		boolean wasComplete = true, doProcess, lastac = false;
		if (!hasFile) {
			lastac = con.getAutoCommit();
			out.println("auto commit mode: " + (lastac ? "on" : "off"));
			out.print(getPrompt(stack, true));
			out.flush();
		}

		// the main (interactive) process loop
		for (long i = 1; true; i++) {
			// Manually read a line, because we want to detect an EOF
			// (ctrl-D).  Doing so allows to have a terminator for query
			// which is not based on a special character, as is the
			// problem for XQuery
			curLine = in.readLine();
			if (curLine == null) {
				out.println("");
				if (!query.isEmpty()) {
					try {
						executeQuery(query, stmt, out, !hasFile);
					} catch (SQLException e) {
						out.flush();
						do {
							if (hasFile) {
								System.err.println("Error on line " + i + ": [" + e.getSQLState() + "] " + e.getMessage());
							} else {
								System.err.println("Error [" + e.getSQLState() + "]: " + e.getMessage());
							}
							// print all error messages in the chain (if any)
						} while ((e = e.getNextException()) != null);
					}
					query = "";
					wasComplete = true;
					if (!hasFile) {
						boolean ac = con.getAutoCommit();
						if (ac != lastac) {
							out.println("auto commit mode: " + (ac ? "on" : "off"));
							lastac = ac;
						}
						out.print(getPrompt(stack, wasComplete));
					}
					out.flush();
					// try to read again
					continue;
				} else {
					// user did ctrl-D without something in the buffer,
					// so terminate
					break;
				}
			}

			if (doEcho) {
				out.println(curLine);
				out.flush();
			}
			qp = scanQuery(curLine, stack, scolonterm);
			if (!qp.isEmpty()) {
				String command = qp.getQuery();
				doProcess = true;
				if (wasComplete) {
					doProcess = false;
					// check for commands only when the previous row was
					// complete
					if (command.equals("\\q")) {
						// quit
						break;
					} else if (command.startsWith("\\h")) {
						out.println("Available commands:");
						out.println("\\q      quits this program");
						out.println("\\h      this help screen");
						if (dbmd != null)
							out.println("\\d      list available tables and views");
						out.println("\\d<obj> describes the given table or view");
						out.println("\\l<uri> executes the contents of the given file or URL");
						out.println("\\i<uri> batch executes the inserts from the given file or URL");
					} else if (dbmd != null && command.startsWith("\\d")) {
						try {
							String object = command.substring(2).trim().toLowerCase();
							if (scolonterm && object.endsWith(";"))
								object = object.substring(0, object.length() - 1);
							if (!object.isEmpty()) {
								String schema;
								int dot = object.indexOf(".");
								if (dot != -1) {
									// use provided schema
									schema = object.substring(0, dot);
									object = object.substring(dot + 1);
								} else {
									// use current schema
									schema = con.getSchema();
								}
								ResultSet tbl = dbmd.getTables(null, schema, null, null);

								// we have an object, see if we can find it
								boolean found = false;
								while (tbl.next()) {
									String tableName = tbl.getString("TABLE_NAME");
									String schemaName = tbl.getString("TABLE_SCHEM");
									if ((dot == -1 && tableName.equalsIgnoreCase(object)) ||
										(schemaName + "." + tableName).equalsIgnoreCase(object))
									{
										// we found it, describe it
										exporter.dumpSchema(dbmd,
												tbl.getString("TABLE_TYPE"),
												tbl.getString("TABLE_CAT"),
												schemaName,
												tableName);

										found = true;
										break;
									}
								}
								tbl.close();

								if (!found)
									System.err.println("Unknown table or view: " + object);
							} else {
								String current_schema = con.getSchema();
								ResultSet tbl = dbmd.getTables(null, current_schema, null, null);

								// give us a list of all non-system tables and views (including temp ones)
								while (tbl.next()) {
									String tableType = tbl.getString("TABLE_TYPE");
									if (tableType != null && !tableType.startsWith("SYSTEM "))
										out.println(tableType + "\t" +
											tbl.getString("TABLE_SCHEM") + "." +
											tbl.getString("TABLE_NAME"));
								}
								tbl.close();
							}
						} catch (SQLException e) {
							out.flush();
							do {
								System.err.println("Error [" + e.getSQLState() + "]: " + e.getMessage());
								// print all error messages in the chain (if any)
							} while ((e = e.getNextException()) != null);
						}
					} else if (command.startsWith("\\l") || command.startsWith("\\i")) {
						String object = command.substring(2).trim();
						if (scolonterm && object.endsWith(";"))
							object = object.substring(0, object.length() - 1);
						if (object.isEmpty()) {
							System.err.println("Usage: '" + command.substring(0, 2) + "<uri>' where <uri> is a file or URL");
						} else {
							// temporarily redirect input from in
							BufferedReader console = in;
							try {
								in = getReader(object);
								if (command.startsWith("\\l")) {
									processInteractive(true, doEcho, scolonterm, user);
								} else {
									processBatch(0);
								}
							} catch (Exception e) {
								out.flush();
								System.err.println("Error: " + e.getMessage());
							} finally {
								// put back in redirection
								in = console;
							}
						}
					} else {
						doProcess = true;
					}
				}

				if (doProcess) {
					query += command + (qp.hasOpenQuote() ? "\\n" : " ");
					if (qp.isComplete()) {
						// strip off trailing ';'
						query = query.substring(0, query.length() - 2);
						// execute query
						try {
							executeQuery(query, stmt, out, !hasFile);
						} catch (SQLException e) {
							out.flush();
							do {
								if (hasFile) {
									System.err.println("Error on line " + i + ": [" + e.getSQLState() + "] " + e.getMessage());
								} else {
									System.err.println("Error [" + e.getSQLState() + "]: " + e.getMessage());
								}
								// print all error messages in the chain (if any)
							} while ((e = e.getNextException()) != null);
						}
						query = "";
						wasComplete = true;
					} else {
						wasComplete = false;
					}
				}
			}

			if (!hasFile) {
				boolean ac = con.getAutoCommit();
				if (ac != lastac) {
					out.println("auto commit mode: " + (ac ? "on" : "off"));
					lastac = ac;
				}
				out.print(getPrompt(stack, wasComplete));
			}
			out.flush();
		}
	}

	/**
	 * Executes the given query and prints the result tabularised to the
	 * given PrintWriter stream.  The result of this method is the
	 * default output of a query: tabular data.
	 *
	 * @param query the query to execute
	 * @param stmt the Statement to execute the query on
	 * @param out the PrintWriter to write to
	 * @param showTiming flag to specify if timing information nees to be printed
	 * @throws SQLException if a database related error occurs
	 */
	private static void executeQuery(String query,
			Statement stmt,
			PrintWriter out,
			boolean showTiming)
		throws SQLException
	{
		// warnings generated during querying
		SQLWarning warn;
		long startTime = (showTiming ? System.currentTimeMillis() : 0);
		long finishTime = 0;

		// execute the query, let the driver decide what type it is
		int aff = -1;
		boolean	nextRslt = stmt.execute(query, Statement.RETURN_GENERATED_KEYS);
		if (!nextRslt)
			aff = stmt.getUpdateCount();
		do {
			if (nextRslt) {
				// we have a ResultSet, print it
				ResultSet rs = stmt.getResultSet();

				exporter.dumpResultSet(rs);
				if (showTiming) {
					finishTime = System.currentTimeMillis();
					out.println("Elapsed Time: " + (finishTime - startTime) + " ms");
					startTime = finishTime;
				}

				// if there were warnings for this result,
				// show them!
				warn = rs.getWarnings();
				if (warn != null) {
					// force stdout to be written so the
					// warning appears below it
					out.flush();
					do {
						System.err.println("ResultSet warning: " +
							warn.getMessage());
						warn = warn.getNextWarning();
					} while (warn != null);
					rs.clearWarnings();
				}
				rs.close();
			} else if (aff != -1) {
				String timingoutput = "";
				if (showTiming) {
					finishTime = System.currentTimeMillis();
					timingoutput = ". Elapsed Time: " + (finishTime - startTime) + " ms";
					startTime = finishTime;
				}

				if (aff == Statement.SUCCESS_NO_INFO) {
					out.println("Operation successful" + timingoutput);
				} else {
					// we have an update count
					// see if a key was generated
					ResultSet rs = stmt.getGeneratedKeys();
					boolean hasGeneratedKeyData = rs.next();
					out.println(aff + " affected row" + (aff != 1 ? "s" : "") +
						(hasGeneratedKeyData ? ", last generated key: " + rs.getString(1) : "") +
						timingoutput);
					rs.close();
				}
			}

			out.flush();
		} while ((nextRslt = stmt.getMoreResults()) ||
			 (aff = stmt.getUpdateCount()) != -1);

		// if there were warnings for this statement,
		// and/or connection show them!
		warn = stmt.getWarnings();
		while (warn != null) {
			System.err.println("Statement warning: " + warn.getMessage());
			warn = warn.getNextWarning();
		}
		stmt.clearWarnings();

		warn = con.getWarnings();
		while (warn != null) {
			System.err.println("Connection warning: " + warn.getMessage());
			warn = warn.getNextWarning();
		}
		con.clearWarnings();
	}

	/**
	 * Starts a processing loop optimized for processing (large) chunks of
	 * continuous data, such as input from a file.  Unlike in the interactive
	 * loop above, queries are sent only to the database if a certain batch
	 * amount is reached.  No client side query checks are made, but everything
	 * is sent to the server as-is.
	 *
	 * @param batchSize the number of items to store in the batch before
	 *                  sending them to the database for execution.
	 * @throws IOException if an IO exception occurs.
	 */
	public static void processBatch(int batchSize) throws IOException {
		StringBuilder query = new StringBuilder();
		String curLine;
		int i = 0;
		try {
			// the main loop
			for (i = 1; (curLine = in.readLine()) != null; i++) {
				query.append(curLine);
				if (curLine.endsWith(";")) {
					// lousy check for end of statement, but in batch mode it
					// is not very important to catch all end of statements...
					stmt.addBatch(query.toString());
					query.delete(0, query.length());
				} else {
					query.append('\n');
				}
				if (batchSize > 0 && i % batchSize == 0) {
					stmt.executeBatch();
					stmt.clearBatch();
				}
			}
			stmt.addBatch(query.toString());
			stmt.executeBatch();
			stmt.clearBatch();
		} catch (SQLException e) {
			do {
				System.err.println("Error at line " + i + ": [" + e.getSQLState() + "] " + e.getMessage());
				// print all error messages in the chain (if any)
			} while ((e = e.getNextException()) != null);
		}
	}

	/**
	 * Wrapper method that decides to dump SQL or XML.  In the latter case,
	 * this method does the XML data generation.
	 *
	 * @param out a Writer to write the data to
	 * @param table the table to dump
	 * @throws SQLException if a database related error occurs
	 */
	public static void doDump(PrintWriter out, Table table) throws SQLException {
		String tableType = table.getType();

		// dump CREATE definition of this table/view
		exporter.dumpSchema(dbmd, tableType, null, table.getSchem(), table.getName());
		out.println();

		// only dump data from real tables, not from views
		if (tableType.contains("TABLE")) {
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + table.getFqnameQ());
			exporter.dumpResultSet(rs);
			rs.close();
			out.println();
		}
	}

	/**
	 * Simple helper method that generates a prompt.
	 *
	 * @param stack the current SQLStack
	 * @param compl whether the statement is complete
	 * @return a prompt which consist of "sql" plus the top of the stack
	 */
	private static String getPrompt(SQLStack stack, boolean compl) {
		return (compl ? "sql" : "more") +
			(stack.empty() ? ">" : "" + stack.peek()) + " ";
	}

	/**
	 * Scans the given string and tries to discover if it is a complete query
	 * or that there needs something to be added.  If a string doesn't end with
	 * a ; it is considered not to be complete.  SQL string quotation using ' and
	 * SQL identifier quotation using " is taken into account when scanning a
	 * string this way.
	 * Additionally, this method removes comments from the SQL statements,
	 * identified by -- and removes white space where appropriate.
	 *
	 * @param query the query to parse
	 * @param stack query stack to work with
	 * @param scolonterm whether a ';' makes this query part complete
	 * @return a QueryPart object containing the results of this parse
	 */
	private static QueryPart scanQuery(
			String query,
			SQLStack stack,
			boolean scolonterm)
	{
		// examine string, char for char
		boolean wasInString = (stack.peek() == '\'');
		boolean wasInIdentifier = (stack.peek() == '"');
		boolean escaped = false;
		int len = query.length();
		for (int i = 0; i < len; i++) {
			switch(query.charAt(i)) {
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
					if (!escaped && stack.peek() != '"') {
						if (stack.peek() != '\'') {
							// although it makes no sense to escape a quote
							// outside a string, it is escaped, thus not meant
							// as quote for us, apparently
							stack.push('\'');
						} else {
							stack.pop();
						}
					}
					// reset escaped flag
					escaped = false;
				break;
				case '"':
					if (!escaped && stack.peek() != '\'') {
						if (stack.peek() != '"') {
							stack.push('"');
						} else {
							stack.pop();
						}
					}
					// reset escaped flag
					escaped = false;
				break;
				case '-':
					if (!escaped && stack.peek() != '\'' && stack.peek() != '"' && i + 1 < len && query.charAt(i + 1) == '-') {
						len = i;
					}
					escaped = false;
				break;
				case '(':
					if (!escaped && stack.peek() != '\'' && stack.peek() != '"') {
						stack.push('(');
					}
					escaped = false;
				break;
				case ')':
					if (!escaped && stack.peek() == '(') {
						stack.pop();
					}
					escaped = false;
				break;
			}
		}

		int start = 0;
		if (!wasInString && !wasInIdentifier && len > 0) {
			// trim spaces at the start of the string
			for (; start < len && Character.isWhitespace(query.charAt(start)); start++);
		}
		int stop = len - 1;
		if (stack.peek() !=  '\'' && !wasInIdentifier && stop > start) {
			// trim spaces at the end of the string
			for (; stop >= start && Character.isWhitespace(query.charAt(stop)); stop--);
		}
		stop++;

		if (start == stop) {
			// we have an empty string
			return new QueryPart(false, null, stack.peek() ==  '\'' || stack.peek() == '"');
		} else if (stack.peek() ==  '\'' || stack.peek() == '"') {
			// we have an open quote
			return new QueryPart(false, query.substring(start, stop), true);
		} else {
			// see if the string is complete
			if (scolonterm && query.charAt(stop - 1) == ';') {
				return new QueryPart(true, query.substring(start, stop), false);
			} else {
				return new QueryPart(false, query.substring(start, stop), false);
			}
		}
	}

	public static String dq(String in) {
		return "\"" + in.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"") + "\"";
	}
}

/**
 * A QueryPart is (a part of) a SQL query.  In the QueryPart object information
 * like the actual SQL query string, whether it has an open quote and the like
 * is stored.
 */
class QueryPart {
	private boolean complete;
	private String query;
	private boolean open;

	public QueryPart(boolean complete, String query, boolean open) {
		this.complete = complete;
		this.query = query;
		this.open = open;
	}

	public boolean isEmpty() {
		return query == null;
	}

	public boolean isComplete() {
		return complete;
	}

	public String getQuery() {
		return query;
	}

	public boolean hasOpenQuote() {
		return open;
	}
}

/**
 * An SQLStack is a simple stack that keeps track of open brackets and
 * (single and double) quotes in an SQL query.
 */
class SQLStack {
	StringBuilder stack = new StringBuilder();

	public char peek() {
		if (empty()) {
			return '\0';
		} else {
			return stack.charAt(stack.length() - 1);
		}
	}

	public char pop() {
		char tmp = peek();
		if (tmp != '\0') {
			stack.setLength(stack.length() - 1);
		}
		return tmp;
	}

	public char push(char item) {
		stack.append(item);
		return item;
	}

	public boolean empty() {
		return stack.length() == 0;
	}
}

/**
 * A Table represents an SQL table.  All data required to
 * generate a fully qualified name is stored, as well as dependency
 * data.
 */
class Table {
	final String schem;
	final String name;
	final String type;
	final String fqname;
	List<Table> needs = new ArrayList<Table>();

	Table(String schem, String name, String type) {
		this.schem = schem;
		this.name = name;
		this.type = type;
		this.fqname = schem + "." + name;
	}

	void addDependancy(Table dependsOn) throws Exception {
		if (this.fqname.equals(dependsOn.fqname))
			throw new Exception("Cyclic dependancy graphs are not supported (foreign key relation references self)");

		if (dependsOn.needs.contains(this))
			throw new Exception("Cyclic dependancy graphs are not supported (foreign key relation a->b and b->a)");

		if (!needs.contains(dependsOn))
			needs.add(dependsOn);
	}

	List<Table> requires(List<Table> existingTables) {
		if (existingTables == null || existingTables.isEmpty())
			return new ArrayList<Table>(needs);

		List<Table> req = new ArrayList<Table>();
		for (Table n : needs) {
			if (!existingTables.contains(n))
				req.add(n);
		}

		return req;
	}

	String getSchem() {
		return schem;
	}

	String getSchemQ() {
		return JdbcClient.dq(schem);
	}

	String getName() {
		return name;
	}

	String getNameQ() {
		return JdbcClient.dq(name);
	}

	String getType() {
		return type;
	}

	String getFqname() {
		return fqname;
	}

	String getFqnameQ() {
		return getSchemQ() + "." + getNameQ();
	}

	public String toString() {
		return fqname;
	}


	static Table findTable(String fqname, List<Table> list) {
		for (Table l : list) {
			if (l.fqname.equals(fqname))
				return l;
		}
		// not found
		return null;
	}

	static void checkForLoop(Table table, List<Table> parents) throws Exception {
		parents.add(table);
		for (int i = 0; i < table.needs.size(); i++) {
			Table child = table.needs.get(i);
			if (parents.contains(child))
				throw new Exception("Cyclic dependancy graphs are not supported (cycle detected for " + child.fqname + ")");
			checkForLoop(child, parents);
		}
	}
}
