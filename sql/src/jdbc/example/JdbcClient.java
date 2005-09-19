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
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 1.1
 */

public class JdbcClient {
	private static Connection con;
	private static Statement stmt;
	private static BufferedReader in;
	private static PrintWriter out;
	private static DatabaseMetaData dbmd;

	public final static void main(String[] args) throws Exception {
		// the arguments we handle
		Map arg = new HashMap();
		// arguments which need a value
		ArrayList value = new ArrayList();
		value.add("1");

		arg.put("h", value);
		arg.put("-host", value);
		value = (ArrayList)(value.clone());
		arg.put("p", value);
		arg.put("-port", value);
		value = (ArrayList)(value.clone());
		arg.put("f", value);
		arg.put("-file", value);
		value = (ArrayList)(value.clone());
		arg.put("u", value);
		arg.put("-user", value);
		value = (ArrayList)(value.clone());
		arg.put("Xmode", value);
		value = (ArrayList)(value.clone());
		arg.put("Xblksize", value);
		value = (ArrayList)(value.clone());
		arg.put("Xoutput", value);
		value = (ArrayList)(value.clone());
		arg.put("b", value);
		arg.put("-database", value);
		value = (ArrayList)(value.clone());
		arg.put("l", value);
		arg.put("-language", value);
		value = (ArrayList)(value.clone());
		arg.put("Xprepare", value);

		// arguments which can have zero to lots of arguments
		value = new ArrayList();
		value.add("0*");

		arg.put("d", value);
		arg.put("-dump", value);

		// arguments which can have zero or one argument(s)
		value = new ArrayList();
		value.add("01");

		value = (ArrayList)(value.clone());
		arg.put("Xdebug", value);
		value = (ArrayList)(value.clone());
		arg.put("Xbatching", value);

		// arguments which have no argument(s)
		value = new ArrayList();
		value.add("0");

		value = (ArrayList)(value.clone());
		arg.put("-help", value);

		value = (ArrayList)(value.clone());
		arg.put("e", value);
		arg.put("-echo", value);

		// default values, the username is prefixed with a space to identify
		// at a later stage if it has been set via the command line
		((ArrayList)(arg.get("u"))).add(" " + System.getProperty("user.name"));
		((ArrayList)(arg.get("h"))).add("localhost");
		((ArrayList)(arg.get("p"))).add("45123");
		((ArrayList)(arg.get("f"))).add(null);
		((ArrayList)(arg.get("Xmode"))).add(null);
		((ArrayList)(arg.get("Xblksize"))).add(null);
		((ArrayList)(arg.get("Xoutput"))).add(null);
		((ArrayList)(arg.get("d"))).add(null);
		((ArrayList)(arg.get("Xdebug"))).add(null);
		((ArrayList)(arg.get("Xbatching"))).add(null);
		((ArrayList)(arg.get("Xprepare"))).add("native");
		((ArrayList)(arg.get("-help"))).add(null);
		((ArrayList)(arg.get("e"))).add(null);
		((ArrayList)(arg.get("b"))).add("demo");
		((ArrayList)(arg.get("l"))).add("sql");

		// we cannot put password in the arg map, since it would be accessable
		// from the command line by then.
		String pass = null;

		// look for a file called .monetdb in the current dir or in the
		// user's homedir and read its preferences
		File pref = new File(".monetdb");
		if (!pref.exists()) pref = new File(System.getProperty("user.home"), ".monetdb");
		if (pref.exists()) {
			// the file is there, parse it and set the default settings
			Properties prop = new Properties();
			try {
				FileInputStream in = new FileInputStream(pref);
				prop.load(in);
				in.close();

				if (prop.containsKey("user"))
					((ArrayList)(arg.get("u"))).set(1, " " + prop.getProperty("user"));
				if (prop.containsKey("host"))
					((ArrayList)(arg.get("h"))).set(1, prop.getProperty("host"));
				if (prop.containsKey("port"))
					((ArrayList)(arg.get("p"))).set(1, prop.getProperty("port"));
				if (prop.containsKey("file"))
					((ArrayList)(arg.get("f"))).set(1, prop.getProperty("file"));
				if (prop.containsKey("mode"))
					((ArrayList)(arg.get("Xmode"))).set(1, prop.getProperty("mode"));
				if (prop.containsKey("debug"))
					((ArrayList)(arg.get("Xdebug"))).set(1, prop.getProperty("debug"));
				if (prop.containsKey("prepare"))
					((ArrayList)(arg.get("Xprepare"))).set(1, prop.getProperty("prepare"));
				if (prop.containsKey("database"))
					((ArrayList)(arg.get("b"))).set(1, prop.getProperty("database"));
				if (prop.containsKey("language"))
					((ArrayList)(arg.get("l"))).set(1, prop.getProperty("language"));

				pass = prop.getProperty("password", pass);
			} catch (IOException e) {
				// ok, forget it
			}
		}


		// parse and set the command line arguments
		value = null;
		int quant = -1;
		int qcount = 0;
		boolean moreData = false;
		for (int i = 0; i < args.length; i++) {
			if (value == null) {
				if (args[i].charAt(0) != '-') {
					System.err.println("Unexpected value: " + args[i]);
					System.exit(-1);
				}

				// see what kind of argument we have
				if (args[i].length() == 1)
					throw new Exception("Illegal argument, -");
				if (args[i].charAt(1) == '-') {
					// we have a long argument
					// since we don't accept inline values we can take
					// everything left in the string as argument
					value = (ArrayList)(arg.get(args[i].substring(1)));
					moreData = false;
				} else if (args[i].charAt(1) == 'X') {
					// extra argument, same as long argument
					value = (ArrayList)(arg.get(args[i].substring(1)));
					moreData = false;
				} else {
					// single char argument
					value = (ArrayList)(arg.get(new String("" + args[i].charAt(1))));
					moreData = args[i].length() > 2 ? true : false;
				}

				if (value != null) {
					String type = (String)(value.get(0));
					if (type == null)
						throw new AssertionError("Internal error, slap that programmer!");
					if (type.equals("1")) {
						if (moreData) {
							value.set(1, args[i].substring(2));
							value = null;
						} else {
							quant = 1;
						}
					} else if (type.equals("01")) {
						// store an object to indicate this argument was specified
						value.set(1, new Object());
						qcount = 1;
						quant = 2;
						if (moreData) {
							value.add(args[i].substring(2));
							value = null;
						}
					} else if (type.equals("0*")) {
						// store an object to indicate this argument was specified
						value.set(1, new Object());
						qcount = 1;
						quant = -1;
						if (moreData) {
							value.add(args[i].substring(2));
							qcount++;
						}
					} else if (type.equals("0")) {
						// no values allowed, put an object in place to indicate
						// this argument was specified
						value.set(1, new Object());
						value = null;
					}
				} else {
					System.err.println("Unknown argument: " + args[i]);
					System.exit(-1);
				}
			} else {
				// store the `value'
				if (qcount == 0) {
					value.set(1, args[i]);
				} else {
					value.add(args[i]);
				}
				if (++qcount == quant) {
					quant = 0;
					qcount = 0;
					value = null;
				}
			}
		}

		if (((ArrayList)(arg.get("-help"))).get(1) != null) {
			System.out.print(
"Usage java -jar MonetDB_JDBC.jar [-h host[:port]] [-p port] [-f file] [-u user]\n" +
"                                 [-l language] [-b [database]] [-d [table]]\n" +
"                                 [-e] [-X<opt>]\n" +
"or using long option equivalents --host --port --file --user --language\n" +
"--dump --echo --database.\n" +
"Arguments may be written directly after the option like -p45123.\n" +
"\n" +
"If no host and port are given, localhost and 45123 are assumed.  An .monetdb\n" +
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
"-h --host  The hostname of the host that runs the MonetDB database.  A port\n" +
"           number can be supplied by use of a colon, i.e. -h somehost:12345.\n" +
"-p --port  The port number to connect to.\n" +
"-f --file  A file name to use either for reading or writing.  The file will\n" +
"           be used for writing when dump mode is used (-d --dump).\n" +
"           In read mode, the file can also be an URL pointing to a plain\n" +
"           text file that is optionally gzip compressed.\n" +
"-u --user  The username to use when connecting to the database.\n" +
"-l --language  Use the given language, for example 'xquery'.\n" +
"-d --dump  Dumps the given table(s), or the complete database if none given.\n" +
"--help     This screen.\n" +
"-e --echo  Also outputs the contents of the input file, if any.\n" +
"-b --database  Try to connect to the given database (only makes sense\n" +
"           if connecting to a DatabasePool or equivalent process).\n" +
"\n" +
"EXTRA OPTIONS\n" +
"-Xdebug    Writes a transmission log to disk for debugging purposes.  If a\n" +
"           file name is given, it is used, otherwise a file called\n" +
"           monet<timestamp>.log is created.  A given file will never be\n" +
"           overwritten; instead a unique variation of the file is used.\n" +
"-Xmode     Specifies whether to use line or block mode when connecting.  Use\n" +
"           block or line to specify which mode to use.\n" +
"-Xblksize  Specifies the blocksize when using block mode, given in bytes.\n" +
"-Xoutput   The output mode when dumping.  Default is sql, xml may be used for\n" +
"           an experimental XML output.\n" +
"-Xbatching Indicates that a batch should be used instead of direct\n" +
"           communication with the server for each statement.  If a number is\n" +
"           given, it is used as batch size.  I.e. 8000 would execute the\n" +
"           contents on the batch after each 8000 read rows.  Batching can\n" +
"           greatly speedup the process of restoring a database dump.\n" +
/*
"-Xprepare  Specifies which PreparedStatement to use.  Valid arguments are:\n" +
"           'native' and 'java'.  The default behaviour if not specified or\n" +
"           with an unknown value is to request for a native PreparedStatement\n" +
"           at the MonetDB JDBC driver.\n" +
*/
""
);
			System.exit(0);
		}

		in = new BufferedReader(new InputStreamReader(System.in));
		out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)));

		// we need the password from the user, fetch it with a pseudo password
		// protector
		String user = ((ArrayList)(arg.get("u"))).get(1).toString();
		if (pass == null || user.charAt(0) != ' ') {
			try {
				char[] tmp = PasswordField.getPassword(System.in, "password: ");
				if (tmp != null) pass = String.valueOf(tmp);
			} catch(IOException ioe) {
				System.err.println("Invalid password!");
				System.exit(-1);
			}
			System.out.println("");
		}

		// remove the trailing space of the username, if any
		user = user.trim();

		// build the hostname
		String host = ((ArrayList)(arg.get("h"))).get(1).toString();
		if (host.indexOf(":") == -1) {
			host = host + ":" + ((ArrayList)(arg.get("p"))).get(1).toString();
		}

		// make sure the driver is loaded
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");

		// build the extra arguments of the JDBC connect string
		String attr = "?";
		String tmp = (String)(((ArrayList)(arg.get("Xmode"))).get(1));
		if ("line".equals(tmp)) attr += "blockmode=false&";
		tmp = (String)(((ArrayList)(arg.get("Xblksize"))).get(1));
		if (tmp != null) attr += "blockmode_blocksize=" + tmp + "&";
		String lang = (String)(((ArrayList)(arg.get("l"))).get(1));
		if (!"sql".equals(lang)) attr += "language=" + lang + "&";
		tmp = (String)(((ArrayList)(arg.get("Xprepare"))).get(1));
		attr += "native_prepared_statements=" + ("java".equals(tmp) ? "false" : "true") + "&";

		ArrayList ltmp = (ArrayList)(arg.get("Xdebug"));
		if (ltmp.get(1) != null) {
			attr += "debug=true&";
			if (ltmp.size() == 3) attr += "logfile=" + ltmp.get(2).toString() + "&";
		}


		// request a connection suitable for MonetDB from the driver manager
		// note that the database specifier is only used when connecting to
		// a proxy-like service, since MonetDB itself can't access multiple
		// databases.
		con = null;
		String database = (String)(((ArrayList)(arg.get("b"))).get(1));
		try {
			con = DriverManager.getConnection(
					"jdbc:monetdb://" + host + "/" + database + attr,
					user,
					pass
			);
			SQLWarning warn = con.getWarnings();
			while (warn != null) {
				System.err.println("Connection warning: " +
					warn.getMessage());
				warn = warn.getNextWarning();
			}
			con.clearWarnings();
		} catch (SQLException e) {
			System.err.println("Database connect failed: " + e.getMessage());
			System.exit(-1);
		}
		try {
			dbmd = con.getMetaData();
		} catch (SQLException e) {
			// we ignore this because it's probably because we don't use
			// SQL language
			dbmd = null;
		}
		stmt = con.createStatement();

		// see if we will have to perform a database dump (only in SQL
		// mode)
		ltmp = (ArrayList)(arg.get("d"));
		if ("sql".equals(lang) && ltmp.get(1) != null) {
			ResultSet tbl;

			// use the given file for writing
			tmp = (String)(((ArrayList)(arg.get("f"))).get(1));
			if (tmp != null) out = new PrintWriter(new BufferedWriter(new FileWriter(tmp)));

			// we only want tables and views to be dumped, unless a specific
			// table is requested
			String[] types = {"TABLE", "VIEW"};
			if (ltmp.size() > 2) types = null;
			// request the tables available in the database
			tbl = dbmd.getTables(null, null, null, types);

			LinkedList tables = new LinkedList();
			while(tbl.next()) {
				tables.add(new Table(
					tbl.getString("TABLE_CAT"),
					tbl.getString("TABLE_SCHEM"),
					tbl.getString("TABLE_NAME"),
					tbl.getString("TABLE_TYPE")));
			}

			// are we doing XML dumping?
			tmp = (String)(((ArrayList)(arg.get("Xoutput"))).get(1));
			boolean hasXMLDump = tmp != null && tmp.equals("xml");

			// start SQL output
			if (!hasXMLDump) out.println("START TRANSACTION;\n");

			// dump specific table(s) or not?
			if (ltmp.size() > 2) { // yes we do
				List dumpers = ltmp.subList(2, ltmp.size());
				for (int i = 0; i < tables.size(); i++) {
					Table ttmp = (Table)(tables.get(i));
					for (int j = 0; j < dumpers.size(); j++) {
						if (ttmp.getName().equalsIgnoreCase(dumpers.get(j).toString()) ||
							ttmp.getFqname().equalsIgnoreCase(dumpers.get(j).toString()))
						{
							// dump the table
							doDump(out, hasXMLDump, ttmp, dbmd, stmt);
						}
					}
				}
			} else {
				tbl = dbmd.getImportedKeys(null, null, null);
				while (tbl.next()) {
					// find FK table object
					Table fk = Table.findTable(tbl.getString("FKTABLE_SCHEM") + "." + tbl.getString("FKTABLE_NAME"), tables);

					// find PK table object
					Table pk = Table.findTable(tbl.getString("PKTABLE_SCHEM") + "." + tbl.getString("PKTABLE_NAME"), tables);

					// should not be possible to happen
					if (fk == null || pk == null)
						throw new AssertionError("Illegal table; table not found in list");

					// add PK table dependancy to FK table
					fk.addDependancy(pk);
				}

				// search for cycles of type a -> (x ->)+ b
				// probably not the most optimal way, but it works by just scanning
				// every table for loops in a recursive manor
				for (int i = 0; i < tables.size(); i++) {
					Table.checkForLoop((Table)(tables.get(i)), new ArrayList());
				}

				// find the graph
				// at this point we know there are no cycles, thus a solution exists
				for (int i = 0; i < tables.size(); i++) {
					List needs = ((Table)(tables.get(i))).requires(tables.subList(0, i + 1));
					if (needs.size() > 0) {
						tables.removeAll(needs);
						tables.addAll(i, needs);

						// re-evaluate this position, for there is a new table now
						i--;
					}
				}

				// we now have the right order to dump tables
				for (int i = 0; i < tables.size(); i++) {
					// dump the table
					Table t = (Table)(tables.get(i));
					doDump(out, hasXMLDump, t, dbmd, stmt);
				}
			}

			if (!hasXMLDump) out.println("COMMIT;");
			out.flush();

			con.close();
			System.exit(0);
		}


		try {
			// use the given file for reading
			tmp = (String)(((ArrayList)(arg.get("f"))).get(1));
			boolean hasFile = tmp != null;
			boolean doEcho = hasFile &&
				(((ArrayList)(arg.get("-echo"))).get(1) != null);
			if (hasFile) {
				int batchSize = 0;
				// figure out whether it is an URL
				try {
					URL u = new URL(tmp);
					HttpURLConnection.setFollowRedirects(true);
					HttpURLConnection con =
						(HttpURLConnection)u.openConnection();
					con.setRequestMethod("GET");
					String ct = con.getContentType();
					if ("application/x-gzip".equals(ct)) {
						// open gzip stream
						in = new BufferedReader(new InputStreamReader(
							new GZIPInputStream(con.getInputStream())));
					} else {
						// text/plain otherwise just attempt to read as is
						in = new BufferedReader(new InputStreamReader(
							con.getInputStream()));
					}
				} catch (MalformedURLException e) {
					// probably a real file, open it
					in = new BufferedReader(new FileReader(tmp));
				}

				// check for batch mode
				ltmp = (ArrayList)(arg.get("Xbatching"));
				if (ltmp.get(1) != null) {
					if (ltmp.size() == 3) {
						// parse the number
						try {
							batchSize = Integer.parseInt(ltmp.get(2).toString());
						} catch (NumberFormatException ex) {
							// complain to the user
							throw new IllegalArgumentException("Illegal argument for Xbatching: " + ltmp.get(2).toString() + " is not a parseable number!");
						}
					}
					processBatch(batchSize);
				} else {
					processInteractive(true, doEcho, user);
				}
			} else {
				// print welcome message
				out.println("Welcome to the MonetDB interactive JDBC terminal!");
				if (dbmd != null) {
					out.println("Database: " + dbmd.getDatabaseProductName() + " " +
						dbmd.getDatabaseProductVersion());
					out.println("Driver: " + dbmd.getDriverName() + " " +
						dbmd.getDriverVersion());
				}
				out.println("Type \\q to quit, \\h for a list of available commands");
				out.println("auto commit mode: on");

				processInteractive(false, doEcho, user);
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
			System.exit(-1);
		}
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
		String user
	)
		throws IOException, SQLException
	{
		// an SQL stack keeps track of ( " and '
		SQLStack stack = new SQLStack();
		// a query part is a line of an SQL query
		QueryPart qp = null;

		String query = "", curLine;
		boolean wasComplete = true, doProcess;
		if (!hasFile) {
			out.print(getPrompt(user, stack, true));
			out.flush();
		}

		// the main (interactive) process loop
		int i = 0;
		for (i = 1; (curLine = in.readLine()) != null; i++) {
			if (doEcho) {
				out.println(curLine);
				out.flush();
			}
			qp = scanQuery(curLine, stack);
			if (!qp.isEmpty()) {
				doProcess = true;
				if (wasComplete) {
					doProcess = false;
					// check for commands only when the previous row was complete
					if (qp.getQuery().equals("\\q")) {
						// quit
						break;
					} else if (qp.getQuery().startsWith("\\h")) {
						out.println("Available commands:");
						out.println("\\q      quits this program");
						out.println("\\h      this help screen");
						out.println("\\d      list available tables and views");
						out.println("\\d<obj> describes the given table or view");
					} else if (dbmd != null && qp.getQuery().startsWith("\\d")) {
						String object = qp.getQuery().substring(2).trim().toLowerCase();
						if (object.endsWith(";")) object = object.substring(0, object.length() - 1);
						if (!object.equals("")) {
							ResultSet tbl = dbmd.getTables(null, null, null, null);
							// we have an object, see is we can find it
							boolean found = false;
							while (tbl.next()) {
								if (tbl.getString("TABLE_NAME").equalsIgnoreCase(object) ||
									(tbl.getString("TABLE_SCHEM") + "." + tbl.getString("TABLE_NAME")).equalsIgnoreCase(object)) {
									// we found it, describe it
									createTable(
										out,
										dbmd,
										new Table(
											tbl.getString("TABLE_CAT"),
											tbl.getString("TABLE_SCHEM"),
											tbl.getString("TABLE_NAME"),
											tbl.getString("TABLE_TYPE")
										),
										true
									);
									
									found = true;
									break;
								}
							}
							if (!found) System.err.println("Unknown table or view: " + object);
							tbl.close();
						} else {
							String[] types = {"TABLE", "VIEW"};
							ResultSet tbl = dbmd.getTables(null, null, null, types);
							// give us a list with tables
							while (tbl.next()) {
								out.println(tbl.getString("TABLE_TYPE") + "\t" +
									tbl.getString("TABLE_SCHEM") + "." +
									tbl.getString("TABLE_NAME"));
							}
							tbl.close();
						}
					} else if (qp.getQuery().toLowerCase().startsWith("start transaction") ||
						qp.getQuery().toLowerCase().startsWith("begin transaction")) {
						try {
							// disable JDBC auto commit
							con.setAutoCommit(false);
							if (!hasFile) System.out.println("auto commit mode: off");
						} catch (SQLException e) {
							System.err.println("Error: " + e.getMessage());
						}
					} else if (qp.getQuery().toLowerCase().startsWith("commit")) {
						try {
							con.commit();
							// enable JDBC auto commit
							con.setAutoCommit(true);
							if (!hasFile) System.out.println("auto commit mode: on");
						} catch (SQLException e) {
							System.err.println("Error: " + e.getMessage());
						}
					} else if (qp.getQuery().toLowerCase().startsWith("rollback")) {
						try {
							con.rollback();
							// enable JDBC auto commit
							con.setAutoCommit(true);
						if (!hasFile) System.out.println("auto commit mode: on");
						} catch (SQLException e) {
							System.err.println("Error: " + e.getMessage());
						}
					} else {
						doProcess = true;
					}
				}

				if (doProcess) {
					query += qp.getQuery() + (qp.hasOpenQuote() ? "\\n" : " ");
					if (qp.isComplete()) {
						// strip off trailing ';'
						query = query.substring(0, query.length() - 2);
						// execute query
						try {
							executeQuery(query, stmt, out);
						} catch (SQLException e) {
							if (hasFile) {
								System.err.println("Error on line " + i + ": " + e.getMessage());
							} else {
								System.err.println("Error: " + e.getMessage());
							}
						}
						query = "";
					}
					wasComplete = qp.isComplete();
				}
			}
			if (!hasFile) out.print(getPrompt(user, stack, wasComplete));
			out.flush();
		}
		if (qp != null) {
			try {
				if (query != "") executeQuery(query, stmt, out);
			} catch (SQLException e) {
				if (hasFile) {
					System.err.println("Error on line " + i + ": " + e.getMessage());
				} else {
					System.err.println("Error: " + e.getMessage());
				}
			}
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
	 * @throws SQLException if a database related error occurs
	 */
	private static void executeQuery(String query,
			Statement stmt,
			PrintWriter out)
		throws SQLException
	{
		// warnings generated during querying
		SQLWarning warn;

		// execute the query, let the driver decide what type it is
		int aff = -1;
		boolean	nextRslt = stmt.execute(query);
		if (!nextRslt) aff = stmt.getUpdateCount();
		do {
			if (nextRslt) {
				// we have a ResultSet, print it
				ResultSet rs = stmt.getResultSet();
				ResultSetMetaData md = rs.getMetaData();
				// find the widths of the columns
				int[] width = new int[md.getColumnCount()];
				for (int j = 0; j < md.getColumnCount(); j++) {
					width[j] = Math.max((md.getColumnDisplaySize(j + 1) < 6 ? 6 : md.getColumnDisplaySize(j + 1)), md.getColumnLabel(j + 1).length());
				}

				out.print("+");
				for (int j = 0; j < width.length; j++)
					out.print("-" + repeat('-', width[j]) + "-+");
				out.println();

				out.print("|");
				for (int j = 0; j < width.length; j++) {
					out.print(" " + md.getColumnLabel(j + 1) + repeat(' ', width[j] - md.getColumnLabel(j + 1).length()) +  " |");
				}
				out.println();

				out.print("+");
				for (int j = 0; j < width.length; j++)
					out.print("=" + repeat('=', width[j]) + "=+");
				out.println();

				int count = 0;
				for (; rs.next(); count++) {
					out.print("|");
					for (int j = 0; j < width.length; j++) {
						Object rdata = rs.getObject(j + 1);
						String data;
						if (rdata == null) {
							data = "<NULL>";
						} else {
							data = rdata.toString();
						}
						if (md.isSigned(j + 1)) {
							// we have a numeric type here
							out.print(" " + repeat(' ', Math.max(width[j] - data.length(), 0)) + data +  " |");
						} else {
							// something else
							out.print(" " + data + repeat(' ', Math.max(width[j] - data.length(), 0)) +  " |");
						}
					}
					out.println();
				}

				out.print("+");
				for (int j = 0; j < width.length; j++)
					out.print("-" + repeat('-', width[j]) + "-+");
				out.println();

				out.println(count + " row" + (count != 1 ? "s" : ""));

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
				if (aff == Statement.SUCCESS_NO_INFO) {
					out.println("Operation successful");
				} else {
					// we have an update count
					out.println(aff + " affected row" + (aff != 1 ? "s" : ""));
				}
			}

			out.println();
			out.flush();
		} while ((nextRslt = stmt.getMoreResults()) ||
				 (aff = stmt.getUpdateCount()) != -1);

		// if there were warnings for this statement,
		// and/or connection show them!
		warn = stmt.getWarnings();
		while (warn != null) {
			System.err.println("Statement warning: " +
				warn.getMessage());
			warn = warn.getNextWarning();
		}
		stmt.clearWarnings();
		warn = con.getWarnings();
		while (warn != null) {
			System.err.println("Connection warning: " +
				warn.getMessage());
			warn = warn.getNextWarning();
		}
		con.clearWarnings();
	}

	/**
	 * Starts a processing loop optimized for processing (large) chunks of
	 * continous data, such as input from a file.  Unlike in the interactive
	 * loop above, queries are sent only to the database if a certain batch
	 * amount is reached.  No client side query checks are made, but everything
	 * is sent to the server as-is.
	 *
	 * @param batchSize the number of items to store in the batch before
	 *                  sending them to the database for execution.
	 * @throws IOException if an IO exception occurs.
	 */
	public static void processBatch(int batchSize) throws IOException {
		StringBuffer query = new StringBuffer();
		String curLine;
		int i = 0;
		try {
			// because this is an explicit batch from a file, we turn off
			// auto-commit
			con.setAutoCommit(false);

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

			// commit the transaction (if we came this far)
			con.commit();
		} catch (SQLException e) {
			System.err.println("Error at line " + i + ": " + e.getMessage());
		}
	}

	/**
	 * Simple helper function to repeat a given character a number of times.
	 *
	 * @param chr the character to repeat
	 * @param cnt the number of times to repeat chr
	 * @return a String holding cnt times chr
	 */
	private static String repeat(char chr, int cnt) {
		if (cnt < 0) return("");
		StringBuffer sb = new StringBuffer(cnt);
		for (int i = 0; i < cnt; i++) sb.append(chr);
		return(sb.toString());
	}

	/**
	 * A helper method to generate SQL CREATE code for a given table.
	 * This method performs all required lookups to find all relations and
	 * column information, as well as additional indices.
	 *
	 * @param out a Writer to write the output to
	 * @param dbmd a DatabaseMetaData object to query on
	 * @param table the table to describe in SQL CREATE format
	 * @throws SQLException if a database related error occurs
	 */
	private static void createTable(
		PrintWriter out,
		DatabaseMetaData dbmd,
		Table table,
		boolean fq
	) throws SQLException {
		// hande views directly
		if (table.getType().indexOf("VIEW") != -1) {
			String[] types = new String[1];
			types[0] = table.getType();
			ResultSet tbl = dbmd.getTables(table.getCat(), table.getSchem(), table.getName(), types);
			if (!tbl.next()) throw new SQLException("Whoops no data for " + table);

			// This will probably only work for MonetDB
			out.print("CREATE " + table.getType() + " ");
		 	out.print(fq ? table.getFqnameQ() : table.getNameQ());
			out.print(" AS ");
		 	out.println(tbl.getString("REMARKS").trim());
			return;
		}

		int i;
		String s;
		out.println("CREATE " + table.getType() + " " +
			(fq ? table.getFqnameQ() : table.getNameQ()) + " (");
		// put all columns with their type in place
		ResultSet cols = dbmd.getColumns(table.getCat(), table.getSchem(), table.getName(), null);
		ResultSetMetaData rsmd = cols.getMetaData();
		int colwidth = rsmd.getColumnDisplaySize(cols.findColumn("COLUMN_NAME"));
		int typewidth = rsmd.getColumnDisplaySize(cols.findColumn("TYPE_NAME"));
		for (i = 0; cols.next(); i++) {
			if (i > 0) out.println(",");
			// print column name
			s = dq(cols.getString("COLUMN_NAME"));
			out.print("\t" + s + repeat(' ', (colwidth - s.length()) + 3));

			s = cols.getString("TYPE_NAME");
			int type = cols.getInt("DATA_TYPE");
			int size = cols.getInt("COLUMN_SIZE");
			int digits = cols.getInt("DECIMAL_DIGITS");
			// small hack to get desired behaviour: set digits when we
			// have a time or timestamp with time zone and at the same
			// time masking the internal types
			if (s.equals("timetz")) {
				digits = 1;
				s = "time";
			} else if (s.equals("timestamptz")) {
				digits = 1;
				s = "timestamp";
			}
			// print column type
			out.print(s + repeat(' ', typewidth - s.length()));

			// do some type specifics 
		 	if (type == Types.FLOAT ||
				type == Types.VARCHAR ||
				type == Types.LONGVARCHAR ||
				type == Types.CHAR
			) {
				if (size <= 0) throw
					new SQLException("Illegal value for precision of type " + cols.getString("TYPE_NAME") + " (" + size + ")");
		 		out.print("(" + size + ")");
			} else if (type == Types.CLOB) {
				if (size > 0) out.print("(" + size + ")");
			} else if (type == Types.DECIMAL ||
				type == Types.NUMERIC
			) {
				if (digits < 0) throw
					new SQLException("Illegal value for scale of decimal type (" + digits + ")");
		 		out.print("(" + size + "," + digits + ")");
			} else if (type == Types.TIMESTAMP ||
				type == Types.TIME
			) {
				if (digits != 0) out.print(" WITH TIME ZONE");
			}
			if (cols.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls)
				out.print("\tNOT NULL");
			if ((s = cols.getString("COLUMN_DEF")) != null)
				out.print("\tDEFAULT '" + s.replaceAll("\\\\", "\\\\\\\\").replaceAll("\\\'", "\\\\\'") + "'");
		}
		cols.close();

		// the primary key constraint
		cols = dbmd.getPrimaryKeys(table.getCat(), table.getSchem(), table.getName());
		for (i = 0; cols.next(); i++) {
			if (i == 0) {
				// terminate the previous line
				out.println(",");
				out.print("\tCONSTRAINT " + dq(cols.getString("PK_NAME")) +
					" PRIMARY KEY (");
			}
			if (i > 0) out.print(", ");
			out.print(dq(cols.getString("COLUMN_NAME")));
		}
		if (i != 0) out.print(")");
		cols.close();

		// unique constraints
		cols = dbmd.getIndexInfo(table.getCat(), table.getSchem(), table.getName(), true, true);
		while (cols.next()) {
			String idxname = cols.getString("INDEX_NAME");
			out.println(",");
			out.print("\tCONSTRAINT " + dq(idxname) + " UNIQUE (" +
				dq(cols.getString("COLUMN_NAME")));

			boolean next;
			while ((next = cols.next()) && idxname != null &&
				idxname.equals(cols.getString("INDEX_NAME")))
			{
				out.print(", " + dq(cols.getString("COLUMN_NAME")));
			}
			// go back one, we've gone one too far
			if (next) cols.previous();

			out.print(")");
		}
		cols.close();

		// foreign keys
		cols = dbmd.getImportedKeys(table.getCat(), table.getSchem(), table.getName());
		while (cols.next()) {
			String fkname = cols.getString("FK_NAME");
			out.println(",");
			out.print("\tCONSTRAINT " + dq(fkname) + " FOREIGN KEY (");

			boolean next;
			Set fk = new LinkedHashSet();
			fk.add(cols.getString("FKCOLUMN_NAME").intern());
			Set pk = new LinkedHashSet();
			pk.add(cols.getString("PKCOLUMN_NAME").intern());

			while ((next = cols.next()) && fkname != null &&
				fkname.equals(cols.getString("FK_NAME")))
			{
				fk.add(cols.getString("FKCOLUMN_NAME").intern());
				pk.add(cols.getString("PKCOLUMN_NAME").intern());
			}
			// go back one
			if (next) cols.previous();

			Iterator it = fk.iterator();
			for (i = 0; it.hasNext(); i++) {
				if (i > 0) out.print(", ");
				out.print(dq((String)it.next()));
			}
			out.print(") ");

			out.print("REFERENCES " + dq(cols.getString("PKTABLE_SCHEM")) +
				"." + dq(cols.getString("PKTABLE_NAME")) + " (");
			it = pk.iterator();
			for (i = 0; it.hasNext(); i++) {
				if (i > 0) out.print(", ");
				out.print(dq((String)it.next()));
			}
		 	out.print(")");
		}
		cols.close();
		out.println();
		// end the create statement
		out.println(");");

		// create indexes
		cols = dbmd.getIndexInfo(table.getCat(), table.getSchem(), table.getName(), false, true);
		while (cols.next()) {
			if (!cols.getBoolean("NON_UNIQUE")) {
				// we already covered this one as UNIQUE
				continue;
			} else {
				String idxname = cols.getString("INDEX_NAME");
				out.print("CREATE INDEX " + dq(idxname) + " ON " +
					dq(cols.getString("TABLE_NAME")) + " (" +
					dq(cols.getString("COLUMN_NAME")));

				boolean next;
				while ((next = cols.next()) && idxname != null &&
					idxname.equals(cols.getString("INDEX_NAME")))
				{
					out.print(", " + dq(cols.getString("COLUMN_NAME")));
				}
				// go back one
				if (next) cols.previous();

				out.println(");");
			}
		}
		cols.close();
	}

	private final static int AS_IS = 0;
	private final static int QUOTE = 1;

	/**
	 * Helper method to dump the contents of a table in SQL INSERT format.
	 *
	 * @param out a Writer to write the data to
	 * @param stmt a Statement to perform the queries on
	 * @param table the (fully qualified) name of the table to dump
	 * @throws SQLException if a database related error occurs
	 */
	public static void dumpTable(
		PrintWriter out,
		Statement stmt,
		Table table
	) throws SQLException
	{
		// Simply select all from the given table
		ResultSet rs = stmt.executeQuery("SELECT * FROM " + table.getFqnameQ());
		ResultSetMetaData rsmd = rs.getMetaData();
		String statement = "INSERT INTO " + table.getNameQ() + " VALUES (";
		int cols = rsmd.getColumnCount();
		int[] types = new int[cols];
		for (int i = 0; i < cols; i++) {
			switch (rsmd.getColumnType(i + 1)) {
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
				case Types.DATE:
				case Types.TIME:
				case Types.TIMESTAMP:
					types[i] = QUOTE;
				break;
				case Types.NUMERIC:
				case Types.DECIMAL:
				case Types.BIT: // we don't use type BIT, it's here for completeness
				case Types.BOOLEAN:
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
				case Types.BIGINT:
				case Types.REAL:
				case Types.FLOAT:
				case Types.DOUBLE:
					types[i] = AS_IS;
				break;

				default:
					types[i] = AS_IS;
			}
		}

		while (rs.next()) {
			out.print(statement);
			for (int i = 1; i <= cols; i++) {
				if (i > 1) out.print(", ");
				if (rs.getString(i) == null) {
					out.print("NULL");
					continue;
				}
				switch (types[i - 1]) {
					case AS_IS:
						out.print(rs.getString(i));
					break;
					case QUOTE:
						out.print("'");
						out.print(rs.getString(i).replaceAll("\\\\", "\\\\\\\\").replaceAll("\\\'", "\\\\\'"));
						out.print("'");
					break;
				}
			}
			out.println(");");
		}
	}

	/**
	 * Wrapper method that decides to dump SQL or XML.  In the latter case,
	 * this method does the XML data generation.
	 *
	 * @param out a Writer to write the data to
	 * @param xml a boolean indicating whether to dump XML or not
	 * @param table the table to dump
	 * @param dbmd the DatabaseMetaData to use
	 * @param stmt the Statement to use
	 * @throws SQLException if a database related error occurs
	 */
	public static void doDump(
		PrintWriter out,
		boolean xml,
		Table table,
		DatabaseMetaData dbmd,
		Statement stmt)
	throws SQLException	{

		if (!xml) {
			changeSchema(table, stmt);
			createTable(
				out,
				dbmd,
				table,
				false
			);
			out.println();
			dumpTable(
				out,
				stmt,
				table
			);
			out.println();
		} else {
			// TODO: add xsd schema prior to dumping the data
			// TODO: wrap in schema tag
			if (table.getType().equals("VIEW")) {
				out.println("<!-- unable to represent VIEW " + table.getFqnameQ() + " -->");
			} else {
				ResultSet rs = stmt.executeQuery("SELECT * FROM " + table.getFqnameQ());
				ResultSetMetaData rsmd = rs.getMetaData();
				out.println("<" + table.getName() + ">");
				String data;
				while (rs.next()) {
					out.println("  <row>");
					for (int i = 1; i <= rsmd.getColumnCount(); i++) {
						data = rs.getString(i);
						// This is the "absent" method (of completely
						// hiding the tag if null.  TODO: add "nil"
						// method that writes <tag xsi:nil="true" />
						if (data != null) {
							out.print("    ");
							out.print("<" + rsmd.getColumnLabel(i));
							if (data.length() == 0) {
								out.println(" />");
							} else {
								out.print(">" + data.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;"));
								out.println("</" + rsmd.getColumnLabel(i) + ">");
							}
						}
					}
					out.println("  </row>");
				}
				out.println("</" + table.getName() + ">");
			}
		}
	}

	private static Stack lastSchema;
	private static void changeSchema(Table t, Statement stmt) {
		if (lastSchema == null) {
			lastSchema = new Stack();
			lastSchema.push(null);
		}

		if (!t.getSchem().equals(lastSchema.peek())) {
			if (!lastSchema.contains(t.getSchem())) {
				// create schema
				out.print("CREATE SCHEMA ");
				out.print(t.getSchemQ());
				out.println(";\n");
				lastSchema.push(t.getSchem());
			}
		
			out.print("SET SCHEMA ");
			out.print(t.getSchemQ());
			out.println(";\n");
		}
	}

	/**
	 * Simple helper method that generates a prompt.
	 *
	 * @param user the username
	 * @param stack the current SQLStack
	 * @param compl whether the statement is complete
	 * @return a prompt which consist of a username plus the top of the stack
	 */
	private static String getPrompt(String user, SQLStack stack, boolean compl) {
		return(user + (compl ? "-" : "=") +
			(stack.empty() ? ">" : "" + stack.peek()) + " ");
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
	 * @return a QueryPart object containing the results of this parse
	 */
	private static QueryPart scanQuery(String query, SQLStack stack) {
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
			return(new QueryPart(false, null, stack.peek() ==  '\'' || stack.peek() == '"'));
		} else if (stack.peek() ==  '\'' || stack.peek() == '"') {
			// we have an open quote
			return(new QueryPart(false, query.substring(start, stop), true));
		} else {
			// see if the string is complete
			if (query.charAt(stop - 1) == ';') {
				return(new QueryPart(true, query.substring(start, stop), false));
			} else {
				return(new QueryPart(false, query.substring(start, stop), false));
			}
		}
	}

	/**
	 * returns the given string between two double quotes for usage as
	 * exact column or table name in SQL queries.
	 *
	 * @param in the string to quote
	 * @return the quoted string
	 */
	public static String dq(String in) {
		return("\"" + in.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"") + "\"");
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
		return(query == null);
	}

	public boolean isComplete() {
		return(complete);
	}

	public String getQuery() {
		return(query);
	}

	public boolean hasOpenQuote() {
		return(open);
	}
}

/**
 * An SQLStack is a simple stack that keeps track of open brackets and
 * (single and double) quotes in an SQL query.
 */
class SQLStack {
	StringBuffer stack;

	public SQLStack() {
		stack = new StringBuffer();
	}

	public char peek() {
		if (empty()) {
			return('\0');
		} else {
			return(stack.charAt(stack.length() - 1));
		}
	}

	public char pop() {
		char tmp = peek();
		if (tmp != '\0') {
			stack.setLength(stack.length() - 1);
		}
		return(tmp);
	}

	public char push(char item) {
		stack.append(item);
		return(item);
	}

	public boolean empty() {
		return(stack.length() == 0);
	}
}

/**
 * This class prompts the user for a password and attempts to mask input with "*".
 */
class PasswordField {

	/**
	 *@param input stream to be used (e.g. System.in)
	 *@param prompt The prompt to display to the user.
	 *@return The password as entered by the user.
	 */
	public static final char[] getPassword(InputStream in, String prompt) throws IOException {
		MaskingThread maskingthread = new MaskingThread(prompt);
		Thread thread = new Thread(maskingthread);
		thread.start();

		char[] lineBuffer;
		char[] buf;
		int i;

		buf = lineBuffer = new char[128];

		int room = buf.length;
		int offset = 0;
		int c;

		boolean finished = false;
		while (!finished) {
			switch (c = in.read()) {
				case -1:
				case '\n':
					finished = true;
					continue;

				case '\r':
					int c2 = in.read();
					if ((c2 != '\n') && (c2 != -1)) {
						if (!(in instanceof PushbackInputStream)) {
							in = new PushbackInputStream(in);
						}
						((PushbackInputStream)in).unread(c2);
					} else {
						finished = true;
						continue;
					}

				default:
					if (--room < 0) {
						buf = new char[offset + 128];
						room = buf.length - offset - 1;
						System.arraycopy(lineBuffer, 0, buf, 0, offset);
						Arrays.fill(lineBuffer, ' ');
						lineBuffer = buf;
					}
					buf[offset++] = (char) c;
					break;
			}
		}
		maskingthread.stopMasking();
		if (offset == 0) {
			return null;
		}
		char[] ret = new char[offset];
		System.arraycopy(buf, 0, ret, 0, offset);
		Arrays.fill(buf, ' ');
		return ret;
	}
}

class MaskingThread extends Thread {
	private volatile boolean stop = false;

	/**
	 *@param prompt The prompt displayed to the user
	 */
	public MaskingThread(String prompt) {
		System.err.print(prompt);
	}


	/**
	 * Begin masking until asked to stop.
	 */
	public void run() {
		int priority = Thread.currentThread().getPriority();
		Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

		try {
			while(!stop) {
				System.err.print("\010 ");
				System.err.flush();
				try {
					// attempt masking at this rate
					Thread.sleep(1);
				} catch (InterruptedException iex) {
					Thread.currentThread().interrupt();
					return;
				}
			}
			System.out.print("\010" + "  \010\010");
		} finally {
			// restore the original priority
			Thread.currentThread().setPriority(priority);
		}
	}


	/**
	 * Instruct the thread to stop masking.
	 */
	public void stopMasking() {
		stop = true;
	}
}

/**
 * A Table represents an SQL table.  All data required to generate a fully
 * qualified name is stored, as well as dependency data.
 */
class Table {
	final String cat;
	final String schem;
	final String name;
	final String type;
	final String fqname;
	List needs;

	Table(String cat, String schem, String name, String type) {
		this.cat = cat;
		this.schem = schem;
		this.name = name;
		this.type = type;
		this.fqname = schem + "." + name;

		needs = new ArrayList();
	}

	void addDependancy(Table dependsOn) throws Exception {
		if (this.fqname.equals(dependsOn.fqname))
			throw new Exception("Cyclic dependancy graphs are not supported (foreign key relation references self)");

		if (dependsOn.needs.contains(this))
			throw new Exception("Cyclic dependancy graphs are not supported (foreign key relation a->b and b->a)");

		if (!needs.contains(dependsOn)) needs.add(dependsOn);
	}

	List requires(List existingTables) {
		if (existingTables == null || existingTables.size() == 0)
			return(new ArrayList(needs));

		List req = new ArrayList();
		for (int i = 0; i < needs.size(); i++) {
			if (!existingTables.contains(needs.get(i)))
				req.add(needs.get(i));
		}

		return(req);
	}

	String getCat() {
		return(cat);
	}

	String getSchem() {
		return(schem);
	}

	String getSchemQ() {
		return(JdbcClient.dq(schem));
	}

	String getName() {
		return(name);
	}

	String getNameQ() {
		return(JdbcClient.dq(name));
	}

	String getType() {
		return(type);
	}

	String getFqname() {
		return(fqname);
	}

	String getFqnameQ() {
		return(getSchemQ() + "." + getNameQ());
	}

	public String toString() {
		return(fqname);
	}


	static Table findTable(String fqname, List list) {
		for (int i = 0; i < list.size(); i++) {
			if (((Table)(list.get(i))).fqname.equals(fqname))
				return((Table)(list.get(i)));
		}
		// not found
		return(null);
	}

	static void checkForLoop(Table table, List parents) throws Exception {
		parents.add(table);
		for (int i = 0; i < table.needs.size(); i++) {
			Table child = (Table)(table.needs.get(i));
			if (parents.contains(child))
				throw new Exception("Cyclic dependancy graphs are not supported (cycle detected for " + child.fqname + ")");
			checkForLoop(child, parents);
		}
	}
}
