import java.sql.*;
import java.io.*;
import java.util.*;

/**
 * This rather awkard implemented program acts like an extended client program
 * for MonetDB. It's look and feel is very much like PostgreSQL's interactive
 * terminal program.
 * Although it looks like this client is designed for MonetDB, it shows
 * the power of the JDBC interface since it built on top of JDBC only.
 * Every database which has a JDBC driver should work with this client.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */

public class JdbcClient {
	public static void main(String[] args) throws Exception {
		// variables that we use, and their defaults
		String file = null;
		String user = System.getProperty("user.name");
		String pass = null;
		String host = "localhost";
		String port = "45123";
		String database = "default";
		String dump = null;
		String blockmode = null;
		// we leave checking if this is a valid number to the driver

		// look for a file called .monetdb in the users homedir
		File pref = new File(System.getProperty("user.home"), ".monetdb");
		if (pref.exists()) {
			// the file is there, parse it and set the default settings
			Properties prop = new Properties();
			try {
				FileInputStream in = new FileInputStream(pref);
				prop.load(in);
				in.close();

				user = prop.getProperty("username", user);
				pass = prop.getProperty("password", pass);
				host = prop.getProperty("hostname", host);
				port = prop.getProperty("port", port);
				database = prop.getProperty("database", database);
				blockmode = prop.getProperty("blockmode", blockmode);

			} catch (IOException e) {
				// ok, then not
			}
		}

		// parse the arguments
		boolean hasFile = false, hasUser = false, hasHost = false,
				hasPort = false, hasDump = false, hasXMLDump = false,
				debug = false;
		for (int i = 0; i < args.length; i++) {
			if (!hasFile && args[i].equals("-f") && i + 1 < args.length) {
				file = args[i + 1];
				i++;
				hasFile = true;
			} else if (!hasFile && args[i].startsWith("-f")) {
				file = args[i].substring(2);
				if (file.equals("")) {
					System.out.println("-f needs a filename as argument");
					System.exit(-1);
				}
				hasFile = true;
			} else if (!hasUser && args[i].equals("-u") && i + 1 < args.length) {
				user = args[i + 1];
				i++;
				hasUser = true;
				pass = null;
			} else if (!hasUser && args[i].startsWith("-u")) {
				user = args[i].substring(2);
				if (user.equals("")) user = null;
				hasUser = true;
				pass = null;
			} else if (!hasHost && args[i].equals("-h") && i + 1 < args.length) {
				host = args[i + 1];
				i++;
				hasHost = true;
			} else if (!hasHost && args[i].startsWith("-h")) {
				host = args[i].substring(2);
				if (host.equals("")) {
					System.out.println("-h needs a hostname as argument");
					System.exit(-1);
				}
				hasHost = true;
			} else if (!hasPort && args[i].equals("-p") && i + 1 < args.length) {
				port = args[i + 1];
				i++;
				hasPort = true;
			} else if (!hasPort && args[i].startsWith("-p")) {
				port = args[i].substring(2);
				if (port.equals("")) {
					System.out.println("-p needs a port as argument");
					System.exit(-1);
				}
				hasPort = true;
			} else if (!debug && args[i].equals("-d")) {
				debug = true;
			} else if (!hasDump && args[i].equals("-D") && i + 1 < args.length) {
				dump = args[i + 1];
				i++;
				hasDump = true;
			} else if (!hasDump && args[i].startsWith("-D")) {
				dump = args[i].substring(2);
				if (dump.equals("")) dump = null;
				hasDump = true;
			} else if (!hasDump && args[i].equals("-X") && i + 1 < args.length) {
				dump = args[i + 1];
				i++;
				hasXMLDump = true;
				hasDump = true;
			} else if (!hasDump && args[i].startsWith("-X")) {
				dump = args[i].substring(2);
				if (dump.equals("")) dump = null;
				hasXMLDump = true;
				hasDump = true;
			} else if (blockmode == null && args[i].startsWith("-b")) {
				blockmode = "false";
			} else if (blockmode == null && args[i].startsWith("-B")) {
				blockmode = "true";
			} else if (args[i].equals("--help")) {
				System.out.println("Usage java -jar MonetJDBC.jar [-h host[:port]] [-p port] [-f file]");
				System.out.println("                              [-u user] [-d] [-D [table]] [-X [table]]");
				System.out.println("where arguments may be written directly after the option like -p45123");
				System.out.println("");
				System.out.println("If no host and port are given, localhost:45123 is assumed. The program");
				System.out.println("will ask for the username if not given or avaiable in .monetdb file in");
				System.out.println("the users home directory. The -u flag overrides the preferences file.");
		 		System.out.println("If no input file is given using the -f flag, an interactive session is");
				System.out.println("started on the terminal. The -d option creates a debug log.");
				System.out.println("");
				System.out.println("The -D and -X options can be used for dumping database tables. If no");
				System.out.println("table is given all tables are assumed. -D dumps using SQL format, -X");
				System.out.println("using an experimental XML format. If the -f flag is used when using");
				System.out.println("-D or -X the file is used for writing the output to.");
				System.exit(-1);
			} else {
				System.out.println("Ignoring unknown argument: " + args[i]);
			}
		}

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)));

		// do we need to ask for the username?
		if (hasUser && user == null) {
			System.err.print("username: ");
			if ((user = in.readLine()) == null) {
				System.err.println("Invalid username!");
				System.exit(-1);
			}
		}

		// we need the password from the user
		if (pass == null) {
			PasswordField passfield = new PasswordField();
			try {
				pass = String.valueOf(passfield.getPassword(System.in, "password: "));
			} catch(IOException ioe) {
				System.err.println("Invalid password!");
				System.exit(-1);
			}
			System.out.println("");
		}

		// fixate the hostname
		if (host.indexOf(":") == -1) {
			host = host + ":" + port;
		}

		// make sure the driver is loaded
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");

		String attr = "?";
		if (blockmode != null) attr += "blockmode=" + blockmode + "&";
		if (debug) attr += "debug=" + debug + "&";

		// request a connection suitable for Monet from the driver manager
		// note that the database specifier is currently not implemented, for
		// Monet itself can't access multiple databases.
		Connection con = null;
		try {
			con = DriverManager.getConnection("jdbc:monetdb://" + host + "/" + database + attr, user, pass);
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}
		DatabaseMetaData dbmd = con.getMetaData();
		Statement stmt = con.createStatement();

		if (hasDump) {
			ResultSet tbl;

			if (hasFile) out = new PrintWriter(new BufferedWriter(new FileWriter(file)));

			String[] types = {"TABLE", "VIEW"};
			if (dump != null) types = null;
			tbl = dbmd.getTables(null, null, null, types);

			LinkedList tables = new LinkedList();
			while(tbl.next()) {
				tables.add(new Table(
					tbl.getString("TABLE_CAT"),
					tbl.getString("TABLE_SCHEM"),
					tbl.getString("TABLE_NAME"),
					tbl.getString("TABLE_TYPE")));
			}

			if (!hasXMLDump) out.println("START TRANSACTION;");

			// dump a specific table or not?
			if (dump != null) { // yes we do
				for (int i = 0; i < tables.size(); i++) {
					Table tmp = (Table)(tables.get(i));
					if (tmp.getName().equalsIgnoreCase(dump) ||
						tmp.getFqname().equalsIgnoreCase(dump))
					{
						// dump the table
						doDump(out, hasXMLDump, tmp, dbmd, stmt);
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
						throw new Exception("Illegal table; table not found in list");

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
					doDump(out, hasXMLDump, (Table)(tables.get(i)), dbmd, stmt);
				}
			}

			if (!hasXMLDump) out.println("COMMIT;");
			out.flush();

			con.close();
			System.exit(0);
		}


		BufferedReader fr;
		if (hasFile) {
			// open the file
			fr = new BufferedReader(new FileReader(file));
		} else {
			// use stdin
			fr = in;

			out.println("Welcome to the MonetDB interactive JDBC terminal!");
			out.println("Database: " + dbmd.getDatabaseProductName() + " " +
				dbmd.getDatabaseProductVersion() + " (" + dbmd.getDatabaseMajorVersion() +
				"." + dbmd.getDatabaseMinorVersion() + ")");
			out.println("Driver: " + dbmd.getDriverName() + " " +
				dbmd.getDriverVersion() + " (" + dbmd.getDriverMajorVersion() +
				"." + dbmd.getDriverMinorVersion() + ")");
			out.println("Type \\q to quit, \\h for a list of available commands");
			out.println("auto commit mode: on");
		}

		SQLStack stack = new SQLStack();
		QueryPart qp;

		String query = "", curLine;
		boolean wasComplete = true, doProcess;
		if (!hasFile) {
			out.print(getPrompt(user, stack));
			out.flush();
		}
		for (int i = 1; (curLine = fr.readLine()) != null; i++) {
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
					} else if (qp.getQuery().startsWith("\\d")) {
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
									if (tbl.getString("TABLE_TYPE").equals("VIEW")) {
										out.println("CREATE VIEW " + tbl.getString("TABLE_NAME") + " AS " + tbl.getString("REMARKS").trim());
									} else {
										createTable(
											out,
											dbmd,
											new Table(
												tbl.getString("TABLE_CAT"),
												tbl.getString("TABLE_SCHEM"),
												tbl.getString("TABLE_NAME"),
												tbl.getString("TABLE_TYPE")
											)
										);
									}
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
						try {
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
										width[j] = Math.max(md.getColumnDisplaySize(j + 1), md.getColumnName(j + 1).length());
									}

									out.print("+");
									for (int j = 0; j < width.length; j++)
										out.print("-" + repeat('-', width[j]) + "-+");
									out.println();

									out.print("|");
									for (int j = 0; j < width.length; j++) {
										out.print(" " + md.getColumnName(j + 1) + repeat(' ', width[j] - md.getColumnName(j + 1).length()) +  " |");
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
											String data = rs.getString(j + 1);
											out.print(" " + data + repeat(' ', width[j] - data.length()) +  " |");
										}
										out.println();
									}

									out.print("+");
									for (int j = 0; j < width.length; j++)
										out.print("-" + repeat('-', width[j]) + "-+");
									out.println();

									out.println(count + " row" + (count != 1 ? "s" : ""));
									rs.close();
								} else if (aff >= 0) {
									// we have an update count
									out.println(aff + " affected row" + (aff != 1 ? "s" : ""));
								}

								out.println();
								out.flush();
							} while ((nextRslt = stmt.getMoreResults()) ||
									 (aff = stmt.getUpdateCount()) != -1);
						} catch (SQLException e) {
							System.err.println("Error on line " + i + ": " + e.getMessage());
							System.err.println("Executed query: " + query);
						} finally {
							query = "";
						}
					}
					wasComplete = qp.isComplete();
				}
			}
			if (!hasFile) out.print(getPrompt(user, stack));
			out.flush();
		}

		// free resources, close the statement
		stmt.close();
		// close the connection with the database
		con.close();
		fr.close();
	}

	private static String repeat(char chr, int cnt) {
		StringBuffer sb = new StringBuffer(cnt);
		for (int i = 0; i < cnt; i++) sb.append(chr);
		return(sb.toString());
	}

	private static void createTable(
		PrintWriter out,
		DatabaseMetaData dbmd,
		Table table
	) throws SQLException {
		if (table.getType().equals("VIEW")) {
			String[] types = new String[0];
			types[0] = table.getType();
			ResultSet tbl = dbmd.getTables(table.getCat(), table.getSchem(), table.getName(), types);
			if (!tbl.next()) throw new SQLException("Whoops no data for " + table);

			out.print("CREATE VIEW ");
		 	out.print(table.getFqnameQ());
			out.print(" AS ");
		 	out.print(tbl.getString("REMARKS").trim());
		}

		String comment = null;
		int i;
		out.print("CREATE "); out.print(table.getType()); out.print(" ");
		out.print(table.getFqnameQ()); out.println(" (");
		// put all columns with their type in place
		ResultSet cols = dbmd.getColumns(table.getCat(), table.getSchem(), table.getName(), null);
		for (i = 0; cols.next(); i++) {
			int type = cols.getInt("DATA_TYPE");
			if (i > 0) out.println(",");
			out.print("\t\""); out.print(cols.getString("COLUMN_NAME"));
		 	out.print("\"\t"); out.print(cols.getString("TYPE_NAME"));
			int size = cols.getInt("COLUMN_SIZE");
		 	if (size != 0 &&
				type != Types.REAL &&
				type != Types.DOUBLE &&
				type != Types.FLOAT &&
				type != Types.BOOLEAN &&
				type != Types.TIMESTAMP &&
				type != Types.DATE &&
				type != Types.TIME)
			{
		 		out.print("("); out.print(size);
		 		if (cols.getInt("DATA_TYPE") == Types.DECIMAL) {
					out.print(","); out.print(cols.getString("DECIMAL_DIGITS"));
				}
				out.print(")");
			}
			if (cols.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls)
				out.print("\tNOT NULL");
		}
		cols.close();

		// the primary key constraint
		cols = dbmd.getPrimaryKeys(table.getCat(), table.getSchem(), table.getName());
		for (i = 0; cols.next(); i++) {
			if (i == 0) {
				out.println(","); out.println();
				out.print("\tPRIMARY KEY (\"");
			}
			if (i > 0) out.print("\", \"");
			out.print(cols.getString("COLUMN_NAME"));
		}
		if (i != 0) {
			out.print("\")");
			comment = cols.getString("PK_NAME");
		}
		cols.close();

		// unique constraints
		cols = dbmd.getIndexInfo(table.getCat(), table.getSchem(), table.getName(), true, true);
		while (cols.next()) {
			out.print(",");
			if (comment != null) {
				out.print(" -- "); out.print(comment);
			}
			out.println();
			out.print("\tUNIQUE (\""); out.print(cols.getString("COLUMN_NAME"));
			comment = cols.getString("INDEX_NAME");

			boolean next;
			while ((next = cols.next()) && comment != null &&
				comment.equals(cols.getString("INDEX_NAME")))
			{
				out.print("\", \""); out.print(cols.getString("COLUMN_NAME"));
			}
			// go back one
			if (next) cols.previous();

			out.print("\")");
		}
		cols.close();

		// foreign keys
		cols = dbmd.getImportedKeys(table.getCat(), table.getSchem(), table.getName());
		while (cols.next()) {
			out.print(",");
			if (comment != null) {
				out.print(" -- "); out.print(comment);
			}
			out.println();
			out.print("\tFOREIGN KEY (\"");
			out.print(cols.getString("FKCOLUMN_NAME")); out.print("\") ");
			out.print("REFERENCES \""); out.print(cols.getString("PKTABLE_SCHEM"));
		 	out.print("\".\""); out.print(cols.getString("PKTABLE_NAME"));
			out.print("\" (\""); out.print(cols.getString("PKCOLUMN_NAME"));
		 	out.print("\")");
			comment = cols.getString("FK_NAME");
		}
		cols.close();
		// if a comment needs to be added, do it now
		if (comment != null) {
			out.print(" -- "); out.print(comment);
		}
		out.println();
		// end the create statement
		out.println(");");

		// create indexes
		cols = dbmd.getIndexInfo(table.getCat(), table.getSchem(), table.getName(), false, true);
		comment = null;
		while (cols.next()) {
			if (!cols.getBoolean("NON_UNIQUE")) {
				// we already covered this one as UNIQUE
				continue;
			} else {
				out.println(",");
				out.print("CREATE INDEX \"");
				out.print(cols.getString("INDEX_NAME"));
				out.print("\" ON \""); out.print(cols.getString("TABLE_NAME"));
				out.print("\" (\""); out.print(cols.getString("COLUMN_NAME"));
				comment = cols.getString("INDEX_NAME");

				boolean next;
				while ((next = cols.next()) && comment != null &&
					comment.equals(cols.getString("INDEX_NAME")))
				{
					out.print("\", \""); out.print(cols.getString("COLUMN_NAME"));
				}
				// go back one
				if (next) cols.previous();

				out.print("\");");
			}
		}
		cols.close();
	}

	private final static int AS_IS = 0;
	private final static int QUOTE = 1;

	public static void dumpTable(PrintWriter out, Statement stmt, String table)
		throws SQLException
	{
		ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);
		ResultSetMetaData rsmd = rs.getMetaData();
		String statement = "INSERT INTO " + table + " VALUES (";
		int cols = rsmd.getColumnCount();
		int[] types = new int[cols];
		for (int i = 0; i < cols; i++) {
			switch (rsmd.getColumnType(i + 1)) {
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
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

	public static void doDump(
		PrintWriter out,
		boolean xml,
		Table table,
		DatabaseMetaData dbmd,
		Statement stmt)
	throws SQLException	{

		if (!xml) {
			createTable(
				out,
				dbmd,
				table
			);
			dumpTable(
				out,
				stmt,
				table.getFqname()
			);
		} else {
			if (table.getType().equals("VIEW")) {
				out.println("<!-- unable to represent VIEW " + table + " -->");
			} else {
				ResultSet rs = stmt.executeQuery("SELECT * FROM " + table.getFqnameQ());
				ResultSetMetaData rsmd = rs.getMetaData();
				out.println("<table name=\"" + table.getFqname()+ "\">");
				String data;
				while (rs.next()) {
					out.println("  <row>");
					for (int i = 1; i <= rsmd.getColumnCount(); i++) {
						data = rs.getString(i);
						if (data != null) {
							out.print("    ");
							out.print("<" + rsmd.getColumnName(i));
							if (data.length() == 0) {
								out.println(" />");
							} else {
								out.print(">" + data.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;"));
								out.println("</" + rsmd.getColumnName(i) + ">");
							}
						}
					}
					out.println("  </row>");
				}
				out.println("</table>");
			}
		}
	}


	private static String getPrompt(String user, SQLStack stack) {
		return(user + "-" + (stack.empty() ? '>' : stack.peek()) + " ");
	}

	/**
	 * scans the given string and tries to discover if it is a complete query
	 * or that there needs something to be added. If a string doesn't end with
	 * a ; it is considered not to be complete. SQL string quotation using ' and
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
}

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
 * This class prompts the user for a password and attempts to mask input with "*"
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
					this.sleep(1);
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

		needs.add(dependsOn);
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

	String getName() {
		return(name);
	}

	String getType() {
		return(type);
	}

	String getFqname() {
		return(fqname);
	}

	String getFqnameQ() {
		return("\"" + schem + "\".\"" + name + "\"");
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
