import java.sql.*;
import java.io.*;
import java.util.Properties;

/**
 * This simple example somewhat implements an extended client program for
 * MonetDB. It's look and feel is very much like PostgreSQL's interactive
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
			} catch (IOException e) {
				// ok, then not
			}
		}

		// parse the arguments
		boolean hasFile = false, hasUser = false, hasHost = false,
				hasPort = false, hasDump = false, debug = false;
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
			} else if (!hasHost && args[i].equals("-D") && i + 1 < args.length) {
				dump = args[i + 1];
				i++;
				hasDump = true;
			} else if (!hasHost && args[i].startsWith("-D")) {
				dump = args[i].substring(2);
				if (host.equals("")) dump = null;
				hasDump = true;
			} else if (args[i].equals("--help")) {
				System.out.println("Usage java -jar MonetJDBC.jar [-h host[:port]] [-p port] [-f file] [-u user] [-d] [-D [table]]");
				System.out.println("where arguments may be written directly after the option like -hlocalhost.");
				System.out.println("If no host and port are given, localhost 45123 are assumed. The program will ask");
				System.out.println("for the username if not given but the -u flag specified. If no input file is given,");
				System.out.println("an interactive session is started on the terminal. The -d option creates a debug log.");
				System.out.println("The -D option can be used for dumping database tables. If no table is given all are assumed");
				System.exit(-1);
			} else {
				System.out.println("Ignoring unknown argument: " + args[i]);
			}
		}

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		// do we need to ask for the username?
		if (hasUser && user == null) {
			System.out.print("username: ");
			if ((user = in.readLine()) == null) {
				System.out.println("Invalid username!");
				System.exit(-1);
			}
		}

		// we need the password from the user
		if (pass == null) {
			PasswordField passfield = new PasswordField();
			try {
				pass = passfield.getPassword("password: ");
			} catch(IOException ioe) {
				System.out.println("Invalid password!");
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
		if (debug) nl.cwi.monetdb.jdbc.MonetConnection.setDebug(true);
		// request a connection suitable for Monet from the driver manager
		// note that the database specifier is currently not implemented, for
		// Monet itself can't access multiple databases.
		Connection con = DriverManager.getConnection("jdbc:monetdb://" + host + "/" + database, user, pass);
		DatabaseMetaData dbmd = con.getMetaData();
		Statement stmt = con.createStatement();

		if (hasDump) {
			System.out.println("START TRANSACTION;");
			if (dump != null) {
				describe(dbmd, stmt, dump, true);
			} else {
				// dump all
				String[] types = {"TABLE", "VIEW"};
				ResultSet tbl = dbmd.getTables(null, null, null, types);
				// dump all tables that are returned
				while (tbl.next()) {
					describe(dbmd, stmt, tbl.getString("TABLE_SCHEM") + "." + tbl.getString("TABLE_NAME"), true);
				}
			}
			System.out.println("COMMIT;");
			System.exit(0);
		}

		BufferedReader fr;
		if (hasFile) {
			// open the file
			fr = new BufferedReader(new FileReader(file));
		} else {
			// use stdin
			fr = in;
			System.out.println("Welcome to the MonetDB interactive JDBC terminal!");
			System.out.println("Database: " + dbmd.getDatabaseProductName() + " " +
				dbmd.getDatabaseProductVersion() + " (" + dbmd.getDatabaseMajorVersion() +
				"." + dbmd.getDatabaseMinorVersion() + ")");
			System.out.println("Driver: " + dbmd.getDriverName() + " " +
				dbmd.getDriverVersion() + " (" + dbmd.getDriverMajorVersion() +
				"." + dbmd.getDriverMinorVersion() + ")");
			System.out.println("Type \\q to quit, \\h for a list of available commands");
			System.out.println("auto commit mode: on");
		}

		SQLStack stack = new SQLStack();
		QueryPart qp;

		String query = "", curLine;
		boolean wasComplete = true, doProcess;
		if (!hasFile) System.out.print(getPrompt(user, stack));
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
						System.out.println("Available commands:");
						System.out.println("\\q      quits this program");
						System.out.println("\\h      this help screen");
						System.out.println("\\d      list available tables and views");
						System.out.println("\\d<obj> describes the given table or view");
					} else if (qp.getQuery().startsWith("\\d")) {
						String object = qp.getQuery().substring(2).trim().toLowerCase();
						if (object.endsWith(";")) object = object.substring(0, object.length() - 1);
						if (!object.equals("")) {
							describe(dbmd, stmt, object, false);
						} else {
							String[] types = {"TABLE", "VIEW"};
							ResultSet tbl = dbmd.getTables(null, null, null, types);
							// give us a list with tables
							while (tbl.next()) {
								System.out.println(tbl.getString("TABLE_TYPE") + "\t" +
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
							if (stmt.execute(query)) {
								// we have a ResultSet, print it
								ResultSet rs = stmt.getResultSet();
								ResultSetMetaData md = rs.getMetaData();
								int col = 1;
								System.out.print("+----------\n| ");
								for (; col < md.getColumnCount(); col++) {
									System.out.print(md.getColumnName(col) + "\t");
								}
								System.out.println(md.getColumnName(col));
								System.out.println("+----------");
								int count = 0;
								for (; rs.next(); count++) {
									col = 1;
									System.out.print("| ");
									for (; col < md.getColumnCount(); col++) {
										System.out.print(rs.getString(col) + "\t");
									}
									System.out.println(rs.getString(col));
								}
								System.out.println("+----------");
								System.out.println(count + " rows");
								rs.close();
							} else {
								// we have an update count
								System.out.println("affected rows\n-------------\n" + stmt.getUpdateCount());
							}
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
			if (!hasFile) System.out.print(getPrompt(user, stack));
		}

		// free resources, close the statement
		stmt.close();
		// close the connection with the database
		con.close();
		fr.close();
	}

	private static void describe(
		DatabaseMetaData dbmd,
		Statement stmt,
		String object,
		boolean dump
	) throws SQLException {
		ResultSet tbl = dbmd.getTables(null, null, null, null);
		// we have an object, see is we can find it
		boolean found = false;
		while (tbl.next()) {
			if (tbl.getString("TABLE_NAME").equalsIgnoreCase(object) ||
				(tbl.getString("TABLE_SCHEM") + "." + tbl.getString("TABLE_NAME")).equalsIgnoreCase(object)) {
				// we found it, describe it
				if (tbl.getString("TABLE_TYPE").equals("VIEW")) {
					System.out.println("CREATE VIEW " + tbl.getString("TABLE_NAME") + " AS " + tbl.getString("REMARKS").trim());
				} else {
					System.out.print(createTable(dbmd, tbl.getString("TABLE_CAT"),
						tbl.getString("TABLE_SCHEM"), tbl.getString("TABLE_NAME"),
						tbl.getString("TABLE_TYPE")));
					if (dump) dumpTable(System.out, stmt, tbl.getString("TABLE_SCHEM") + "." + tbl.getString("TABLE_NAME"));
				}
				found = true;
				break;
			}
		}
		if (!found) System.out.println("Unknown table or view: " + object);
	}

	private static String createTable(
		DatabaseMetaData dbmd,
		String cat,
		String schem,
		String table,
		String tableType
	) throws SQLException {
		StringBuffer ret = new StringBuffer();
		String comment = null;
		int i;
		ret.append("CREATE ").append(tableType).append(" ").append(schem);
		ret.append(".").append(table).append(" (\n");
		// put all columns with their type in place
		ResultSet cols = dbmd.getColumns(cat, schem, table, null);
		for (i = 0; cols.next(); i++) {
			int type = cols.getInt("DATA_TYPE");
			if (i > 0) ret.append(",\n");
			ret.append("\t").append(cols.getString("COLUMN_NAME"));
		 	ret.append("\t").append(cols.getString("TYPE_NAME"));
		 	if (type != Types.REAL &&
				type != Types.DOUBLE &&
				type != Types.FLOAT &&
				type != Types.TIMESTAMP &&
				type != Types.DATE &&
				type != Types.TIME)
			{
		 		ret.append("(").append(cols.getString("COLUMN_SIZE"));
		 		if (cols.getInt("DATA_TYPE") == Types.DECIMAL)
					ret.append(",").append(cols.getString("DECIMAL_DIGITS"));
				ret.append(")");
			}
			if (cols.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls)
				ret.append("\tNOT NULL");
		}
		cols.close();

		// the primary key constraint
		cols = dbmd.getPrimaryKeys(cat, schem, table);
		for (i = 0; cols.next(); i++) {
			if (i == 0) ret.append(",\n\n\tPRIMARY KEY (");
			if (i > 0) ret.append(", ");
			ret.append(cols.getString("COLUMN_NAME"));
		}
		if (i != 0) {
			ret.append(")");
			comment = cols.getString("PK_NAME");
		}
		cols.close();

		// unique constraints
		cols = dbmd.getIndexInfo(cat, schem, table, true, true);
		while (cols.next()) {
			ret.append(",");
			if (comment != null) ret.append(" -- ").append(comment);
			ret.append("\n");
			ret.append("\tUNIQUE (").append(cols.getString("COLUMN_NAME"));
			comment = cols.getString("INDEX_NAME");

			while (cols.next() && comment != null &&
				comment.equals(cols.getString("INDEX_NAME")))
			{
				ret.append(", ").append(cols.getString("COLUMN_NAME"));
			}
			// go back one
			cols.previous();

			ret.append(")");
		}
		cols.close();

		// foreign keys
		cols = dbmd.getImportedKeys(cat, schem, table);
		while (cols.next()) {
			ret.append(",");
			if (comment != null) ret.append(" -- ").append(comment);
			ret.append("\n");
			ret.append("\tFOREIGN KEY (");
			ret.append(cols.getString("FKCOLUMN_NAME")).append(") ");
			ret.append("REFERENCES ").append(cols.getString("PKTABLE_SCHEM"));
		 	ret.append(".").append(cols.getString("PKTABLE_NAME"));
			ret.append("(").append(cols.getString("PKCOLUMN_NAME"));
		 	ret.append(")");
			comment = cols.getString("FK_NAME");
		}
		cols.close();
		// if a comment needs to be added, do it now
		if (comment != null) ret.append(" -- ").append(comment);
		ret.append("\n");
		// end the create statement
		ret.append(");\n");;

		// create indexes
		cols = dbmd.getIndexInfo(cat, schem, table, false, true);
		comment = null;
		while (cols.next()) {
			if (!cols.getBoolean("NON_UNIQUE")) {
				// we already covered this one as UNIQUE
				continue;
			} else {
				ret.append(",\n");
				ret.append("CREATE INDEX ");
				ret.append(cols.getString("INDEX_NAME"));
				ret.append(" ON ").append(cols.getString("TABLE_NAME"));
				ret.append(" (").append(cols.getString("COLUMN_NAME"));
				comment = cols.getString("INDEX_NAME");

				while (cols.next() && comment != null &&
					comment.equals(cols.getString("INDEX_NAME")))
				{
					ret.append(", ").append(cols.getString("COLUMN_NAME"));
				}
				// go back one
				cols.previous();

				ret.append(");");
			}
		}
		cols.close();
		return(ret.toString());
	}

	private final static int AS_IS = 0;
	private final static int QUOTE = 1;
	private final static int TIMESTAMP = 2;
	private final static int TIME = 3;
	private final static int DATE = 4;

	public static void dumpTable(PrintStream out, Statement stmt, String table)
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
				case Types.DATE:
					types[i] = DATE;
				break;
				case Types.TIME:
					types[i] = TIME;
				break;
				case Types.TIMESTAMP:
					types[i] = TIMESTAMP;
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
					case TIMESTAMP:
						out.print("timestamp '");
						out.print(rs.getTimestamp(i));
						out.print("'");
					break;
					case TIME:
						out.print("time '");
						out.print(rs.getTime(i));
						out.print("'");
					break;
					case DATE:
						out.print("date '");
						out.print(rs.getDate(i));
						out.print("'");
					break;
				}
			}
			out.println(");");
		}
	}


	private static String getPrompt(String user, SQLStack stack) {
		return(user + "-" + (stack.empty() ? '>' : stack.peek()) + " ");
	}

	/**
	 * scans the given string and tries to discover if it is a complete query
	 * or that there needs something to be added. If a string doesn't end with
	 * a ; it is considered not to be complete. SQL string quotation using ' is
	 * taken into account when scanning a string this way.
	 * Additionally, this method removes comments from the SQL statements,
	 * identified by -- and removes white space where appropriate.
	 *
	 * @param query the query to parse
	 * @return a QueryPart object containing the results of this parse
	 */
	private static QueryPart scanQuery(String query, SQLStack stack) {
		// examine string, char for char
		boolean inString = (stack.peek() == '\''), escaped = false;
		boolean wasInString = inString;
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
					 * If all strings are wrapped between two quotes, a \" can
					 * never exist outside a string. Thus if we believe that we
					 * are not within a string, we can safely assume we're about
					 * to enter a string if we find a quote.
					 * If we are in a string we should stop being in a string if
					 * we find a quote which is not prefixed by a \, for that
					 * would be an escaped quote. However, a nasty situation can
					 * occur where the string is like "test \\" as obvious, a
					 * test for a \ in front of a " doesn't hold here for all
					 * cases. Because "test \\\"" can exist as well, we need to
					 * know if a quote is prefixed by an escaping slash or not.
					 */
					if (!inString) {
						inString = true;
						stack.push('\'');
					} else if (!escaped) {
						inString = false;
						if (stack.peek() == '\'') stack.pop();
					}

					// reset escaped flag
					escaped = false;
				break;
				case '-':
					if (!inString && i + 1 < len && query.charAt(i + 1) == '-') {
						len = i;
					}
					escaped = false;
				break;
				case '(':
					if (!inString) {
						stack.push('(');
					}
					escaped = false;
				break;
				case ')':
					if (!inString && stack.peek() == '(') {
						stack.pop();
					}
					escaped = false;
				break;
			}
		}

		int start = 0;
		if (!wasInString && len > 0) {
			// trim spaces at the start of the string
			for (; start < len && Character.isWhitespace(query.charAt(start)); start++);
		}
		int stop = len - 1;
		if (!inString && stop > start) {
			// trim spaces at the end of the string
			for (; stop >= start && Character.isWhitespace(query.charAt(stop)); stop--);
		}
		stop++;

		if (start == stop) {
			// we have an empty string
			return(new QueryPart(false, null, inString));
		} else if (inString) {
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
 * This class prompts the user for a password and attempts to mask input with ""
 */
class PasswordField {

  /**
   *@param prompt The prompt to display to the user.
   *@return The password as entered by the user.
   */
   String getPassword(String prompt) throws IOException {
      // password holder
      String password = "";
      MaskingThread maskingthread = new MaskingThread(prompt);
      Thread thread = new Thread(maskingthread);
      thread.start();
      // block until enter is pressed
      while (true) {
         char c = (char)System.in.read();
         // assume enter pressed, stop masking
         maskingthread.stopMasking();

         if (c == '\r') {
            c = (char)System.in.read();
            if (c == '\n') {
               break;
            } else {
               continue;
            }
         } else if (c == '\n') {
            break;
         } else {
            // store the password
            password += c;
         }
      }
      return(password);
   }
}

class MaskingThread extends Thread {
   private boolean stop = false;
   private int index;
   private String prompt;


  /**
   *@param prompt The prompt displayed to the user
   */
   public MaskingThread(String prompt) {
      this.prompt = prompt;
   }


  /**
   * Begin masking until asked to stop.
   */
   public void run() {
      while(!stop) {
         try {
            // attempt masking at this rate
            this.sleep(1);
         } catch (InterruptedException iex) {
            iex.printStackTrace();
         }
         if (!stop) {
            System.out.print("\r" + prompt + " \r" + prompt);
         }
         System.out.flush();
      }
   }


  /**
   * Instruct the thread to stop masking.
   */
   public void stopMasking() {
      this.stop = true;
   }
}
