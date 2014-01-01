/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.client;

import nl.cwi.monetdb.util.*;
import nl.cwi.monetdb.merovingian.*;
import java.io.*;
import java.util.*;

/**
 * This program mimics the monetdb tool.  It is meant as demonstration
 * and test of the MeroControl library.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 1.0
 */

public class JMonetDB {
	private static PrintWriter out;

	public final static void main(String[] args) throws Exception {
		CmdLineOpts copts = new CmdLineOpts();

		// arguments which take exactly one argument
		copts.addOption("h", "host", CmdLineOpts.CAR_ONE, "localhost",
				"The hostname of the host that runs the MonetDB server.  " +
				"A port number can be supplied by use of a colon, i.e. " +
				"-h somehost:12345.");
		copts.addOption("p", "port", CmdLineOpts.CAR_ONE, "50000",
				"The port number to connect to.");
		copts.addOption("P", "passphrase", CmdLineOpts.CAR_ONE, null,
				"The passphrase to tell the MonetDB server");
		copts.addOption("c", "command", CmdLineOpts.CAR_ONE_MANY, null,
				"The command to execute on the MonetDB server");

		// arguments which have no argument(s)
		copts.addOption(null, "help", CmdLineOpts.CAR_ZERO, null,
				"This help screen.");

		// extended options
		copts.addOption(null, "Xhash", CmdLineOpts.CAR_ONE, null,
				"Use the given hash algorithm during challenge response.  " +
				"Supported algorithm names: SHA256, SHA1, MD5.");
		// arguments which can have zero or one argument(s)
		copts.addOption(null, "Xdebug", CmdLineOpts.CAR_ONE, null,
				"Writes a transmission log to disk for debugging purposes.  " +
				"A file name must be given.");

		try {
			copts.processArgs(args);
		} catch (OptionsException e) {
			System.err.println("Error: " + e.getMessage());
			System.exit(1);
		}

		if (copts.getOption("help").isPresent()) {
			System.out.print(
"Usage java -jar jmonetdb.jar\n" +
"                  -h host[:port] -p port -P passphrase [-X<opt>] -c cmd ...\n" +
"or using long option equivalents --host --port --passphrase.\n" +
"Arguments may be written directly after the option like -p50000.\n" +
"\n" +
"If no host and port are given, localhost and 50000 are assumed.\n" +
"\n" +
"OPTIONS\n" +
copts.produceHelpMessage()
);
			System.exit(0);
		}

		out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out)));

		String pass = copts.getOption("passphrase").getArgument();

		// we need the password from the user, fetch it with a pseudo
		// password protector
		if (pass == null) {
			char[] tmp = System.console().readPassword("passphrase: ");
			if (tmp == null) {
				System.err.println("Invalid passphrase!");
				System.exit(1);
			}
			pass = String.valueOf(tmp);
		}

		// build the hostname
		String host = copts.getOption("host").getArgument();
		String sport = copts.getOption("port").getArgument();
		int pos;
		if ((pos = host.indexOf(":")) != -1) {
			sport = host.substring(pos + 1);
			host = host.substring(0, pos);
		}
		int port = Integer.parseInt(sport);

		String hash = null;
		if (copts.getOption("Xhash").isPresent())
			hash = copts.getOption("Xhash").getArgument();

		if (!copts.getOption("command").isPresent()) {
			System.err.println("need a command to execute (-c)");
			System.exit(1);
		}

		Control ctl = null;
		try {
			ctl = new Control(host, port, pass);
		} catch (IllegalArgumentException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		// FIXME: Control needs to respect Xhash

		if (copts.getOption("Xdebug").isPresent()) {
			String fname = copts.getOption("Xdebug").getArgument();
			ctl.setDebug(fname);
		}

		String[] commands = copts.getOption("command").getArguments();
		if (commands[0].equals("status")) {
			List<SabaothDB> sdbs;
			if (commands.length == 1) {
				sdbs = ctl.getAllStatuses();
			} else {
				sdbs = new ArrayList<SabaothDB>();
				for (int i = 1; i < commands.length; i++)
					sdbs.add(ctl.getStatus(commands[i]));
			}
			Iterator<SabaothDB> it = sdbs.iterator();
			while (it.hasNext()) {
				SabaothDB sdb = it.next();
				System.out.println(sdb.getName() + " " + sdb.getURI());
			}
		}
	}
}
