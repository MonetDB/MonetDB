/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.merovingian;

import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.cwi.monetdb.mcl.io.*;
import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * A Control class to perform operations on a remote merovingian
 * instance, using the TCP control protocol.
 * 
 * This class implements the protocol specific bits to perform all
 * possible actions against a merovingian server that has remote control
 * facilities enabled.
 * <br />
 * In the merovingian world, other merovingians in the vicinity are
 * known to each merovingian, allowing to perform cluster wide actions.
 * The implementation taken in this class is to require one known
 * merovingian to get insight in the entire network.  Note that
 * connecting to a merovingian requires a passphrase that is likely to
 * be different for each merovingian.
 *
 * @author Fabian Groffen
 * @version 1.0
 */
public class Control {
	/** The host to connect to */
	private final String host;
	/** The port to connect to */
	private final int port;
	/** The passphrase to use when connecting */
	private final String passphrase;
	/** The file we should write MapiSocket debuglog to */
	private String debug;


	/**
	 * Constructs a new Control object.
	 *
	 * @throws IllegalArgumentException if host, port or passphrase are
	 * null or &lt;= 0
	 */
	public Control(String host, int port, String passphrase)
		throws IllegalArgumentException
	{
		this.host = host;
		this.port = port;
		this.passphrase = passphrase;
	}

	/**
	 * Instructs to write a MCL protocol debug log to the given file.
	 * This affects any newly performed command, and can be changed
	 * inbetween commands.  Passing null to this method disables the
	 * debug log.
	 *
	 * @param filename the filename to write debug information to, or null
	 */
	public void setDebug(String filename) {
		this.debug = filename;
	}

	private String controlHash(String pass, String salt) {
		long ho;
		long h = 0;

		/* use a very simple hash function designed for a single int val
		 * (hash buckets), we can make this more interesting if necessary in
		 * the future.
		 * http://www.cs.hmc.edu/~geoff/classes/hmc.cs070.200101/homework10/hashfuncs.html */

		for (int i = 0; i < pass.length(); i++) {
			ho = h & 0xF8000000L;
			h <<= 5;
			h &= 0xFFFFFFFFL;
			h ^= ho >>> 27;
			h ^= (int)(pass.charAt(i));
		}

		for (int i = 0; i < salt.length(); i++) {
			ho = h & 0xF8000000L;
			h <<= 5;
			h &= 0xFFFFFFFFL;
			h ^= ho >> 27;
			h ^= (int)(salt.charAt(i));
		}

		return(Long.toString(h));
	}
	
	final static private String RESPONSE_OK = "OK";

	private List<String> sendCommand(
			String database, String command, boolean hasOutput)
		throws MerovingianException, IOException
	{
		BufferedMCLReader min;
		BufferedMCLWriter mout;
		MapiSocket ms = new MapiSocket();
		ms.setDatabase("merovingian");
		ms.setLanguage("control");
		if (debug != null)
			ms.debug(debug);
		try {
			ms.connect(host, port, "monetdb", passphrase);
			min = ms.getReader();
			mout = ms.getWriter();
		} catch (MCLParseException e) {
			throw new MerovingianException(e.getMessage());
		} catch (MCLException e) {
			throw new MerovingianException(e.getMessage());
		} catch (AssertionError e) { // mcl panics
			ms.close();
			
			// Try old protocol instead
			Socket s;
			PrintStream out;
			BufferedReader in;
			s = new Socket(host, port);
			out = new PrintStream(s.getOutputStream());
			in = new BufferedReader(
					new InputStreamReader(s.getInputStream()));
			try {
				/* login ritual, step 1: get challenge from server */
				String response = in.readLine();
				if (response == null)
					throw new MerovingianException("server closed the connection");

				if (!response.startsWith("merovingian:1:") &&
						!response.startsWith("merovingian:2:"))
					throw new MerovingianException("unsupported merovingian server");

				String[] tokens = response.split(":");
				if (tokens.length < 3)
					throw new MerovingianException("did not understand merovingian server");
				String version = tokens[1];
				String token = tokens[2];

				response = controlHash(passphrase, token);
				if (version.equals("1")) {
					out.print(response + "\n");
				} else if (version.equals("2")) {
					// we only support control mode for now
					out.print(response + ":control\n");
				}

				response = in.readLine();
				if (response == null) {
					throw new MerovingianException("server closed the connection");
				}

				if (!response.equals(RESPONSE_OK)) {
					throw new MerovingianException(response);
				}

				/* send command, form is simple: "<db> <cmd>\n" */
				out.print(database + " " + command + "\n");

				/* Response has the first line either "OK\n" or an error
				 * message.  In case of a command with output, the data will
				 * follow the first line */
				response = in.readLine();
				if (response == null) {
					throw new MerovingianException("server closed the connection");
				}
				if (!response.equals(RESPONSE_OK)) {
					throw new MerovingianException(response);
				}

				if (!hasOutput)
					return null;

				ArrayList<String> l = new ArrayList<String>();
				while ((response = in.readLine()) != null) {
					l.add(response);
				}
				return l;
			} finally {
				in.close();
				out.close();
				s.close();
			}
		}

		mout.writeLine(database + " " + command + "\n");
		ArrayList<String> l = new ArrayList<String>();
		String tmpLine = min.readLine();
		int linetype = min.getLineType();
		if (linetype == BufferedMCLReader.ERROR)
			throw new MerovingianException(tmpLine.substring(6));
		if (linetype != BufferedMCLReader.RESULT)
			throw new MerovingianException("unexpected line: " + tmpLine);
		if (!tmpLine.substring(1).equals(RESPONSE_OK))
			throw new MerovingianException(tmpLine.substring(1));
		tmpLine = min.readLine();
		linetype = min.getLineType();
		while (linetype != BufferedMCLReader.PROMPT) {
			if (linetype != BufferedMCLReader.RESULT)
				throw new MerovingianException("unexpected line: " +
						tmpLine);

			l.add(tmpLine.substring(1));

			tmpLine = min.readLine();
			linetype = min.getLineType();
		}

		ms.close();
		return l;
	}

	public void start(String database)
		throws MerovingianException, IOException
	{
		sendCommand(database, "start", false);
	}

	public void stop(String database)
		throws MerovingianException, IOException 
	{   
		sendCommand(database, "stop", false);
	}

	public void kill(String database)
		throws MerovingianException, IOException
	{
		sendCommand(database, "kill", false);
	}

	public void create(String database)
		throws MerovingianException, IOException
	{
		sendCommand(database, "create", false);
	}

	public void destroy(String database)
		throws MerovingianException, IOException
	{
		sendCommand(database, "destroy", false);
	}

	public void lock(String database)
		throws MerovingianException, IOException
	{
		sendCommand(database, "lock", false);
	}

	public void release(String database)
		throws MerovingianException, IOException
	{
		sendCommand(database, "release", false);
	}

	public void rename(String database, String newname)
		throws MerovingianException, IOException
	{
		if (newname == null)
			newname = ""; /* force error from merovingian */

		sendCommand(database, "name=" + newname, false);
	}

	/**
	 * Sets property for database to value.  If value is null, the
	 * property is unset, and its inherited value becomes active again.
	 *
	 * @param database the target database
	 * @param property the property to set value for
	 * @param value the value to set
	 * @throws MerovingianException if performing the command failed at
	 *         the merovingian side
	 * @throws IOException if connecting to or communicating with
	 *         merovingian failed
	 */
	public void setProperty(String database, String property, String value)
		throws MerovingianException, IOException
	{
		/* inherit: set to empty string */
		if (value == null)
			value = "";

		sendCommand(database, property + "=" + value, false);
	}

	public void inheritProperty(String database, String property)
		throws MerovingianException, IOException
	{
		setProperty(database, property, null);
	}

	public Properties getProperties(String database)
		throws MerovingianException, IOException
	{
		Properties ret = new Properties();
		List<String> response = sendCommand(database, "get", true);
		for (String responseLine : response) {
			if (responseLine.startsWith("#"))
				continue;
			int pos = responseLine.indexOf("=");
			if (pos > 0) {
				ret.setProperty(
						responseLine.substring(0, pos),
						responseLine.substring(pos + 1, responseLine.length()));
			}
		}
		return ret;
	}

	public Properties getDefaultProperties()
		throws MerovingianException, IOException
	{
		return(getProperties("#defaults"));
	}

	public SabaothDB getStatus(String database)
		throws MerovingianException, IOException
	{
		List<String> response = sendCommand(database, "status", true);
		if (response.isEmpty())
			throw new MerovingianException("communication error");
		return new SabaothDB(response.get(0));
	}
	
	/**
	 * Test whether a specific database exists. 
	 * 
	 * @param database
	 * @return true, iff database already exists.
	 * @throws MerovingianException
	 * @throws IOException
	 */
	public boolean exists(String database)
		throws MerovingianException, IOException
	{
		List<SabaothDB> all = getAllStatuses();
		for (SabaothDB db : all) {
			if (db.getName().equals(database)) {
				return true;
			}
		}
		return false;
	}

	public List<SabaothDB> getAllStatuses()
		throws MerovingianException, IOException
	{
		List<SabaothDB> l = new ArrayList<SabaothDB>();
		List<String> response = sendCommand("#all", "status", true);
		try {
			for (String responseLine : response) {
				l.add(new SabaothDB(responseLine));
			}
		} catch (IllegalArgumentException e) {
			throw new MerovingianException(e.getMessage());
		}
		return Collections.unmodifiableList(l);
	}

	public List<URI> getAllNeighbours()
		throws MerovingianException, IOException
	{
		List<URI> l = new ArrayList<URI>();
		List<String> response = sendCommand("anelosimus", "eximius", true);
		try {
			for (String responseLine : response) {
				// format is <db>\t<uri>
				String[] parts = responseLine.split("\t", 2);
				if (parts.length != 2)
					throw new MerovingianException("invalid entry: " +
							responseLine);
				if (parts[0].equals("*")) {
					l.add(new URI(parts[1]));
				} else {
					l.add(new URI(parts[1] + parts[0]));
				}
			}
		} catch (URISyntaxException e) {
			throw new MerovingianException(e.getMessage());
		}
		return Collections.unmodifiableList(l);
	}
}
