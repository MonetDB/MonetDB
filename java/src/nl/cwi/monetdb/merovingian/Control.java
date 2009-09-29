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
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2009 MonetDB B.V.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.merovingian;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * A Control class to perform operations on a remote merovingian
 * instance, using the TCP control protocol.
 * <br /><br />
 * This class implements the protocol specific bits to perform all
 * possible actions against a merovingian server that has remote control
 * facilities enabled.
 * <br />
 * In the merovingian world, other merovingians in the vincinity are
 * known to each merovingian, allowing to perform cluster wide actions.
 * The implementation taken in this class is to require one known
 * merovingian to get insight in the entire network.  Note that
 * connecting to a merovingian requires a passphrase that is likely to
 * be different for each merovingian.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 1.0
 */
public class Control {
	/** The host to connect to */
	private String host = null;
	/** The port to connect to */
	private int port = -1;
	/** The passphrase to use when connecting */
	private String passphrase = null;


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

	private String controlHash(String pass, String salt) {
		long ho;
		long h = 0;

		/* use a very simple hash function designed for a single int val
		 * (hash buckets), we can make this more interesting if necessary in
		 * the future.
		 * http://www.cs.hmc.edu/~geoff/classes/hmc.cs070.200101/homework10/hashfuncs.html */

		for (int i = 0; i < pass.length(); i++) {
			ho = h & 0xf8000000;
			h <<= 5;
			h ^= ho >> 27;
			h ^= pass.codePointAt(i);
		}

		for (int i = 0; i < salt.length(); i++) {
			ho = h & 0xf8000000;
			h <<= 5;
			h ^= ho >> 27;
			h ^= salt.codePointAt(i);
		}

		return(Long.toString(h));
	}

	private String[] sendCommand(
			String database, String command, boolean hasOutput)
		throws MerovingianException, IOException
	{
		Socket s = new Socket(host, port);
		PrintStream out = new PrintStream(s.getOutputStream());
		BufferedReader in = new BufferedReader(
				new InputStreamReader(s.getInputStream()));
		String response;

		/* login ritual, step 1: get challenge from server */
		response = in.readLine();
		if (!response.startsWith("merovingian:1:"))
			throw new MerovingianException("unsupported merovingian server");
		String token = response.substring("merovingian:1:".length());

		response = controlHash(passphrase, token);
		out.println(response);

		response = in.readLine();
		if (!response.equals("OK")) {
			in.close();
			out.close();
			s.close();
			throw new MerovingianException(response);
		}

		/* send command, form is simple: "<db> <cmd>\n" */
		out.println(database + " " + command);

		/* Response has the first line either "OK\n" or an error
		 * message.  In case of a command with output, the data will
		 * follow the first line */
		response = in.readLine();
		if (!response.equals("OK")) {
			in.close();
			out.close();
			s.close();
			throw new MerovingianException(response);
		}

		String[] ret = null;
		if (hasOutput) {
			ArrayList l = new ArrayList();
			while ((response = in.readLine()) != null) {
				l.add(response);
			}
			ret = (String[])(l.toArray(new String[l.size()]));
		}

		return(ret);
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
		String[] response = sendCommand(database, "get", true);
		for (int i = 0; i < response.length; i++) {
			if (response[i].startsWith("#"))
				continue;
			int pos = response[i].indexOf("=");
			if (pos > 0) {
				ret.setProperty(
						response[i].substring(0, pos),
						response[i].substring(pos + 1, response[i].length()));
			}
		}
		return(ret);
	}

	public Properties getDefaultProperties()
		throws MerovingianException, IOException
	{
		return(getProperties("#defaults"));
	}

	public SabaothDB getStatus(String database)
		throws MerovingianException, IOException
	{
		String[] response = sendCommand(database, "status", true);
		return(new SabaothDB(response[0]));
	}

	public SabaothDB[] getAllStatuses()
		throws MerovingianException, IOException
	{
		ArrayList l = new ArrayList();
		String[] response = sendCommand("#all", "status", true);
		try {
			for (int i = 0; i < response.length; i++) {
				l.add(new SabaothDB(response[i]));
			}
		} catch (IllegalArgumentException e) {
			throw new MerovingianException(e.getMessage());
		}
		return((SabaothDB[])(l.toArray(new SabaothDB[l.size()])));
	}

	public URI[] getAllNeighbours()
		throws MerovingianException, IOException
	{
		ArrayList l = new ArrayList();
		String[] response = sendCommand("anelosimus", "eximius", true);
		try {
			for (int i = 0; i < response.length; i++) {
				if (response[i].endsWith("*"))
					response[i] = response[i].substring(0, response.length - 1);
				l.add(new URI(response[i]));
			}
		} catch (URISyntaxException e) {
			throw new MerovingianException(e.getMessage());
		}
		return((URI[])(l.toArray(new URI[l.size()])));
	}
}
