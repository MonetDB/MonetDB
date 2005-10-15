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

package nl.cwi.monetdb.jdbc;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * A Socket for communicating with the MonetDB database.
 * <br /><br />
 * This MonetSocket performs basic operations like sending the server a
 * message and/or receiving a line from it. A small interpretation of
 * all what is read is done, to supply some basic tools for the using
 * classes.<br /> For each read line, it is determined what type of line
 * it is according to the MonetDB MAPI protocol. This results in a line
 * to be PROMPT, HEADER, RESULT, ERROR or UNKNOWN. Use the getLineType()
 * function to retrieve the type of the last line read.
 * <br /><br />
 * For debugging purposes a socket level debugging is implemented where
 * each and every interaction to and from the MonetDB server is logged
 * to a file on disk.  Incoming messages are prefixed by "RX", outgoing
 * messages by "TX".
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 1.7
 */
class MonetSocket {
	/** Reader from the Socket */
	private BufferedReader fromMonet;
	/** Writer to the Socket */
	private BufferedWriter toMonet;
	/** The TCP Socket to Mserver */
	protected Socket con;

	/** Whether we are debugging or not */
	protected boolean debug = false;
	/** The Writer for the debug log-file */
	protected FileWriter log;

	/** The type of the last line read */
	protected int lineType;

	/** "there is currently no line", or the the type is unknown is
	    represented by UNKNOWN */
	final static int UNKNOWN = 0;
	/** a line starting with ! indicates ERROR */
	final static int ERROR = 1;
	/** a line starting with # indicates HEADER */
	final static int HEADER = 2;
	/** a line starting with [ indicates RESULT */
	final static int RESULT = 3;
	/** a line which matches the pattern of prompt1 is a PROMPT1 */
	final static int PROMPT1 = 4;
	/** a line which matches the pattern of prompt2 is a PROMPT2 */
	final static int PROMPT2 = 5;
	/** a line starting with #- indicates the start of a header block */
	final static int SOHEADER = 6;

	/** The maximum size of the chunks when we fetch data from the stream */
	final int readcapacity;
	/** The maximum size of the chunks when we push data to the stream */
	final int writecapacity;


	// MonetDB prompts
	/** MAPI PROMPT1 */
	final static String prompt1 = "" + (char)1 + (char)1;
	/** MAPI PROMPT2 */
	final static String prompt2 = "" + (char)1 + (char)2;
	/** MAPI start of header */
	final static String START_OF_HEADER = "#-";


	MonetSocket(String host, int port) throws IOException {
		con = new Socket(host, port);
		fromMonet = new BufferedReader(
			new InputStreamReader(con.getInputStream(), "UTF-8"));
		toMonet = new BufferedWriter(
			new OutputStreamWriter(con.getOutputStream(), "UTF-8"));
		
		// set the blocksize, use socket default
		readcapacity = con.getReceiveBufferSize();
		writecapacity = con.getSendBufferSize();
	}

	protected MonetSocket(Socket con) throws IOException {
		this.con = con;
		// set the blocksize, use socket default
		readcapacity = con.getReceiveBufferSize();
		writecapacity = con.getSendBufferSize();
	}

	/**
	 * enables logging to a file what is read and written from and to MonetDB
	 *
	 * @param filename the name of the file to write to
	 * @throws IOException if the file could not be opened for writing
	 */
	public void debug(String filename) throws IOException {
		log = new FileWriter(filename);
		debug = true;
	}

	/**
	 * write puts the given string on the stream as is, where is no newline
	 * appended, and the stream will not be flushed after the write.
	 * To flush the stream use flush(), or use writeln().
	 *
	 * @param data the data to write to the stream
	 * @throws IOException if writing to the stream failed
	 * @see #flush()
	 * @see #writeln(String data)
	 */
	public void write(String data) throws IOException {
		synchronized (toMonet) {
			toMonet.write(data);
			// reset the lineType variable, since we've sent data now and the last
			// line isn't valid anymore
			lineType = UNKNOWN;

			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			if (debug) logTx(data);
		}
	}

	/**
	 * flushes the stream to monet, forcing all data in the buffer to be
	 * actually written to the stream.
	 *
	 * @throws IOException if writing to the stream failed
	 */
	public void flush() throws IOException {
		synchronized (toMonet) {
			toMonet.flush();
			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			if (debug) log.flush();
		}
	}

	/**
	 * writeln puts the given string plus a new line character on the
	 * stream and flushes the stream afterwards so the data will
	 * actually be sent.  The given data String is wrapped within the
	 * query template.
	 *
	 * @param templ the query template to apply
	 * @param data the data to write to the stream
	 * @throws IOException if writing to the stream failed
	 */
	public void writeln(String[] templ, String data) throws IOException {
		if (templ[0] != null) write(templ[0]);
		write(data);
		if (templ[1] != null) write(templ[1]);
		write("\n");
		flush();
	}

	/**
	 * writeln puts a concatenation of the given strings plus a new line
	 * character on the stream and flushes the stream afterwards so the
	 * data will actually be sent.  The given data Strings are wrapped
	 * within and separated by the query template.
	 *
	 * @param templ the query template to apply
	 * @param data the data to write to the stream
	 * @throws IOException if writing to the stream failed
	 */
	public void writeln(String[] templ, List data) throws IOException {
		if (templ[0] != null) write(templ[0]);
		for (int i = 0; i < data.size(); i++) {
			write(data.get(i).toString());
			if (i < data.size() - 1 && templ[2] != null) write(templ[2]);
		}
		if (templ[1] != null) write(templ[1]);
		write("\n");
		flush();
	}

	/**
	 * readLine reads one line terminated by a newline character and returns
	 * it without the newline character. This operation can be blocking if there
	 * is no information available (yet)
	 *
	 * @return a string representing the next line from the stream
	 * @throws IOException if reading from the stream fails
	 */
	public String readLine() throws IOException {
		synchronized (fromMonet) {
			String line = fromMonet.readLine();

			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			if (debug) logRx(line);

			if (line != null) {
				lineType = getLineType(line);
			} else {
				throw new IOException("End of stream reached");
			}

			return(line);
		}
	}

	/**
	 * Returns the type of the string given.
	 * This method assumes a non-null string
	 *
	 * @param line the string to examine
	 * @return the type of the given string
	 */
	int getLineType(String line) {
		// return unknown if the line is empty, will force error on higher level
		if (line.length() == 0) return(UNKNOWN);

		switch (line.charAt(0)) {
			case '!': return(ERROR);
			case '#':
				if (START_OF_HEADER.equals(line)) {
					return(SOHEADER);
				} else {
					return(HEADER);
				}
			case '[': return(RESULT);
			default:
				if (prompt1.equals(line)) {
					return(PROMPT1);	// prompt1 found
				} else if (prompt2.equals(line)) {
					return(PROMPT2);	// prompt2 found
				} else {
					return(UNKNOWN);	// unknown :-(
				}
		}
	}

	/**
	 * getLineType returns the type of the last line read
	 *
	 * @return an integer representing the kind of line this is, one of the
	 *         following constants: UNKNOWN, HEADER, ERROR, PROMPT, RESULT
	 */
	public int getLineType() {
		return(lineType);
	}

	/**
	 * Reads up till the MonetDB prompt, indicating the server is ready for a
	 * new command. All read data is discarded.<br />
	 * If the last line read by readLine() was a prompt, this method will
	 * immediately return.
	 * <br /><br />
	 * If there are errors present in the lines that are read, then they are put
	 * in one string and returned <b>after</b> the prompt has been found. If no
	 * errors are present, null will be returned.
	 *
	 * @return a string containing error messages, or null if there aren't any
	 * @throws IOException if an IO exception occurs while talking to the server
	 */
	public synchronized String waitForPrompt() throws IOException {
		String ret = "", tmp;
		while (getLineType() != PROMPT1) {
			if ((tmp = readLine()) == null)
				throw new IOException("Connection to server lost!");
			if (getLineType() == ERROR) ret += "\n" + tmp.substring(1);
		}
		return(ret == "" ? null : ret.trim());
	}

	/**
	 * disconnect closes the streams and socket connected to the MonetDB server
	 * if possible. If an error occurs during disconnecting it is ignored.
	 */
	public synchronized void disconnect() {
		try {
			fromMonet.close();
			toMonet.close();
			con.close();
			if (debug) log.close();
		} catch (IOException e) {
			// ignore it
		}
	}

	/**
	 * Returns whether data can be read from the stream or not.
	 */
	public synchronized boolean hasData() {
		try {
			return(fromMonet.ready());
		} catch (IOException e) {
			return(false);
		}
	}

	/**
	 * destructor called by garbage collector before destroying this object
	 * tries to disconnect the MonetDB connection if it has not been disconnected
	 * already
	 */
	protected void finalize() throws Throwable {
		disconnect();
		super.finalize();
	}


	/**
	 * Writes a logline tagged with a timestamp using the given string.
	 * Used for debugging purposes only and represents a message that is
	 * connected to writing to the socket.
	 * A logline might look like:
	 * TX 152545124: Hello MonetDB!
	 *
	 * @param message the message to log
	 * @throws IOException if an IO error occurs while writing to the logfile
	 */
	void logTx(String message) throws IOException {
		log.write("TX " + System.currentTimeMillis() +
			": " + message + "\n");
	}

	/**
	 * Writes a logline tagged with a timestamp using the given string,
	 * and flushes afterwards.  Used for debugging purposes only and
	 * represents a message that is connected to reading from the socket.
	 * A logline might look like:
	 * RX 152545124: Hi JDBC!
	 *
	 * @param message the message to log
	 * @throws IOException if an IO error occurs while writing to the logfile
	 */
	void logRx(String message) throws IOException {
		log.write("RX " + System.currentTimeMillis() +
			": " + message + "\n");
		log.flush();
	}
}
