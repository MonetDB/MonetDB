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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.jdbc;

import java.io.*;
import java.nio.*;
import java.net.*;

/**
 * A Socket for communicating with the MonetDB database in block mode.
 * <br /><br />
 * This MonetSocket performs basic operations like sending the server a
 * message and/or receiving a line from it.  A small interpretation of
 * all what is read is done, to supply some basic tools for the using
 * classes.<br />
 * For each read line, it is determined what type of line it is
 * according to the MonetDB MAPI protocol.i  This results in a line to
 * be PROMPT, HEADER, RESULT, ERROR or UNKNOWN.i  Use the getLineType()
 * function to retrieve the type of the last line read.
 * <br /><br />
 * For debugging purposes a socket level debugging is implemented where
 * each and every interaction to and from the MonetDB server is logged
 * to a file on disk.<br />
 * Incoming messages are prefixed by "RX" (received by the driver),
 * outgoing messages by "TX" (transmitted by the driver).  Special
 * decoded non-human readable messages are prefixed with "RD" and "TD"
 * instead.  Following this two char prefix, a timestamp follows as
 * the number of milliseconds since the UNIX epoch.  The rest of the
 * line is a String representation of the data sent or received.
 * <br /><br />
 * This implementation of MonetSocket uses block mode on the mapi
 * protocol.  It allows sending a multi line query as data is sent in
 * 'blocks' that are prefixed with a two byte integer indicating its
 * size.  The least significant bit of this integer represents the last
 * block in a sequence.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 2.10
 */
final class MonetSocketBlockMode {
	/** Stream from the Socket for reading */
	private InputStream fromMonetRaw;
	/** Stream from the Socket for writing */
	private OutputStream toMonetRaw;
	/** The TCP Socket to Mserver */
	private Socket con;

	/** Whether we are debugging or not */
	private boolean debug = false;
	/** The Writer for the debug log-file */
	private FileWriter log;

	/** The type of the last line read */
	private int lineType;

	/** "there is currently no line", or the the type is unknown is
	    represented by UNKNOWN */
	final static int UNKNOWN   = 0;
	/** a line starting with ! indicates ERROR */
	final static int ERROR     = 1;
	/** a line starting with % indicates HEADER */
	final static int HEADER    = 2;
	/** a line starting with [ indicates RESULT */
	final static int RESULT    = 3;
	/** a line which matches the pattern of prompt1 is a PROMPT1 */
	final static int PROMPT1   = 4;
	/** a line which matches the pattern of prompt2 is a PROMPT2 */
	final static int PROMPT2   = 5;
	/** a line starting with &amp; indicates the start of a header block */
	final static int SOHEADER  = 6;
	/** a line starting with ^ indicates REDIRECT */
	final static int REDIRECT  = 7;
	/** a line starting with # indicates INFO */
	final static int INFO      = 8;

	/** The blocksize (hardcoded in compliance with stream.mx) */
	final static int BLOCK     = 8 * 1024 - 2;

	/** A buffer which holds the blocks read */
	private StringBuffer readBuffer;
	/** The number of available bytes to read */
	private short readState = 0;

	/** A short in two bytes for holding the block size in bytes */
	private byte[] blklen = new byte[2];

	MonetSocketBlockMode(String host, int port)
		throws IOException
	{
		con = new Socket(host, port);
		// set nodelay, as it greatly speeds up small messages (like we
		// often do)
		con.setTcpNoDelay(true);

		// note: Always use buffered streams, as they perform better,
		// even though you know exactly which blocks you have to fetch
		// from the stream.  They are probably able to prefetch so the
		// IO is blocking while the program is still doing something
		// else.
		fromMonetRaw = new BufferedInputStream(con.getInputStream(), BLOCK + 2);
		toMonetRaw = new BufferedOutputStream(con.getOutputStream(), BLOCK + 2);

		readBuffer = new StringBuffer();
	}

	/**
	 * Enables logging to a file what is read and written from and to
	 * MonetDB.
	 *
	 * @param filename the name of the file to write to
	 * @throws IOException if the file could not be opened for writing
	 */
	public void debug(String filename) throws IOException {
		log = new FileWriter(filename);
		debug = true;
	}

	/**
	 * write puts the given string on the stream as UTF-8 data.  The
	 * stream will not be flushed after the write.  To flush the stream
	 * use flush(), or use writeLine().
	 *
	 * @param data the data to write to the stream
	 * @throws IOException if writing to the stream failed
	 * @see #flush()
	 * @see #writeLine(String data)
	 */
	public void write(String data) throws IOException {
		write(data.getBytes("UTF-8"));
	}

	/**
	 * Writes the given bytes to the stream
	 *
	 * @param data the bytes to be written
	 * @throws IOException if writing to the stream failed
	 */
	public void write(byte[] data) throws IOException {
		synchronized (toMonetRaw) {
			toMonetRaw.write(data);

			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			if (debug) logTx(new String(data, "UTF-8"));

			// reset the lineType variable, since we've sent data now and
			// the last line isn't valid anymore
			lineType = UNKNOWN;
		}
	}

	/**
	 * flushes the stream to monet, forcing all data in the buffer to be
	 * actually written to the stream.
	 *
	 * @throws IOException if writing to the stream failed
	 */
	public void flush() throws IOException {
		synchronized (toMonetRaw) {
			toMonetRaw.flush();

			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			if (debug) {
				log.flush();
			}
		}
	}

	/**
	 * writeLine puts the given string on the stream and flushes the
	 * stream afterwards so the data will actually be sent.  The given
	 * data String is wrapped within the query template.
	 *
	 * @param templ the query template to apply
	 * @param data the data to write to the stream
	 * @throws IOException if writing to the stream failed
	 */
	public void writeLine(String[] templ, String data) throws IOException {
		synchronized (toMonetRaw) {
			// In the same way as we read chunks from the socket, we
			// write chunks to the socket, so the server can start
			// processing while sending the rest of the input.
			data =
				(templ[0] != null ? templ[0] : "") +
				data +
				(templ[1] != null ? templ[1] : "");
			byte[] bytes = data.getBytes("UTF-8");
			int len = bytes.length;
			int todo = len;
			short blocksize;
			while (todo > 0) {
				if (todo <= BLOCK) {
					// always fits, because of BLOCK's size
					blocksize = (short)todo;
					// this is the last block, so encode least
					// significant bit in the first byte (little-endian)
					blklen[0] = (byte)(blocksize << 1 & 0xFF | 1);
					blklen[1] = (byte)(blocksize >> 7);
				} else {
					// always fits, because of BLOCK's size
					blocksize = (short)BLOCK;
					// another block will follow, encode least
					// significant bit in the first byte (little-endian)
					blklen[0] = (byte)(blocksize << 1 & 0xFF);
					blklen[1] = (byte)(blocksize >> 7);
				}

				toMonetRaw.write(blklen);

				// write the actual block
				toMonetRaw.write(bytes, len - todo, blocksize);
				
				if (debug) {
					if (todo <= BLOCK) {
						logTd("write final block: " + blocksize + " bytes");
					} else {
						logTd("write block: " + blocksize + " bytes");
					}
					logTx(new String(bytes, len - todo, blocksize, "UTF-8"));
				}

				todo -= blocksize;
			}

			// flush the stream
			flush();

			if (debug) log.flush();

			// reset the lineType variable, since we've sent data now and
			// the last line isn't valid anymore
			lineType = UNKNOWN;
		}
	}

	/**
	 * Reads up to count bytes from the stream, and returns them in a
	 * byte array
	 *
	 * @param data a byte array, which should be filled with data from
	 *             the stream
	 * @return the number of bytes actually read, never less than zero
	 * @throws IOException if some IO error occurs
	 */
	public int read(byte[] data) throws IOException {
		synchronized (fromMonetRaw) {
			// read the data
			int size = fromMonetRaw.read(data);
			if (size == -1) throw
				new IOException("End of stream reached");

			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			if (debug) logRx((new String(data, "UTF-8")).substring(0, size));

			return(size);
		}
	}

	private int readPos = 0;
	private boolean lastBlock = false;
	/**
	 * readLine reads one line terminated by a newline character and
	 * returns it without the newline character.  This operation can be
	 * blocking if there is no information available (yet).  If a block
	 * is marked as the last one, and the (left over) data does not end
	 * in a newline, it is returned as the last "line" before returning
	 * the prompt.
	 *
	 * @return a string representing the next line from the stream
	 * @throws IOException if reading from the stream fails
	 */
	public String readLine() throws IOException {
		synchronized (fromMonetRaw) {
			/*
			 * The blocked stream protocol consists of first a two byte
			 * integer indicating the length of the block, then the
			 * block, followed by another length + block.  The end of
			 * such sequence is put in the last bit of the length, and
			 * hence this length should be shifted to the right to
			 * obtain the real length value first.
			 * In this implementation we do not detect or use the user
			 * flush as it is not needed to detect for us since the
			 * higher level MAPI protocol defines a prompt which is used
			 * for synchronisation.  We simply fetch blocks here as soon
			 * as they are needed to process them line-based.
			 *
			 * The user-flush is a legacy thing now, and we simulate it
			 * to the levels above, by inserting it at the end of each
			 * 'lastBlock'.
			 */
			int nl;
			while ((nl = readBuffer.indexOf("\n", readPos)) == -1) {
				// not found, fetch us some more data
				// start reading a new block of data if appropriate
				if (readState == 0) {
					if (lastBlock) {
						if (readPos < readBuffer.length()) {
							// there is still some stuff, but not
							// terminated by a \n, send it to the user
							String line = readBuffer.substring(readPos);

							setLineType(readBuffer.charAt(readPos), line);

							// move the cursor position
							readPos = readBuffer.length();

							return(line);
						}

						lastBlock = false;
						// emit a fake prompt message
						if (debug) logRd("generating prompt");

						lineType = PROMPT1;

						return("");	// we omit putting the prompt in here
					}

					// read next two bytes (short)
					int size = fromMonetRaw.read(blklen);
					if (size == -1) throw
						new IOException("End of stream reached");
					if (size < 2) throw
						new AssertionError("Illegal start of block");

					// Get the int-value and store it in the readState.
					// We store having the last block or not, for later
					// to generate a prompt message.
					readState = (short)(
							(blklen[0] & 0xFF) >> 1 |
							(blklen[1] & 0xFF) << 7
						);
					lastBlock = (blklen[0] & 0x1) == 1;

					if (debug) {
						if (lastBlock) {
							logRd("final block: " + readState + " bytes");
						} else {
							logRd("new block: " + readState + " bytes");
						}
					}
				}
				// 'continue' fetching current block
				byte[] data = new byte[BLOCK < readState ? BLOCK : readState];
				int size = fromMonetRaw.read(data);
				if (size == -1) throw
					new IOException("End of stream reached");

				// update the state
				readState -= size;

				// clean up the buffer
				readBuffer.delete(0, readPos);
				readPos = 0;
				// append the stuff to the buffer; let String do the charset
				// conversion stuff
				readBuffer.append(new String(data, 0, size, "UTF-8"));

				if (debug) {
					logRd("read chunk: " + size +
						" bytes, left: " + readState + " bytes");
					logRx(new String(data, "UTF-8"));
				}
			}
			// fill line, excluding newline
			String line = readBuffer.substring(readPos, nl);

			setLineType(readBuffer.charAt(readPos), line);

			// move the cursor position
			readPos = nl + 1;

			return(line);
		}
	}

	/**
	 * Returns the type of the string given.  This method assumes a
	 * non-null string.
	 *
	 * @param first the first char from line
	 * @param line the string to examine
	 * @return the type of the given string
	 */
	private void setLineType(char first, String line) {
		lineType = UNKNOWN;
		switch (first) {
			case '!':
				lineType = ERROR;
			break;
			case '&':
				lineType = SOHEADER;
			break;
			case '%':
				lineType = HEADER;
			break;
			case '[':
				lineType = RESULT;
			break;
			case '^':
				lineType = REDIRECT;
			break;
			case '#':
				lineType = INFO;
			break;
			default:
				if (first == (char)1 && line.length() == 2) {
					if (line.charAt(1) == (char)1) {
						/* MAPI PROMPT1 */
						lineType = PROMPT1;
					} else if (line.charAt(1) == (char)2) {
						/* MAPI PROMPT2 (MORE) */
						lineType = PROMPT2;
					}
				}
			break;
		}
	}

	/**
	 * getLineType returns the type of the last line read.
	 *
	 * @return an integer representing the kind of line this is, one of the
	 *         following constants: UNKNOWN, HEADER, ERROR, PROMPT,
	 *         RESULT, REDIRECT, INFO
	 */
	public int getLineType() {
		return(lineType);
	}

	/**
	 * Reads up till the MonetDB prompt, indicating the server is ready
	 * for a new command.  All read data is discarded.  If the last line
	 * read by readLine() was a prompt, this method will immediately
	 * return.
	 * <br /><br />
	 * If there are errors present in the lines that are read, then they
	 * are put in one string and returned <b>after</b> the prompt has
	 * been found. If no errors are present, null will be returned.
	 *
	 * @return a string containing error messages, or null if there aren't any
	 * @throws IOException if an IO exception occurs while talking to the server
	 */
	final public synchronized String waitForPrompt() throws IOException {
		String ret = "", tmp;
		while (lineType != PROMPT1) {
			if ((tmp = readLine()) == null)
				throw new IOException("Connection to server lost!");
			if (lineType == ERROR) ret += "\n" + tmp.substring(1);
		}
		return(ret == "" ? null : ret.trim());
	}

	/**
	 * disconnect closes the streams and socket connected to the MonetDB server
	 * if possible. If an error occurs during disconnecting it is ignored.
	 */
	public synchronized void disconnect() {
		try {
			fromMonetRaw.close();
			toMonetRaw.close();
			con.close();
			if (debug) log.close();
		} catch (IOException e) {
			// ignore it
		}
	}

	/**
	 * Destructor called by garbage collector before destroying this
	 * object tries to disconnect the MonetDB connection if it has not
	 * been disconnected already.
	 */
	protected void finalize() throws Throwable {
		disconnect();
		super.finalize();
	}


	/**
	 * Writes a logline tagged with a timestamp using the given string.
	 * Used for debugging purposes only and represents a message that is
	 * connected to writing to the socket.  A logline might look like:
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
	 * Writes a logline tagged with a timestamp using the given string.
	 * Lines written using this log method are tagged as "added
	 * metadata" which is not strictly part of the data sent.
	 *
	 * @param message the message to log
	 * @throws IOException if an IO error occurs while writing to the logfile
	 */
	void logTd(String message) throws IOException {
		log.write("TD " + System.currentTimeMillis() +
			": " + message + "\n");
	}

	/**
	 * Writes a logline tagged with a timestamp using the given string,
	 * and flushes afterwards.  Used for debugging purposes only and
	 * represents a message that is connected to reading from the
	 * socket.  The log is flushed after writing the line.  A logline
	 * might look like:
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

	/**
	 * Writes a logline tagged with a timestamp using the given string,
	 * and flushes afterwards.  Lines written using this log method are
	 * tagged as "added metadata" which is not strictly part of the data
	 * received.
	 *
	 * @param message the message to log
	 * @throws IOException if an IO error occurs while writing to the logfile
	 */
	void logRd(String message) throws IOException {
		log.write("RD " + System.currentTimeMillis() +
			": " + message + "\n");
		log.flush();
	}
}
