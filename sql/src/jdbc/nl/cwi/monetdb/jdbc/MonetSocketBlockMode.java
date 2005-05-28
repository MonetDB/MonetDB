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
import java.nio.*;
import java.net.*;

/**
 * A Socket for communicating with the MonetDB database in block mode.
 * <br /><br />
 * This MonetSocket performs basic operations like sending the server a message
 * and/or receiving a line from it. A small interpretation of all what is read
 * is done, to supply some basic tools for the using classes.<br />
 * For each read line, it is determined what type of line it is according to the
 * MonetDB MAPI protocol. This results in a line to be PROMPT, HEADER, RESULT,
 * ERROR or UNKNOWN. Use the getLineType() function to retrieve the type of the
 * last line read.
 * <br /><br />
 * For debugging purposes a socket level debugging is implemented where each and
 * every interaction to and from the MonetDB server is logged to a file on disk.
 * Incomming messages are prefixed by "&lt;&lt;", outgoing messages by
 * "&gt;&gt;".
 * <br /><br />
 * This implementation of MonetSocket uses block mode on the mapi protocol in
 * order to circumvent the drawbacks of line mode. It allows sending a multi
 * line query, and should be less intensive for the server.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 2.4
 */
class MonetSocketBlockMode extends MonetSocket {
	/** Stream from the Socket for reading */
	private InputStream fromMonetRaw;
	/** Stream from the Socket for writing */
	private OutputStream toMonetRaw;

	/** A buffer which holds the blocks read */
	private StringBuffer readBuffer;
	/** The number of available bytes to read */
	private int readState = 0;

	/** A ByteBuffer for performing a possible swab on an integer for reading */
	private ByteBuffer inputBuffer;
	/** A ByteBuffer for performing a possible swab on an integer for sending */
	private ByteBuffer outputBuffer;

	/** The maximum size of the chunks when we fetch data from the stream
	 *  (overridden!) */
	final int readcapacity;
	/** The maximum size of the chunks when we push data to the stream
	 *  (overridden!) */
	final int writecapacity;


	MonetSocketBlockMode(String host, int port, int blocksize) throws IOException {
		super(new Socket(host, port));

		fromMonetRaw = new BufferedInputStream(con.getInputStream());
		toMonetRaw = new BufferedOutputStream(con.getOutputStream());

		outputBuffer = ByteBuffer.allocateDirect(4);
		inputBuffer = ByteBuffer.allocateDirect(4);
		// leave the buffer byte-order as is, it can be modified later
		// by using setByteOrder()

		readBuffer = new StringBuffer();

		// set the blocksize, use socket default on 0 or negative
		// correct the value with the protocol overhead of 4 bytes
		if (blocksize <= 5) {
			readcapacity = con.getReceiveBufferSize() - 4;
			writecapacity = con.getSendBufferSize() - 4;
		} else {
			readcapacity = blocksize - 4;
			writecapacity = blocksize - 4;
		}
	}

	/**
	 * write puts the given string on the stream as is.
	 * The stream will not be flushed after the write.
	 * To flush the stream use flush(), or use writeln().
	 *
	 * @param data the data to write to the stream
	 * @throws IOException if writing to the stream failed
	 * @see #flush()
	 * @see #writeln(String data)
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

	private final static byte[] BLK_TERMINATOR =
		{(byte)0, (byte)0, (byte)0, (byte)0};
	private byte[] blklen = new byte[4];
	/**
	 * writeln puts the given string on the stream
	 * and flushes the stream afterwards so the data will actually be sent
	 *
	 * @param data the data to write to the stream
	 * @throws IOException if writing to the stream failed
	 */
	public void writeln(String data) throws IOException {
		synchronized (toMonetRaw) {
			// In the same way as we read chunks from the socket, we write
			// chunks to the socket, so the server can start processing while
			// sending the rest of the input.
			byte[] bytes = data.getBytes("UTF-8");
			int len = bytes.length;
			int todo = len;
			int blocksize;
			while (todo > 0) {
				// write the length of this block
				blocksize = todo < writecapacity ? todo : writecapacity;
				outputBuffer.rewind();
				outputBuffer.putInt(blocksize);

				outputBuffer.rewind();
				outputBuffer.get(blklen);

				toMonetRaw.write(blklen);

				// write the actual block
				toMonetRaw.write(bytes, len - todo, blocksize);
				
				if (debug) {
					logTx("write block: " + blocksize + " bytes");
					logTx(new String(bytes, len - todo, blocksize, "UTF-8"));
				}

				todo -= blocksize;
			}

			// the server wants an empty block as termination (flush)
			toMonetRaw.write(BLK_TERMINATOR);
			flush();

			if (debug) {
				logTx("zero block (flush)");
				log.flush();
			}

			// reset the lineType variable, since we've sent data now and
			// the last line isn't valid anymore
			lineType = UNKNOWN;
		}
	}

	/**
	 * Reads up to count bytes from the stream, and returns them in a byte array
	 *
	 * @param data a byte array, which should be filled with data from the stream
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
	/**
	 * readLine reads one line terminated by a newline character and returns
	 * it without the newline character. This operation can be blocking if there
	 * is no information available (yet)
	 *
	 * @return a string representing the next line from the stream
	 * @throws IOException if reading from the stream fails
	 */
	public String readLine() throws IOException {
		synchronized (fromMonetRaw) {
			String line;
			int nl;

			/*
			 * The blocked stream protocol consists of first an int indicating
			 * the length of the block, then the block, followed by another
			 * length + block or a block with length 0 which indicates a
			 * user flush.
			 * In this implementation we do not detect or use the user flush
			 * as it is not needed to detect for us since the higher level
			 * MAPI protocol defines a prompt which is used for syncronisation.
			 * We simply fetch blocks here as soon as they are needed to
			 * process them line-based.  A side effect of not acting upon the
			 * user flush is that the zero block that is sent, is only read
			 * upon a next retrieval of a block instead of directly after a
			 * block.  This does not harm (since it is empty), but shows up
			 * when viewing the debug log as a zero block right after each
			 * query, since then the next block is fetched.
			 */
			while ((nl = readBuffer.indexOf("\n", readPos)) == -1) {
				// not found, fetch us some more data
				// start reading a new block of data if appropriate
				if (readState == 0) {
					// read next four bytes (int)
					byte[] data = new byte[4];
					int size = fromMonetRaw.read(data);
					if (size == -1) throw
						new IOException("End of stream reached");
					if (size < 4) throw
						new AssertionError("Illegal start of block");

					// start with a new block
					inputBuffer.rewind();
					inputBuffer.put(data);

					// get the int-value and store it in the state
					inputBuffer.rewind();
					readState = inputBuffer.getInt();

					if (readState == 0) {	// user flush
						if (debug) logRx("zero block (flush)");
						// skip the rest, it makes no sense to process
						// zero bytes of data
						continue;
					}
					
					if (debug) logRx("new block: " + readState + " bytes");
				}
				// 'continue' fetching current block
				byte[] data = new byte[readcapacity < readState ? readcapacity : readState];
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
					logRx("read chunk: " + size +
						" bytes, left: " + readState + " bytes");
					logRx(new String(data, "UTF-8"));
				}
			}
			// fill line, excluding newline
			line = readBuffer.substring(readPos, nl);

			// move the cursor position
			readPos = nl + 1;

			lineType = getLineType(line);

			return(line);
		}
	}

	/**
	 * Sets the byte-order to reading data from the server. By default the
	 * byte order is big-endian or network order.
	 *
	 * @param order either ByteOrder.BIG_ENDIAN or ByteOrder.LITTLE_ENDIAN
	 */
	public synchronized void setByteOrder(ByteOrder order) {
		inputBuffer.order(order);
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
	 * Returns whether data can be read from the stream or not.
	 */
	public boolean hasData() {
		synchronized (fromMonetRaw) {
			try {
				return(fromMonetRaw.available() > 0);
			} catch (IOException e) {
				return(false);
			}
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
}
