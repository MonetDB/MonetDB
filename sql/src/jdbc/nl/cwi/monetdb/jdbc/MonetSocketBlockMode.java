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
 * @version 2.0
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

	/** The maximum size of the chunks we fetch data from the stream with */
	private final int capacity;

	MonetSocketBlockMode(String host, int port, int blocksize) throws IOException {
		super(new Socket(host, port));

		fromMonetRaw = new BufferedInputStream(con.getInputStream());
		toMonetRaw = new BufferedOutputStream(con.getOutputStream());

		outputBuffer = ByteBuffer.allocateDirect(4);
		inputBuffer = ByteBuffer.allocateDirect(4);
		// leave the buffer byte-order as is, it can be modified later
		// by using setByteOrder()

		readBuffer = new StringBuffer();

		// set the blocksize, use hardcoded default on 0 or negative
		capacity = blocksize <= 0 ? 8192 : blocksize;
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
		write(data.getBytes());
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
			if (debug) {
				log.write("<< " + new String(data) + "\n");
			}

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
			int len = data.length();
			int todo = len;
			int blocksize;
			while (todo > 0) {
				// write the length of this block
				blocksize = Math.min(todo, capacity);
				outputBuffer.rewind();
				outputBuffer.putInt(blocksize);

				outputBuffer.rewind();
				outputBuffer.get(blklen);

				toMonetRaw.write(blklen);
				toMonetRaw.write(data.substring(len - todo, (len - todo) + blocksize).getBytes());
				
				if (debug) {
					log.write("<< write block: " + blocksize + " bytes\n");
					log.write(data.substring(len - todo, (len - todo) + blocksize) + "\n");
				}

				todo -= blocksize;
			}

			// the server wants an empty int as termination
			toMonetRaw.write(BLK_TERMINATOR);
			flush();

			if (debug) {
				log.write("<< zero block\n");
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
			if (debug) {
				log.write(">> " + (new String(data)).substring(0, size) + "\n");
				log.flush();
			}

			return(size);
		}
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
		synchronized (fromMonetRaw) {
			String line;
			int nl;
			while ((nl = readBuffer.indexOf("\n")) == -1) {
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

					if (debug) {
						log.write(">> new block: " + readState + " bytes\n");
						log.flush();
					}
				}
				// 'continue' fetching current block
				byte[] data = new byte[Math.min(capacity, readState)];
				int size = fromMonetRaw.read(data);
				if (size == -1) throw
					new IOException("End of stream reached");

				// update the state
				readState -= size;

				// append the stuff to the buffer; let String do the charset
				// conversion stuff
				readBuffer.append((new String(data)).substring(0, size));

				if (debug) {
					log.write(">> read chunk: " + size + " bytes, left: " + readState + " bytes\n");
					log.write((new String(data)).substring(0, size) + "\n");
					log.flush();
				}
			}
			// fill line, excluding newline
			line = readBuffer.substring(0, nl);

			// remove from the buffer, including newline
			readBuffer.delete(0, nl + 1);

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
	 * destructor called by garbage collector before destroying this object
	 * tries to disconnect the MonetDB connection if it has not been disconnected
	 * already
	 */
	protected void finalize() throw Throwable {
		disconnect();
		super.finalize();
	}
}
