package nl.cwi.monetdb.jdbc;

import java.io.*;
import java.net.*;

/**
 * A Socket for communicating with the Monet database in block mode
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
 * every interaction to and from the Monet server is logged to a file on disk.
 * Incomming messages are prefixed by "&lt;&lt;", outgoing messages by
 * "&gt;&gt;".
 * <br /><br />
 * This implementation of MonetSocket uses block mode on the mapi protocol in
 * order to circumvent the drawbacks of line mode. It allows sending a multi
 * line query, and should be less intensive for the server.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 0.1 (part of MonetDB JDBC beta release)
 */
class MonetSocketBlockMode extends MonetSocket {
	private InputStream fromMonetRaw;
	private OutputStream toMonetRaw;

	private StringBuffer readBuffer;
	private StringBuffer writeBuffer;

	MonetSocketBlockMode(String host, int port) throws IOException {
		super(new Socket(host, port));

		fromMonetRaw = con.getInputStream();
		toMonetRaw = con.getOutputStream();

		readBuffer = new StringBuffer();
		writeBuffer = new StringBuffer();
	}

	/**
	 * write puts the given string in the buffer as is.
	 * The stream will not be flushed after the write.
	 * To flush the stream use flush(), or use writeln().
	 *
	 * @param data the data to write to the stream
	 * @throws IOException if writing to the stream failed
	 * @see flush(), writeln()
	 */
	public synchronized void write(String data) throws IOException {
		writeBuffer.append(data);
		// reset the lineType variable, since we've sent data now and the last
		// line isn't valid anymore
		lineType = EMPTY;
	}

	/**
	 * flushes the stream to monet, forcing all data in the buffer to be
	 * actually written to the stream.
	 *
	 * @throws IOException if writing to the stream failed
	 */
	public synchronized void flush() throws IOException {
		// write the length of this block
		toMonetRaw.write(writeBuffer.length());	// note: writes a byte, requires an int
		// write the data
		toMonetRaw.write(writeBuffer.toString().getBytes());
		// and flush...
		toMonetRaw.flush();

		// it's a bit nasty if an exception is thrown from the log,
		// but ignoring it can be nasty as well, so it is decided to
		// let it go so there is feedback about something going wrong
		// it's a bit nasty if an exception is thrown from the log,
		// but ignoring it can be nasty as well, so it is decided to
		// let it go so there is feedback about something going wrong
		if (debug) {
			log.write("<<" + writeBuffer.length() + ") " +
				writeBuffer.toString() + "\n");
			log.flush();
		}

		// clear the write buffer; do not create a new object here for
		// performance's sake
		writeBuffer.delete(0, writeBuffer.length());
	}

	/**
	 * writeln puts the given string on the stream
	 * and flushes the stream afterwards so the data will actually be sent
	 *
	 * @param data the data to write to the stream
	 * @throws IOException if writing to the stream failed
	 */
	public synchronized void writeln(String data) throws IOException {
		write(data);
		flush();
	}

	/**
	 * Reads up to count bytes from the stream, and returns them in a byte array
	 *
	 * @param count the number of bytes to read
	 * @return an array containing the read bytes
	 * @throws IOException if some IO error occurs or the requested bytes could
	 *         not be read
	 */
	public synchronized byte[] read(int count) throws IOException {
		// read the data
		byte[] data = new byte[count];
		int size = fromMonetRaw.read(data);
		// note: this catches also end of stream (-1)
		if (size != count) throw
			new IOException("Stream did not contain (full) block!");

		// it's a bit nasty if an exception is thrown from the log,
		// but ignoring it can be nasty as well, so it is decided to
		// let it go so there is feedback about something going wrong
		if (debug) {
			log.write(">> " + new String(data) + "\n");
			log.flush();
		}

		return(data);
	}

	/**
	 * readLine reads one line terminated by a newline character and returns
	 * it without the newline character. This operation can be blocking if there
	 * is no information available (yet)
	 *
	 * @return a string representing the next line from the stream or null if
	 *         the stream is closed and no data is available (end of stream)
	 * @throws IOException if reading from the stream fails
	 */
	public synchronized String readLine() throws IOException {
		String line;
		int nl;
		while ((nl = readBuffer.indexOf("\n")) == -1) {
			// not found, fetch us some more data

			// get the block size
			int len = fromMonetRaw.read();	// note: reads byte, returns an int
			if (len == -1) throw new IOException("End of stream reached!");

			// read the data
			byte[] data = new byte[len];
			int size = fromMonetRaw.read(data);
			// note: this catches also end of stream (-1)
			if (size != len) throw
				new IOException("Stream did not contain (full) block!");

			// append the stuff to the buffer; let String do the charset
			// conversion stuff
			readBuffer.append(new String(data));

			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			if (debug) {
				log.write(">> " + new String(data) + "\n");
				log.flush();
			}
		}

		// fill line, excluding newline
		line = readBuffer.substring(0, nl);

		// remove from the buffer, including newline
		readBuffer.delete(0, nl + 1);

		if (line.length() != 0) {
			switch (line.charAt(0)) {
				case '!': lineType = ERROR; break;
				case '#': lineType = HEADER; break;
				case '[': lineType = RESULT; break;
				default:
					if (MonetDriver.prompt1.equals(line)) {
						lineType = PROMPT1;	// prompt1 found
					} else if (MonetDriver.prompt2.equals(line)) {
						lineType = PROMPT2;	// prompt2 found
					} else {
						lineType = EMPTY;	// unknown :-(
					}
				break;
			}
		} else {
			// I really think empty lines should not be sent by the server
			throw new IOException("MonetBadTasteException: it sent us an empty line");
		}

		return(line);
	}

	/**
	 * disconnect closes the streams and socket connected to the Monet server
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
}

