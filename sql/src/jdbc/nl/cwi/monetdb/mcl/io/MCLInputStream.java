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

package nl.cwi.monetdb.mcl.io;

import java.io.*;
import java.nio.*;
import nl.cwi.monetdb.mcl.*;
import nl.cwi.monetdb.mcl.messages.*;

/**
 * An MCLInputStream allows to conveniently read sentences of the
 * associated input stream.  MCL sentences are presented on the wire as
 * sequences of bytes prefixed with their length represented in an
 * 4-byte integer value.  It may be efficient to supply a
 * BufferedInputStream to the constructor of this class to increase
 * performance.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class MCLInputStream {
	private final InputStream in;
	private final ByteBuffer bbuf;

	private int lastType = 0;	// there is no line type with (char)0

	/**
	 * Creates a MCLInputStream and saves its argument, the input stream
	 * in for later use.
	 *
	 * @param in the underlying input stream
	 */
	public MCLInputStream(InputStream in) {
		this.in = in;
		bbuf = ByteBuffer.allocate(4);	// an int
	}

	/**
	 * Returns the next sentence on the stream as a Sentence object.
	 * This method will block if no data is available.
	 *
	 * @return the sentence read
	 * @throws MCLException if an IO error occurs or the sentence seems
	 *         to be invalid
	 */
	public synchronized MCLSentence readSentence() throws MCLException {
		try {
			byte[] data = new byte[4];
			int size = in.read(data);
			if (size == -1) throw
				new MCLIOException("End of stream reached");
			if (size < 4) throw
				new MCLIOException("Illegal start of sentence (not enough data available)");

			// put the four bytes in the ByteBuffer
			bbuf.rewind();
			bbuf.put(data);

			// get the int-value and store it in the state
			bbuf.rewind();
			size = bbuf.getInt();

			if (size < 0) throw
				new MCLException("Illegal sentence length: " + size);

			data = new byte[size - 1]; // minus the first byte (identifier)
			lastType = in.read();
			if (lastType < 0) throw
				new MCLIOException("End of stream reached");
			if (in.read(data) != size) throw
				new MCLIOException("Unable to read complete sentence");

			// note: MCLSentence constructor throws MCLException
			return(new MCLSentence(lastType, data));
		} catch (IOException e) {
			throw new MCLIOException("IO operation failed: " + e.getMessage());
		}
	}

	/**
	 * Tries to sync the protocol dialog by skipping sentences until
	 * the prompt is found.  After an error has occurred when reading
	 * sentences, like when an unknown sentence has been encountered,
	 * a call of sync() tries to resync the protocol in order to allow
	 * the client to continue the session.  If syncing fails (for
	 * example because the stream is closed) an Exception is thrown.
	 * Calling the method sync() on an MCLInputStream which is already
	 * in sync (for instance when calling sync() twice) is a no-op, and
	 * does not affect the stream in any way.
	 *
	 * @throws MCLException if syncing failed
	 */
	public void sync() throws MCLException {
		while (lastType != MCLSentence.PROMPT) {
			try {
				readSentence();
			} catch (MCLIOException e) {
				// Hey look! we can catch a sub-exception of the
				// Exception being thrown! :)  Doesn't that save us from
				// a lot of work? ;;)  Anywayz, this is fatal, so
				// rethrow it
				throw e;
			} catch (MCLException e) {
				// we can just ignore this, for this error is
				// recoverable
			}
		}
	}

	/**
	 * Closes the input stream associated to this MCLInputStream.
	 */
	public void close() {
		try {
			in.close();
		} catch (IOException e) {
			// ignore
		}
	}

	/**
	 * Makes sure IO resources are released by closing this object.
	 */
	protected void finalize() {
		close();
	}
}
