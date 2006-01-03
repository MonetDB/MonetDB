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
 * Portions created by CWI are Copyright (C) 1997-2006 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.mcl.io;

import java.io.*;
import java.nio.*;
import nl.cwi.monetdb.mcl.*;
import nl.cwi.monetdb.mcl.messages.*;

/**
 * This class provides an implementation of an output stream geared
 * towards MCL sentences.  Setting up an MCLOutputStream allows an
 * application to write MCL sentences to the underlying output stream in
 * the right way, conforming to the MCL specifications.  It may be
 * efficient to supply a BufferedOutputStream to the constructor of this
 * class to increase performance.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class MCLOutputStream {
	private final OutputStream out;
	private final ByteBuffer bbuf;

	/**
	 * Creates a MCLOutputStream and saves its argument, the output stream
	 * out for later use.
	 *
	 * @param out the underlying output stream
	 */
	public MCLOutputStream(OutputStream out) {
		this.out = out;
		bbuf = ByteBuffer.allocate(4);	// an int
	}

	/**
	 * Writes the given sentence to the underlying stream.
	 *
	 * @param sentence the sentence to write
	 * @throws MCLException if an IO error occurs
	 */
	public void writeSentence(MCLSentence sentence) throws MCLException {
		try {
			byte[] data = new byte[4];
			byte[] value = sentence.getData();
			bbuf.rewind();
			bbuf.putInt(value.length + 1); // include linetype specifier

			bbuf.rewind();
			bbuf.get(data);

			// write the length of the sentence to come
			out.write(data);
			// write the linetype identifier
			out.write(sentence.getType());
			// write the actual data block
			out.write(value);
		} catch (IOException e) {
			throw new MCLException("IO operation failed: " + e.getMessage());
		}
	}

	/**
	 * Issues a flush to the underlying output stream.
	 */
	public void flush() {
		try {
			out.flush();
		} catch (IOException e) {
			// ignore
		}
	}

	/**
	 * Closes the input stream associated to this MCLInputStream.
	 */
	public void close() {
		try {
			out.close();
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
