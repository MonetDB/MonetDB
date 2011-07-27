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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.mcl.io;

import java.io.*;

/**
 * Write text to a character-output stream, buffering characters so as
 * to provide a means for efficient writing of single characters,
 * arrays, and strings.
 *
 * In contrast to the BufferedWriter class, this class' newLine()
 * method always writes the newline character '\n', regardless the
 * platform's own notion of line separator.  Apart from that there are
 * no differences in the behaviour of this class, compared to its parent
 * class, the BufferedWriter.  A small convenience is built into this
 * class for cooperation with the BufferedMCLReader, via the
 * registerReader() method.  It causes the reader to be reset upon each
 * write performed though this class.  This effectuates the MCL protocol
 * flow where a write invalidates the state of the read buffers, since
 * each write must be answered by the server.  That also makes this
 * class client-oriented when a reader is registered.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 * @see nl.cwi.monetdb.mcl.net.MapiSocket
 * @see nl.cwi.monetdb.mcl.io.BufferedMCLWriter
 */
public class BufferedMCLWriter extends BufferedWriter {
	private BufferedMCLReader reader;

	/**
	 * Create a buffered character-output stream that uses a
	 * default-sized output buffer.
	 *
	 * @param in A Writer
	 */
	public BufferedMCLWriter(Writer in) {
		super(in);
	}

	/**
	 * Create a buffered character-output stream that uses a
	 * default-sized output buffer, from an OutputStream.
	 *
	 * @param in An OutputStream
	 */
	public BufferedMCLWriter(OutputStream in, String enc)
		throws UnsupportedEncodingException
	{
		super(new OutputStreamWriter(in, enc));
	}

	/**
	 * Registers the given reader in this writer.  A registered reader
	 * receives a linetype reset when a line is written from this
	 * writer.
	 *
	 * @param r an BufferedMCLReader
	 */
	public void registerReader(BufferedMCLReader r) {
		reader = r;
	}

	/**
	 * Write a line separator.  The line separator string is in this
	 * class always the single newline character '\n'.
	 *
	 * @throws IOException If an I/O error occurs
	 */
	public void newLine() throws IOException {
		write('\n');
	}

	/**
	 * Write a single line, terminated with a line separator, and flush
	 * the stream.  This is a shorthand method for a call to write()
	 * and flush().
	 *
	 * @throws IOException If an I/O error occurs
	 */
	public void writeLine(String line) throws IOException {
		write(line);
		flush();
		// reset reader state, last line isn't valid any more now
		if (reader != null) reader.setLineType(null);
	}
}
