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

package nl.cwi.monetdb.mcl.io;

import java.io.*;

/**
 * Write text to a character-output stream, buffering characters so as
 * to provide for the efficient writing of single characters, arrays,
 * and strings.
 *
 * In contrast to the BufferedWriter class, this class' newLine()
 * method always writes the newline character '\n', regardless the
 * platform's own notion of line separator.
 *
 * @author Fabian Groffen <Fabian.Groffen>
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
	public BufferedMCLWriter(OutputStream in) {
		super(new OutputStreamWriter(in));
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
