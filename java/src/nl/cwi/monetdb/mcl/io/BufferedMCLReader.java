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
 * Read text from a character-input stream, buffering characters so as
 * to provide a means for efficient reading of characters, arrays and
 * lines.  This class is based on the BufferedReader class, and provides
 * extra functionality useful for MCL.
 * <br /><br />
 * The BufferedMCLReader is typically used as layer inbetween an
 * InputStream and a specific interpreter of the data.
 * <pre>
 *                         / Response
 * BufferedMCLReader ---o &lt;- Tuple
 *                         \ DataBlock
 * </pre>
 * Because the BufferedMCLReader provides an efficient way to access the
 * data from the stream in a linewise fashion, whereby each line is
 * identified as a certain type, consumers can easily decide how to
 * parse each retrieved line.  The line parsers from
 * nl.cwi.monetdb.mcl.parser are well suited to work with the lines
 * outputted by the BufferedMCLReader.
 * This class is client-oriented, as it doesn't take into account the
 * messages as the server receives them.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 * @see nl.cwi.monetdb.mcl.net.MapiSocket
 * @see nl.cwi.monetdb.mcl.io.BufferedMCLWriter
 */
public class BufferedMCLReader extends BufferedReader {
	/** The type of the last line read */
	private int lineType;

	/** "there is currently no line", or the the type is unknown is
	    represented by UNKNOWN */
	public final static int UNKNOWN   = 0;
	/** a line starting with ! indicates ERROR */
	public final static int ERROR     = '!';
	/** a line starting with % indicates HEADER */
	public final static int HEADER    = '%';
	/** a line starting with [ indicates RESULT */
	public final static int RESULT    = '[';
	/** a line which matches the pattern of prompt1 is a PROMPT */
	public final static int PROMPT    = '.';
	/** a line which matches the pattern of prompt2 is a MORE */
	public final static int MORE      = ',';
	/** a line starting with &amp; indicates the start of a header block */
	public final static int SOHEADER  = '&';
	/** a line starting with ^ indicates REDIRECT */
	public final static int REDIRECT  = '^';
	/** a line starting with # indicates INFO */
	public final static int INFO      = '#';

	/**
	 * Create a buffering character-input stream that uses a
	 * default-sized input buffer.
	 *
	 * @param in A Reader
	 */
	public BufferedMCLReader(Reader in) {
		super(in);
	}

	/**
	 * Create a buffering character-input stream that uses a
	 * default-sized input buffer, from an InputStream.
	 *
	 * @param in An InputStream
	 */
	public BufferedMCLReader(InputStream in, String enc)
		throws UnsupportedEncodingException
	{
		super(new InputStreamReader(in, enc));
	}

	/**
	 * Read a line of text.  A line is considered to be terminated by
	 * any one of a line feed ('\n'), a carriage return ('\r'), or a
	 * carriage return followed immediately by a linefeed.  Before this
	 * method returns, it sets the linetype to any of the in MCL
	 * recognised line types.
	 *
	 * @return A String containing the contents of the line, not
	 *         including any line-termination characters, or null if the
	 *         end of the stream has been reached
	 * @throws IOException If an I/O error occurs
	 */
	public String readLine() throws IOException {
		String r = super.readLine();
		setLineType(r);
		return(r);
	}

	/**
	 * Sets the linetype to the type of the string given.  If the string
	 * is null, lineType is set to UNKNOWN.
	 *
	 * @param line the string to examine
	 */
	void setLineType(String line) {
		lineType = UNKNOWN;
		if (line == null || line.length() == 0)
			return;
		switch (line.charAt(0)) {
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
			case '=':
				lineType = RESULT;
			break;
			case '^':
				lineType = REDIRECT;
			break;
			case '#':
				lineType = INFO;
			break;
			case '.':
				lineType = PROMPT;
			break;
			case ',':
				lineType = MORE;
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
		while (lineType != PROMPT) {
			if ((tmp = readLine()) == null)
				throw new IOException("Connection to server lost!");
			if (lineType == ERROR) ret += "\n" + tmp.substring(1);
		}
		return(ret == "" ? null : ret.trim());
	}

}
