/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

/**
 * Read text from a character-input stream, buffering characters so as
 * to provide a means for efficient reading of characters, arrays and
 * lines.  This class is based on the BufferedReader class, and provides
 * extra functionality useful for MCL.
 * 
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
	 * @param enc Encoding
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
	 * Warning: until the server properly prefixes all of its error
	 * messages with SQLSTATE codes, this method prefixes all errors it
	 * sees without sqlstate with the generic data exception code
	 * (22000).
	 *
	 * @return A String containing the contents of the line, not
	 *         including any line-termination characters, or null if the
	 *         end of the stream has been reached
	 * @throws IOException If an I/O error occurs
	 */
	@Override
	public String readLine() throws IOException {
		String r = super.readLine();
		setLineType(r);
		if (lineType == ERROR && !r.matches("^![0-9A-Z]{5}!.+"))
			r = "!22000!" + r.substring(1);
		return r;
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
		return lineType;
	}

	/**
	 * Reads up till the MonetDB prompt, indicating the server is ready
	 * for a new command.  All read data is discarded.  If the last line
	 * read by readLine() was a prompt, this method will immediately
	 * return.
	 * 
	 * If there are errors present in the lines that are read, then they
	 * are put in one string and returned <b>after</b> the prompt has
	 * been found. If no errors are present, null will be returned.
	 *
	 * @return a string containing error messages, or null if there aren't any
	 * @throws IOException if an IO exception occurs while talking to the server
	 * 
	 * TODO(Wouter): should probably not have to be synchronized. + StringBuilder...
	 */
	final public synchronized String waitForPrompt() throws IOException {
		String ret = "", tmp;
		while (lineType != PROMPT) {
			if ((tmp = readLine()) == null)
				throw new IOException("Connection to server lost!");
			if (lineType == ERROR)
				ret += "\n" + tmp.substring(1);
		}
		return ret == "" ? null : ret.trim();
	}

}
