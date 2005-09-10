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

package nl.cwi.monetdb.mcl.messages;

import java.util.*;
import nl.cwi.monetdb.mcl.*;

/**
 * A DataMessage is a server originated message, sent by the
 * server to ship a (part of a) tabular result.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class DataMessage extends MCLVariableMessage {
	/** The character that identifies this message */
	public static final char identifier = 'd';

	private final static MCLSentence startOfMessageSentence;
	
	static {
		try {
			startOfMessageSentence = new MCLSentence('&', "" + identifier);
		} catch (MCLException e) {
			throw new AssertionError("Unable to create core sentence");
		}
	}

	// these represent the internal values of this Message
	private int rowcount;
	
	/**
	 * Constructs an empty DataMessage.  The sentences need to
	 * be added using the addSentence() method.  This constructor is
	 * suitable when reconstructing messages from a stream.
	 */
	public DataMessage() {
		// nothing has to be done here
		sentences = new MCLSentence[1];
		variableSentences = new ArrayList();
	}

	/**
	 * Constructs a filled DataMessage.  All MCL metadata information
	 * is supplied and stored in this DataMessage.
	 *
	 * @param rowcount the number of rows in this result
	 * @throws MCLException if the rowcount is zero
	 */
	public DataMessage(int rowcount) throws MCLException {
		if (rowcount == 0) throw
			new MCLException("A data set with no rows makes no sense");

		sentences = new MCLSentence[1];
		this.rowcount = rowcount;
		sentences[0] = new MCLSentence('$', "rowcount", "" + rowcount);
		variableSentences = new ArrayList(rowcount);
	}

	/**
	 * Returns the type of this Message as an integer type.
	 * 
	 * @return an integer value that represents the type of this Message
	 */
	public int getType() {
		return(identifier);
	}

	/**
	 * Returns the start of message sentence for this Message: &amp;e.
	 *
	 * @return the start of message sentence
	 */
	public MCLSentence getSomSentence() {
		return(startOfMessageSentence);
	}


	/**
	 * Adds the given String to this Message if it matches the Message
	 * type.  The sentence is parsed as far as that is considered to be
	 * necessary to validate it against the Message type.  If a sentence
	 * is not valid, an MCLException is thrown.
	 * 
	 * @param in an MCLSentence object
	 * @throws MCLException if the given sentence is not considered to
	 * be valid
	 */
	public void addSentence(MCLSentence in) throws MCLException {
		String prop;
		// see if it is a supported header
		switch (in.getType()) {
			case MCLSentence.MCLMETADATA:
				prop = in.getField(1);
				if (prop == null) throw
					new MCLException("Illegal sentence (no property): " + in.getString());
				String value = in.getField(2);
				if (value == null) throw
					new MCLException("Illegal sentence (no value): " + in.getString());

				if (prop.equals("rowcount")) {
					try {
						rowcount = Integer.parseInt(value);
						if (rowcount <= 0) throw
							new MCLException("rowcount cannot be less than zero (" + rowcount + ")");
						sentences[0] = in;
					} catch (NumberFormatException e) {
						throw new MCLException("Illegal value for header 'rowcount': " + value);
					}
				}
			break;
			case MCLSentence.DATA:
				if (variableSentences.size() == rowcount) throw
					new MCLException("This Message already contains rowcount (" + rowcount + ") data sentences");
				variableSentences.add(in);
			break;
			default:
				throw new MCLException("Sentence type not allowed for this message: " + (char)in.getType());
		}
	}


	// the following are message specific getters that retrieve the
	// values inside the message

	/**
	 * Retrieves the column count contained in this Message object.
	 *
	 * @return the column count
	 */
	public int getRowCount() {
		return(rowcount);
	}
}
