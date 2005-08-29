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
 * An AffectedRowsMessage returns the number of rows affected by a query
 * or operation in a single row as single value.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class AffectedRowsMessage extends MCLMessage {
	/** The character that identifies this message */
	public static final char identifier = 'u';

	private final static MCLSentence startOfMessageSentence;
	
	static {
		try {
			startOfMessageSentence =
				new MCLSentence(MCLSentence.STARTOFMESSAGE, "" + identifier);
		} catch (MCLException e) {
			throw new AssertionError("Unable to create core sentence");
		}
	}

	// internal data
	private int affectedRows;

	/**
	 * Constructs an empty AffectedRowsMessage.  The sentences need to be
	 * added using the addSentence() method.  This constructor is
	 * suitable when reconstructing messages from a stream.
	 */
	public AffectedRowsMessage() {
		// nothing has to be done here
		sentences = new MCLSentence[1];
	}

	/**
	 * Constructs a filled AffectedRowsMessage.  All required information
	 * is supplied and stored in this AffectedRowsMessage.
	 *
	 * @param count the number of affected rows
	 * @throws MCLException if the data is null
	 */
	public AffectedRowsMessage(int count) throws MCLException {
		sentences = new MCLSentence[1];
		this.affectedRows = count;
		sentences[0] = new MCLSentence(MCLSentence.DATA, "" + count);
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
		// see if it is a supported header
		if (in.getType() == MCLSentence.DATA) {
			if (sentences[0] != null) throw
				new MCLException("Data sentence already set, can only have one!");
			try {
				affectedRows = Integer.parseInt(in.getString());
				sentences[0] = in;
			} catch (NumberFormatException e) {
				throw new MCLException("illegal value: " + in.getString());
			}
		} else {
			throw new MCLException("Sentence type not allowed for this message: " + (char)in.getType());
		}
	}


	// the following are message specific getters that retrieve the
	// values inside the message

	/**
	 * Retrieves the number of affected rows contained in this Message object.
	 *
	 * @return the number of affected rows
	 * @throws MCLException if the number is not yet set
	 */
	public int getAffectedRows() throws MCLException {
		if (sentences[0] == null) throw
			new MCLException("Number of affected rows not yet set!");

		return(affectedRows);
	}
}
