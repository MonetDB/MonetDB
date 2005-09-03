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
 * A SuccessMessage is a server originated message, that indicates the
 * requested operation was completed successfully.  There can optionally
 * be some extra information stored as info in this MCLMessage.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class SuccessMessage extends MCLVariableMessage {
	/** The character that identifies this message */
	public static final char identifier = 'v';

	private final static MCLSentence startOfMessageSentence;
	
	static {
		try {
			startOfMessageSentence =
				new MCLSentence(MCLSentence.STARTOFMESSAGE, "" + identifier);
		} catch (MCLException e) {
			throw new AssertionError("Unable to create core sentence");
		}
	}

	/**
	 * Constructs an empty SuccessMessage.  The sentences need to be
	 * added using the addSentence() method.  This constructor is
	 * suitable when reconstructing messages from a stream.
	 */
	public SuccessMessage() {
		// nothing has to be done here
		sentences = new MCLSentence[0];
		variableSentences = new ArrayList();
	}

	/**
	 * Constructs a filled SuccessMessage.  All required information
	 * is supplied and stored in this SuccessMessage.
	 *
	 * @param message the extra info string
	 * @throws MCLException if the message is null
	 */
	public SuccessMessage(String message) throws MCLException {
		if (message == null) throw
			new MCLException("message may not be null");
		
		sentences = new MCLSentence[0];
		variableSentences = new ArrayList();
		variableSentences.add(new MCLSentence(MCLSentence.INFO, message));
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
		if (in.getType() == MCLSentence.INFO) {
			variableSentences.add(in);
		} else {
			throw new MCLException("Sentence type not allowed for this message: " + (char)in.getType());
		}
	}
}
