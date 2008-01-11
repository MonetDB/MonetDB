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
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.mcl.messages;

import java.util.*;
import nl.cwi.monetdb.mcl.*;

/**
 * A PrepareMessage is a server received message, sent by the client in
 * order to prepare a query that will be sent later which has optional
 * unknown values represented by '?' characters.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class PrepareMessage extends MCLMessage {
	/** The character that identifies this message */
	public static final char identifier = 'p';

	private final static MCLSentence startOfMessageSentence;
	
	static {
		try {
			startOfMessageSentence =
				new MCLSentence(MCLSentence.STARTOFMESSAGE, "" + identifier);
		} catch (MCLException e) {
			throw new AssertionError("Unable to create core sentence");
		}
	}

	// these represent the internal values of this Message
	private String data;
	
	/**
	 * Constructs an empty PrepareMessage.  The sentences need to be
	 * added using the addSentence() method.  This constructor is
	 * suitable when reconstructing messages from a stream.
	 */
	public PrepareMessage() {
		// nothing has to be done here
		sentences = new MCLSentence[1];
	}

	/**
	 * Constructs a filled PreparedMessage.  All required
	 * information is supplied and stored in this
	 * PrepareMessage.
	 *
	 * @param data the prepare query
	 * @throws MCLException if the data string is null
	 */
	public PrepareMessage(String data) throws MCLException {
		if (data == null) throw
			new MCLException("Query data may not be null");
		
		sentences = new MCLSentence[1];
		this.data = data;
		sentences[0] = new MCLSentence(MCLSentence.QUERY, data);
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
	 * Returns the start of message sentence for this Message: &amp;p.
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
		// see if it is a supported line
		if (in.getType() != MCLSentence.QUERY) throw
			new MCLException("Sentence type not allowed for this message: " + (char)in.getType());
		data = in.getString();
		if (data == null) throw
			new MCLException("Illegal sentence (no data)");
		sentences[0] = in;
	}


	// the following are message specific getters that retrieve the
	// values inside the message

	/**
	 * Retrieves the query data contained in this Message object.
	 *
	 * @return the query data
	 */
	public String getData() {
		return(data);
	}
}
