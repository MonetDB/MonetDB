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

package nl.cwi.monetdb.mcl.messages;

import java.util.*;
import nl.cwi.monetdb.mcl.*;

/**
 * A BatchMessage is a server received message, sent by the client to
 * execute a batch of statements.  The statements are all required to be
 * statements which does not result in a relational output.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class BatchMessage extends MCLVariableMessage {
	/** The character that identifies this message */
	public static final char identifier = 'b';

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
	 * Constructs an empty BatchMessage.  The sentences need to be added
	 * using the addSentence() method.  This constructor is suitable
	 * when reconstructing messages from a stream.
	 */
	public BatchMessage() {
		// nothing has to be done here
		sentences = new MCLSentence[0];
		variableSentences = new ArrayList();
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
	 * Adds a query statement to this Message's batch.  There can only be one data
	 * Sentence in a QueryMessage.  This method throws an MCLException
	 * if the data Sentence is already set.
	 *
	 * @param sentence the Sentence to add
	 * @throws MCLException if the given Sentence is invalid or a data
	 * Sentence is already set.
	 */
	public void addDataSentence(MCLSentence sentence)
		throws MCLException
	{
		if (sentence == null) throw
			new MCLException("sentence may not be null");
		if (sentence.getType() != MCLSentence.QUERY) throw
			new MCLException("sentence should be of query type");

		variableSentences.add(sentence);
	}

	/**
	 * Adds the given Sentence to this Message if it matches the Message
	 * type.  The sentence is parsed as far as that is considered to be
	 * necessary to validate it against the Message type.  If a sentence
	 * is not valid, an MCLException is thrown.
	 * 
	 * @param in an MCLSentence object
	 * @throws MCLException if the given sentence is not considered to
	 * be valid
	 */
	public void addSentence(MCLSentence in) throws MCLException {
		// since we only have data sentences, we can defer the validity
		// check to the addDataSentence() method
		addDataSentence(in);
	}
}
