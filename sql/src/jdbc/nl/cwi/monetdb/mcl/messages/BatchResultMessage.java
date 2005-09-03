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
 * An BatchResultMessage is a server received message, sent by the
 * client as a response to a PrepareResultMessage that identifies which
 * unknown values represented by '?' characters are to be filled in.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class BatchResultMessage extends MCLVariableMessage {
	/** The character that identifies this message */
	public static final char identifier = 'B';

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
	private int tuplecount;

	/**
	 * Constructs an empty BatchResultMessage.  The sentences need
	 * to be added using the addSentence() method.  This constructor is
	 * suitable when reconstructing messages from a stream.
	 */
	public BatchResultMessage() {
		// nothing has to be done here
		sentences = new MCLSentence[1];
		variableSentences = new ArrayList();
	}

	/**
	 * Constructs a filled BatchResultMessage.  All required header
	 * information is supplied and stored in this
	 * BatchResultMessage.  To add the data, the method
	 * addDataSentence() should be used.
	 *
	 * @param values the values to store in this BatchResultMessage
	 * @throws MCLException if one of the arguments is null or zero
	 */
	public BatchResultMessage(int[] values) throws MCLException {
		if (tuplecount == 0) throw
			new MCLException("tuplecount may not be zero");
		
		sentences = new MCLSentence[1];
		tuplecount = values.length;
		sentences[0] = new MCLSentence(MCLSentence.MCLMETADATA, "tuplecount", "" + tuplecount);
		variableSentences = new ArrayList(tuplecount);
		for (int i = 0; i < tuplecount; i++) {
			addDataSentence(
					new MCLSentence(MCLSentence.DATA, "" + values[i])
			);
		}
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
		switch (in.getType()) {
			case MCLSentence.MCLMETADATA:
				/* Warning: currently we don't check for order or
				 * duplicates, neither do we check whether we have all
				 * required headers already. */
				String prop = in.getField(1);
				if (prop == null) throw
					new MCLException("Illegal sentence (no property): " + in.getString());
				String value = in.getField(2);
				if (value == null) throw
					new MCLException("Illegal sentence (no value): " + in.getString());
				
				if ("tuplecount".equals(prop)) {
					try {
						tuplecount = Integer.parseInt(value);
					} catch (NumberFormatException e) {
						throw new MCLException("tuplecount (" + value + ") is not a parsable number");
					}
					sentences[0] = in;
				} else {
					throw new MCLException("Unsupported property: " + prop);
				}
			break;
			case MCLSentence.DATA:
				addDataSentence(in);
			break;
			default:
				throw new MCLException("Sentence type not allowed for this message: " + (char)in.getType());
		}
	}

	/**
	 * Adds a data sentence to this message.  The message is added as
	 * last message in an (ordered) list.  Upon attempt to store more
	 * messages in this Message than its tuplecount header says it can
	 * contain, the given sentence is not a data message, or the
	 * tuplecount header is not yet set, an MCLException is thrown.
	 *
	 * @param data the MCLSentence to add as data 
	 * @throws MCLException if the input parameter is null or incorrect,
	 * or if there are already tuplecount number of sentences as data.
	 */
	public void addDataSentence(MCLSentence data) throws MCLException {
		if (data == null) throw
			new MCLException("sentence should not be null");
		if (data.getType() != MCLSentence.DATA) throw
			new MCLException("data sentence should have data line type");
		// the data from the Sentence should never be null, but
		// who knows...
		if (data.getString() == null) throw
			new MCLException("data cannot be null!");

		// check bounds
		if (variableSentences.size() == tuplecount) throw
			new MCLException("Message inconsistency: got more tuples (" + variableSentences.size() + ") than tuplecount would suggest (" + tuplecount + ")");
		// just add the sentence
		variableSentences.add(data);
	}

	// the following are message specific getters that retrieve the
	// values inside the message

	/**
	 * Retrieves the tuple count contained in this Message object.
	 *
	 * @return the tuple count
	 */
	public int getTupleCount() {
		return(tuplecount);
	}
}
