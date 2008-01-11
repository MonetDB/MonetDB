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
 * An ExportRequestMessage is a server received message, sent by the
 * client in order to retrieve a (part of a) result set.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class ExportRequestMessage extends MCLMessage {
	/** The character that identifies this message */
	public static final char identifier = 'e';

	private final static MCLSentence startOfMessageSentence;
	
	static {
		try {
			startOfMessageSentence = new MCLSentence('&', "" + identifier);
		} catch (MCLException e) {
			throw new AssertionError("Unable to create core sentence");
		}
	}

	// these represent the internal values of this Message
	private String id;
	private int offset;
	private int length;
	
	/**
	 * Constructs an empty ExportRequestMessage.  The sentences need to
	 * be added using the addSentence() method.  This constructor is
	 * suitable when reconstructing messages from a stream.
	 */
	public ExportRequestMessage() {
		// nothing has to be done here
		sentences = new MCLSentence[3];
	}

	/**
	 * Constructs a filled ExportRequestMessage.  All required
	 * information is supplied and stored in this ExportRequestMessage.
	 *
	 * @param id the server side result id
	 * @param offset the index position to start from, inclusive
	 * starting from zero
	 * @param length the maximum number of tuples to receive
	 * @throws MCLException if the id string is null
	 */
	public ExportRequestMessage(String id, int offset, int length) throws MCLException {
		if (id == null) throw
			new MCLException("Result set id may not be null");
		
		sentences = new MCLSentence[3];
		this.id = id;
		this.offset = offset;
		this.length = length;
		sentences[0] = new MCLSentence('$', "id", id);
		sentences[1] = new MCLSentence('$', "offset", "" + offset);
		sentences[2] = new MCLSentence('$', "length", "" + length);
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
		if (in.getType() != '$') throw
			new MCLException("Sentence type not allowed for this message: " + (char)in.getType());
		String prop = in.getField(1);
		if (prop == null) throw
			new MCLException("Illegal sentence (no property): " + in.getString());
		String value = in.getField(2);
		if (value == null) throw
			new MCLException("Illegal sentence (no value): " + in.getString());


		if (prop.equals("id")) {
			sentences[0] = in;
		} else if (prop.equals("offset")) {
			try {
				offset = Integer.parseInt(value);
				if (offset < 0) throw
					new MCLException("Offset cannot be less than zero (" + offset + ")");
				sentences[1] = in;
			} catch (NumberFormatException e) {
				throw new MCLException("Illegal value for header 'offset': " + value);
			}
		} else if (prop.equals("length")) {
			try {
				length = Integer.parseInt(value);
				if (length < 0) throw
					new MCLException("Length cannot be less than zero (" + length + ")");
				sentences[2] = in;
			} catch (NumberFormatException e) {
				throw new MCLException("Illegal value for header 'length': " + value);
			}
		} else {
			throw new MCLException("Illegal property '" + prop + "' for this Message");
		}
	}


	// the following are message specific getters that retrieve the
	// values inside the message

	/**
	 * Retrieves the result set id contained in this Message object.
	 *
	 * @return the result set id
	 */
	public String getId() {
		return(id);
	}

	/**
	 * Retrieves the offset contained in this Message object.
	 *
	 * @return the offset
	 */
	public int getOffset() {
		return(offset);
	}

	/**
	 * Retrieves the length contained in this Message object.
	 *
	 * @return the length
	 */
	public int getLength() {
		return(length);
	}
}
