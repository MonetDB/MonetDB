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
 * A ChallengeMessage is a server originated message, sent right after
 * the client made its connection.  In a ChallengeMessage, the server
 * identifies itself to the client, and provides a seed that the client
 * should use to salt the password when sending it back to the server.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class ChallengeMessage extends MCLMessage {
	/** The character that identifies this message */
	public static final char identifier = 'C';

	private final static MCLSentence startOfMessageSentence;
	
	static {
		try {
			startOfMessageSentence = new MCLSentence('&', "" + identifier);
		} catch (MCLException e) {
			throw new AssertionError("Unable to create core sentence");
		}
	}

	// these represent the interval values of this Message
	private int protover;
	private String server;
	private String seed;
	
	/**
	 * Constructs an empty ChallengeMessage.  The sentences need to be
	 * added using the addSentence() method.  This constructor is
	 * suitable when reconstructing messages from a stream.
	 */
	public ChallengeMessage() {
		// nothing has to be done here
		sentences = new MCLSentence[3];
	}

	/**
	 * Constructs a filled ChallengeMessage.  All required information
	 * is supplied and stored in this ChallengeMessage.
	 *
	 * @param protover the protocol version
	 * @param server the server identification string
	 * @throws MCLException if the server string is null
	 */
	public ChallengeMessage(int protover, String server) throws MCLException {
		if (server == null) throw
			new MCLException("Server identification string may not be null");
		
		sentences = new MCLSentence[3];
		this.protover = protover;
		this.server = server;
		sentences[0] = new MCLSentence('$', "protover", "" + protover);
		sentences[1] = new MCLSentence('$', "server", server);
		generateSeed();
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
	 * Returns the start of message sentence for this Message: &amp;C.
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


		if (prop.equals("protover")) {
			try {
				protover = Integer.parseInt(value);
				sentences[0] = in;
			} catch (NumberFormatException e) {
				throw new MCLException("Illegal value for header 'protover': " + value);
			}
		} else if (prop.equals("server")) {
			if (value.equals("mserver") || value.equals("dbpool")) {
				server = value;
				sentences[1] = in;
			} else {
				throw new MCLException("Illegal value for header 'server': " + value);
			}
		} else if (prop.equals("seed")) {
			if (value.length() < 7 || value.length() > 19) throw
				new MCLException("Illegal value for header 'seed': " + value);

			seed = value;
			sentences[2] = in;
		} else {
			throw new MCLException("Illegal property '" + prop + "' for this Message");
		}
	}


	/**
	 * Generates a random seed and stores it in the sentences associated
	 * with this Message.  If the sentence already exists, it is
	 * silently overwritten.
	 *
	 * @throws MCLException if creation of the seed MCLSentence fails
	 */
	public void generateSeed() throws MCLException {
		/* Perform some random stuff:
		 * - first pick a random length of the seed, between 9 and 17
		 *   characters (inclusive)
		 * - pick a random character [a-z][A-Z][0-9]
		 */
		Random rand = new Random();
		seed = "";
		int length = 9 + rand.nextInt(17 - 9 + 1);
		for (int i = 0; i < length; i++) {
			switch(rand.nextInt(3)) {
				case 0:
					seed += (char)('a' + rand.nextInt('z' - 'a' + 1));
				break;
				case 1:
					seed += (char)('A' + rand.nextInt('Z' - 'A' + 1));
				break;
				case 2:
					seed += (char)('0' + rand.nextInt('9' - '0' + 1));
				break;
			}
		}
		sentences[2] = new MCLSentence('$', "seed", seed);
	}

	// the following are message specific getters that retrieve the
	// values inside the message

	/**
	 * Retrieves the protocol version contained in this Message object.
	 *
	 * @return the protocol version
	 */
	public int getProtover() {
		return(protover);
	}

	/**
	 * Retrieves the server identification string contained in this
	 * Message object.
	 *
	 * @return the server string
	 */
	public String getServer() {
		return(server);
	}

	/**
	 * Retrieves the seed contained in this Message object.
	 *
	 * @return the seed
	 */
	public String getSeed() {
		return(seed);
	}
}
