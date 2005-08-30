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

import java.io.*;
import java.util.*;
import java.security.*;	// for MD5 hash
import nl.cwi.monetdb.mcl.*;

/**
 * A ChallengeResponseMessage is a server received message, sent by the
 * client as a response to a ChallengeMessage.  The client can use this
 * message to authenticate and specify what services it wants to use.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class ChallengeResponseMessage extends MCLMessage {
	/** The character that identifies this message */
	public static final char identifier = 'a';

	private final static MCLSentence startOfMessageSentence;
	
	static {
		try {
			startOfMessageSentence = new MCLSentence('&', "" + identifier);
		} catch (MCLException e) {
			throw new AssertionError("Unable to create core sentence");
		}
	}

	// these represent the interval values of this Message
	private String username;
	private String password;
	private String language;
	private String database;
	
	/**
	 * Constructs an empty ChallengeResponseMessage.  The sentences need
	 * to be added using the addSentence() method.  This constructor is
	 * suitable when reconstructing messages from a stream.
	 */
	public ChallengeResponseMessage() {
		// nothing has to be done here
		sentences = new MCLSentence[4];
	}

	/**
	 * Constructs a filled ChallengeResponseMessage.  All required
	 * information is supplied and stored in this
	 * ChallengeResponseMessage.
	 *
	 * @param username the username
	 * @param password the md5 of the md5 of the password + seed
	 * @param language the language: sql, mal, xquery
	 * @param database the database to use
	 * @throws MCLException if one of the parameters is null
	 */
	public ChallengeResponseMessage(
			String username,
			String password,
			String language,
			String database
	) throws MCLException {
		if (username == null || password == null ||
				language == null || database == null) throw
			new MCLException("Parameters may not be null");
		if (password.length() != 32) throw
			new MCLException("password should be an md5 hash in hex notation");
		
		sentences = new MCLSentence[4];
		this.username = username;
		this.password = password;
		this.language = language;
		this.database = database;
		sentences[0] = new MCLSentence('$', "username", username);
		sentences[1] = new MCLSentence('$', "password", password);
		sentences[2] = new MCLSentence('$', "language", language);
		sentences[3] = new MCLSentence('$', "database", database);
	}

	/**
	 * Convenience constructor to construct a filled
	 * ChallengeResponseMessage with a hashed password.  All required
	 * information is supplied to generate the right hash of the
	 * password.
	 *
	 * @param username the username
	 * @param seed the seed to salt the password with
	 * @param password the cleartext password
	 * @param language the language: sql, mal, xquery
	 * @param database the database to use
	 * @throws MCLException if one of the parameters is null
	 */
	public ChallengeResponseMessage(
			String username,
			String seed,
			String password,
			String language,
			String database
	) throws MCLException {
		if (username == null || password == null || seed == null ||
				language == null || database == null) throw
			new MCLException("Parameters may not be null");
		
		sentences = new MCLSentence[4];

		try {
			// in the future SHA?
			MessageDigest md = MessageDigest.getInstance("MD5");

			md.update(password.getBytes("UTF-8"));
			byte[] digest = md.digest();
			// convert the bytes into a hex string
			StringBuffer buf = new StringBuffer(32);
			for (int i = 0; i < digest.length; i++) {
				String tmp = Integer.toHexString(digest[i]);
				if (tmp.length() == 1) buf.append('0');
				buf.append(tmp);
			}
			password = buf.toString();
			md.reset();
			md.update((password + seed).getBytes("UTF-8"));
			digest = md.digest();
			// convert the bytes into a hex string
			buf = new StringBuffer(32);
			for (int i = 0; i < digest.length; i++) {
				String tmp = Integer.toHexString(digest[i]);
				if (tmp.length() == 1) buf.append('0');
				buf.append(tmp);
			}
			password = buf.toString();
		} catch (UnsupportedEncodingException e) {
			throw new AssertionError("UTF-8 not supported?!?");
		} catch (NoSuchAlgorithmException e) {
			throw new AssertionError("Holy Moly!  No MD5 support!?!");
		}

		this.username = username;
		this.password = password;
		this.language = language;
		this.database = database;
		sentences[0] = new MCLSentence('$', "username", username);
		sentences[1] = new MCLSentence('$', "password", password);
		sentences[2] = new MCLSentence('$', "language", language);
		sentences[3] = new MCLSentence('$', "database", database);
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
	 * Returns the start of message sentence for this Message: &amp;R.
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


		if (prop.equals("username")) {
			username = value;
			sentences[0] = in;
		} else if (prop.equals("password")) {
			if (value.length() != 32) throw
				new MCLException("Illegal value for header 'password': " + value);

			password = value;
			sentences[1] = in;
		} else if (prop.equals("language")) {
			language = value;
			sentences[2] = in;
		} else if (prop.equals("database")) {
			database = value;
			sentences[3] = in;
		} else {
			throw new MCLException("Illegal property '" + prop + "' for this Message");
		}
	}


	// the following are message specific getters that retrieve the
	// values inside the message

	/**
	 * Retrieves username contained in this Message object.
	 *
	 * @return the username
	 */
	public String getUsername() {
		return(username);
	}

	/**
	 * Retrieves the password as md5 of the md5 of the password + seed
	 * contained in this Message object.
	 *
	 * @return the password
	 */
	public String getPassword() {
		return(password);
	}

	/**
	 * Retrieves the language contained in this Message object.
	 *
	 * @return the language
	 */
	public String getLanguage() {
		return(language);
	}

	/**
	 * Retrieves the database contained in this Message object.
	 *
	 * @return the database
	 */
	public String getDatabase() {
		return(database);
	}
}
