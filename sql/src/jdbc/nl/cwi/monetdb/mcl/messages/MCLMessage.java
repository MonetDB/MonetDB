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
import nl.cwi.monetdb.mcl.io.*;

/**
 * A Message object represents one of the messages present in the MCL
 * protocol.  A Message has some default functionality, which is defined
 * in this abstract class, and some required functionality which each
 * implementing class is required to fill in.  Since MCL is defined to
 * have a fixed number of different Messages, it is not possible to
 * dynamically extend the set of known Messages.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public abstract class MCLMessage {
	protected final static MCLSentence promptSentence;
	protected final static MCLSentence morePromptSentence;

	static {
		try {
			promptSentence =
				new MCLSentence(MCLSentence.PROMPT, "");
			morePromptSentence =
				new MCLSentence(MCLSentence.MOREPROMPT, "");
		} catch (MCLException e) {
			throw new AssertionError("Unable to create core sentences");
		}
	}

	protected MCLSentence[] sentences;
	
	/**
	 * Returns a String representation of the current contents of this
	 * Message object.  The String representation is equal to what would
	 * get sent over the wire when calling the method writeToStream()
	 * with the exception that each sentence is not prefixed with its
	 * length and ends with a newline.  If this Message is not valid,
	 * this method will return the string "Invalid Message".
	 *
	 * @return a String representation of this object
	 */
	public String toString() {
		Iterator it = getSentences();
		MCLSentence sentence;
		String ret = getName() + ":\n";

		ret += getSomSentence().toString();
		while (it.hasNext()) {
			sentence = (MCLSentence)(it.next());
			if (sentence == null)
				return("Invalid Message");

			ret += sentence.toString() + "\n";
		}
		ret += promptSentence.toString();
		
		return(ret);
	}

	/**
	 * Returns the name of this Message.  In practice the string
	 * returned will be equal to the class name of the implementing
	 * class.
	 *
	 * @return a String representing the name of the type of this
	 *         Message
	 */
	public String getName() {
		String name = this.getClass().getName();
		return(name.substring(name.lastIndexOf(".")));
	}

	/**
	 * Returns the type of this Message as one of the constants of the
	 * class MessageTypes.
	 * 
	 * @return an integer value that represents the type of this Message
	 */
	abstract public int getType();

	/**
	 * Returns a sentence that represents the start of message.  The
	 * returned sentence is the right sentence that identifies the
	 * implementing class' Message.
	 *
	 * @return the start of message sentence
	 */
	abstract public MCLSentence getSomSentence();


	/**
	 * Writes the current contents of this Message object to the given
	 * OutputStream.  Writing is done per sentence, after which a flush
	 * is performed.
	 *
	 * @param out the OutputStream to write to
	 * @throws MCLException if this Message is not valid, or writing to
	 * the stream failed
	 */
	public void writeToStream(MCLOutputStream out) throws MCLException {
		Iterator it = getSentences();
		MCLSentence sentence;

		out.writeSentence(getSomSentence());
		while (it.hasNext()) {
			sentence = (MCLSentence)(it.next());
			if (sentence == null) throw
				new MCLException("Invalid Message");

			out.writeSentence(sentence);
		}
		out.writeSentence(promptSentence);

		out.flush();
	}

	/**
	 * Returns an Iterator over all Sentences contained within this
	 * Message.  The StartOfMessage and Prompt Sentences are not
	 * included in the Iterator.
	 *
	 * @return an Iterator over all Sentences
	 */
	public Iterator getSentences() {
		return(new Iterator() {
			boolean hasNext = true;
			int pos = 0;

			/**
			 * Returns true if the iteration has more elements. (In
			 * other words, returns true if next would return an element
			 * rather than throwing an exception.)
			 *
			 * @return true if the iterator has more elements.
			 */
			public boolean hasNext() {
				return(hasNext);
			}

			/**
			 * Returns the next element in the iteration.
			 *
			 * @return the next element in the iteration.
			 * @throws NoSuchElementException iteration has no more
			 * elements.
			 */
			public Object next() throws NoSuchElementException {
				if (!hasNext) throw new NoSuchElementException();
				
				if (sentences.length == 0 || sentences.length - 1 == pos)
					hasNext = false;

				return(sentences[pos]);
			}

			/**
			 * Remove is not supported.
			 */
			public void remove() throws UnsupportedOperationException {
				throw new UnsupportedOperationException();
			}
		});
	}

	/**
	 * Adds the given String to this Message if it matches the Message
	 * type.  The sentence is parsed as far as that is considered to be
	 * necessary to validate it against the Message type.  If a sentence
	 * is not valid, an MCLException is thrown.
	 * 
	 * @param sentence an MCLSentence object
	 * @throws MCLException if the given sentence is not considered to
	 * be valid
	 */
	abstract public void addSentence(MCLSentence sentence) throws MCLException;


	/**
	 * Reads from the stream until the Message on that stream is fully
	 * read or an error occurs.  All sentences read are passed on to the
	 * addSentence() method which checks them for validity.
	 *
	 * @param in the InputStream to write from
	 * @return an MCLMessage as read from the stream
	 * @throws MCLException if the data read from the stream is invalid
	 * or reading from the stream failed
	 */
	public static MCLMessage readFromStream(MCLInputStream in)
		throws MCLException
	{
		// read till prompt and feed to addSentence implementation
		// function
		MCLSentence sentence = in.readSentence();

		// find out which message we need: synchronisation problem
		MCLMessage msg = new ChallengeMessage();
		
		while (sentence.getType() != MCLSentence.PROMPT) {
			msg.addSentence(sentence);
		}

		return(msg);
	}
}
