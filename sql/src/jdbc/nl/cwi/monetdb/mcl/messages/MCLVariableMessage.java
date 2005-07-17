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
abstract class MCLVariableMessage extends MCLMessage {
	protected List variableSentences;
	
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
		String ret = getName() + ":\n";
		ret += getSomSentence().toString();
		for (int i = 0; i < sentences.length; i++) {
			if (sentences[i] == null)
				return("Invalid Message");

			ret += sentences[i].toString() + "\n";
		}
		for (int i = 0; i < variableSentences.size(); i++) {
			if (sentences[i] == null)
				return("Invalid Message");

			ret += ((MCLMessage)variableSentences.get(i)).toString() + "\n";
		}
		ret += promptSentence.toString();
		
		return(ret);
	}

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
		out.writeSentence(getSomSentence());
		for (int i = 0; i < sentences.length; i++) {
			if (sentences[i] == null) throw
				new MCLException("Invalid Message");

			out.writeSentence(sentences[i]);
		}
		for (int i = 0; i < variableSentences.size(); i++) {
			if (sentences[i] == null) throw
				new MCLException("Invalid Message");

			out.writeSentence((MCLSentence)variableSentences.get(i));
		}
		out.writeSentence(promptSentence);

		out.flush();
	}
}
