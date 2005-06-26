package nl.cwi.monetdb.mcl.messages;

import java.util.*; // needed?

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
abstract class MCLMessage {
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
	public String toString();

	/**
	 * Returns the name of this Message.  In practice the string
	 * returned will be equal to the class name of the implementing
	 * class.
	 *
	 * @return a String representing the name of the type of this
	 *         Message
	 */
	public String getName();

	/**
	 * Returns the type of this Message as one of the constants of the
	 * class MessageTypes.
	 * 
	 * @return an integer value that represents the type of this Message
	 */
	public int getType();


	// candidates ServerOriginated
	/**
	 * Writes the current contents of this Message object to the given
	 * OutputStream.  Writing is done per sentence, where each sentence
	 * is prefixed with a network order integer indicating the length of
	 * the sentence.
	 *
	 * @param out the OutputStream to write to
	 * @throws MCLException if this Message is not valid, or writing to
	 * the stream failed
	 */
	public void writeToStream(MCLOutputStream out) throws MCLException;

	/**
	 * Adds the given String to this Message if it matches the Message
	 * type.  The sentence is parsed as far as that is considered to be
	 * necessary to validate it against the Message type.  If a sentence
	 * is not valid, an MCLException is thrown.
	 * 
	 * @param in a sentence as String without the leading length integer
	 * @throws MCLException if the given sentence is not considered to
	 * be valid
	 */
	public void addSentence(String in) throws MCLException;


	// not in interface: implementation code function
	/**
	 * Reads from the stream until the Message on that stream is fully
	 * read or an error occurs.  All sentences read are passed on to the
	 * addSentence() method which checks them for validity.
	 *
	 * @param in the InputStream to write from
	 * @throws MCLException if the data read from the stream is invalid
	 * or reading from the stream failed
	 */
	public static void readFromStream(MCLInputStream in) throws MCLException;
}
