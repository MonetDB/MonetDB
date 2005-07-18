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


	// the following are message specific getters that retrieve the
	// values inside the message

}
