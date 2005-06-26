package nl.cwi.monetdb.mcl.messages;

public class MCLSentence {
	private final int type;
	private final byte[] data;
	
	/** Indicates the end of a message (sequence). */
	public final int PROMPT          = '.';
	/** Indicates the end of a message (sequence), expecting more query
	 * input. */
	public final int MOREPROMPT      = ',';
	/** Indicates the start of a new message. */
	public final int STARTOFMESSAGE  = '&';
	/** Metadata for the MCL layer. */
	public final int MCLMETADATA     = '$';
	/** Indicates client/server roles are switched. */
	public final int SWITCHROLES     = '^';
	/** Metadata. */
	public final int METADATA        = '%';
	/** Response data. */
	public final int DATA            = '[';
	/** Query data. */
	public final int QUERY           = ']';
	/** Additional info, comments or messages. */
	public final int INFO            = '#';

	
	/**
	 * Constucts a new sentence with the given type and data elements.
	 * The sentence type needs to be one of the supported (and known)
	 * sentence types.
	 *
	 * @param type an int representing the type of this sentence
	 * @param data a byte array containing the sentence value
	 * @throws MCLException if the type is invalid, or the data is empty
	 */
	public MCLSentence(int type, byte[] data) throws MCLException {
		if (data == null) throw
			new MCLException("data may not be null");
		if (type != PROMPT &&
				type != MOREPROMPT &&
				type != STARTOFMESSAGE &&
				type != MCLMETADATA &&
				type != SWITCHROLES &&
				type != METADATA &&
				type != DATA &&
				type != QUERY &&
				type != INFO)
		{
			throw new MCLException("Unknown sentence type: " + (char)type + " (" + type + ")");
		}
		
		this.type = type;
		this.data = data;
	}

	/**
	 * Constructs a new sentence with the given type and string data
	 * value.
	 *
	 * @param type an int representing the type of this sentence
	 * @param data a String containing the sentence value
	 * @throws MCLException if the type is invalid, or the data is empty
	 */
	public MCLSentence(int type, String data) throws MCLException {
		this(type, data.getBytes("UTF-8"));
	}

	/**
	 * Convenience constuctor which constructs a new sentence from the
	 * given string, assuming the first character of the string is the
	 * sentence type.
	 *
	 * @param sentence a String representing a full sentence
	 * @throws MCLException if the type is invalid, or the data is empty
	 */
	public MCLSentence(String sentence) throws MCLException {
		this(sentence.charAt(0), sentence.substring(1));
	}

	/**
	 * Returns the type of this sentence as an integer value.  The
	 * integer values are one of the defined constants from this class.
	 *
	 * @return the type of this sentence as a constant value
	 */
	public int getType() {
		return(type);
	}

	/**
	 * Returns the value of this sentence as raw bytes.
	 *
	 * @return the raw bytes
	 */
	public byte[] getData() {
		return(data);
	}

	/**
	 * Returns a String representing the value of this sentence.  The
	 * raw byte data is converted properly to a String value.
	 *
	 * @return the value of this sentence as String
	 */
	public String getString() {
		return(new String(data, "UTF-8"));
	}

	/**
	 * Returns a String representing this complete sentence.  The string
	 * is constructed by appending the value to the type.
	 *
	 * @return a String describing this MCLSentence
	 */
	public String toString() {
		return("" + (char)type + getString());
	}
}
