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
	 *
	 * @param type a byte representing the type of this sentence
	 * @param data a byte array containing the sentence value
	 */
	public MCLSentence(int type, byte[] data) {
		this.type = type;
		this.data = data;
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
		return(new String(data));
	}
}
