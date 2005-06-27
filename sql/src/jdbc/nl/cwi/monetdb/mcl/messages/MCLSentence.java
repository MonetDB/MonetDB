package nl.cwi.monetdb.mcl.messages;

import java.io.*;
import nl.cwi.monetdb.mcl.*;

public class MCLSentence {
	private final int type;
	private final byte[] data;

	/** Indicates the end of a message (sequence). */
	public final static int PROMPT          = '.';
	/** Indicates the end of a message (sequence), expecting more query
	 * input. */
	public final static int MOREPROMPT      = ',';
	/** Indicates the start of a new message. */
	public final static int STARTOFMESSAGE  = '&';
	/** Metadata for the MCL layer. */
	public final static int MCLMETADATA     = '$';
	/** Indicates client/server roles are switched. */
	public final static int SWITCHROLES     = '^';
	/** Metadata. */
	public final static int METADATA        = '%';
	/** Response data. */
	public final static int DATA            = '[';
	/** Query data. */
	public final static int QUERY           = ']';
	/** Additional info, comments or messages. */
	public final static int INFO            = '#';


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

		if (!isValidType(type)) throw
			new MCLException("Unknown sentence type: " + (char)type + " (" + type + ")");

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
		if (data == null) throw
			new MCLException("data may not be null");

		if (!isValidType(type)) throw
			new MCLException("Unknown sentence type: " + (char)type + " (" + type + ")");

		this.type = type;
		try {
			this.data = data.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// this should not happen actually
			throw new AssertionError("UTF-8 encoding not supported!");
		}
	}

	/**
	 * Convenience constructor which takes a type, a property and a
	 * value and constructs a Sentence of type with property and value.
	 *
	 * @param type an int representing the type of this sentence
	 * @param property a property name
	 * @param value the property value
	 */
	public MCLSentence(int type, String property, String value) throws MCLException {
		if (property == null || value == null) throw
			new MCLException("property or value may not be null");

		if (!isValidType(type)) throw
			new MCLException("Unknown sentence type: " + (char)type + " (" + type + ")");

		this.type = type;
		try {
			this.data = (property + "\t" + value).getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// this should not happen actually
			throw new AssertionError("UTF-8 encoding not supported!");
		}
	}

	/**
	 * Convenience constructor which takes a type, a property and a list
	 * of values and constructs a Sentence of type with property and
	 * values.
	 *
	 * @param type an int representing the type of this sentence
	 * @param property a property name
	 * @param value the property values
	 */
	public MCLSentence(int type, String property, String[] values) throws MCLException {
		if (property == null || values == null) throw
			new MCLException("property or values may not be null");
		if (values.length == 0) throw
			new MCLException("values should contain at least one value");

		if (!isValidType(type)) throw
			new MCLException("Unknown sentence type: " + (char)type + " (" + type + ")");

		this.type = type;
		try {
			String tmp = property;
			for (int i = 0; i < values.length; i++)
				tmp += "\t" + values[i];
			this.data = tmp.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// this should not happen actually
			throw new AssertionError("UTF-8 encoding not supported!");
		}
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
		try {
			return(new String(data, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// this should not happen
			throw new AssertionError("UTF-8 encoding not supported!");
		}
	}

	/**
	 * Extracts the xth field from the sentence where fields are
	 * separated by tab characters.  This method only works correctly
	 * when dealing with sentences which have no values containing tab
	 * characters.  No interpretation of the sentence is performed.
	 *
	 * @param field the field to extract starting from 1
	 * @return the extracted field or null if no such field exists
	 */
	public String getField(int field) {
		if (field < 1) return(null);
		String[] fields = getString().split("\t");
		if (field > fields.length) return(null);
		return(fields[field - 1]);
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


	/**
	 * Utility function which checks whether a given type is valid.
	 * Returns true if the given type is valid and supported.
	 *
	 * @param type the type to check
	 * @return true if the type is valid, false otherwise
	 */
	private static boolean isValidType(int type) {
		if (
				type != PROMPT &&
				type != MOREPROMPT &&
				type != STARTOFMESSAGE &&
				type != MCLMETADATA &&
				type != SWITCHROLES &&
				type != METADATA &&
				type != DATA &&
				type != QUERY &&
				type != INFO
		) {
			return(false);
		} else {
			return(true);
		}
	}
}
