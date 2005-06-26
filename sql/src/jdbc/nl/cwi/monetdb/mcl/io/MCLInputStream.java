package nl.cwi.monetdb.mcl.io;

import java.io.*;
import java.nio.*;
import nl.cwi.monetdb.mcl.*;
import nl.cwi.monetdb.mcl.messages.*;

/**
 * An MCLInputStream allows to conveniently read sentences of the
 * associated input stream.  MCL sentences are presented on the wire as
 * sequences of bytes prefixed with their length represented in an
 * 4-byte integer value.  It may be efficient to supply a
 * BufferedInputStream to the constructor of this class to increase
 * performance.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class MCLInputStream {
	private final InputStream in;
	private final ByteBuffer bbuf;

	/**
	 * Creates a MCLInputStream and saves its argument, the input stream
	 * in for later use.
	 *
	 * @param in the underlying input stream
	 */
	public MCLInputStream(InputStream in) {
		this.in = in;
		bbuf = ByteBuffer.allocate(4);	// an int
	}

	/**
	 * Returns the next sentence on the stream as a Sentence object.
	 * This method will block if no data is available.
	 *
	 * @return the sentence read
	 * @throws MCLException if an IO error occurs or the sentence seems
	 *         to be invalid
	 */
	public MCLSentence readSentence() throws MCLException {
		try {
			byte[] data = new byte[4];
			int size = in.read(data);
			if (size == -1) throw
				new MCLException("End of stream reached");
			if (size < 4) throw
				new MCLException("Illegal start of sentence");

			// put the four bytes in the ByteBuffer
			inputBuffer.rewind();
			inputBuffer.put(data);

			// get the int-value and store it in the state
			inputBuffer.rewind();
			size = inputBuffer.getInt();

			if (size < 0) throw
				new MCLException("Illegal sentence length: " + size);

			data = new byte[size - 1]; // minus the first byte (identifier)
			int type = in.read();
			if (type < 0) throw
				new MCLException("End of stream reached");
			if (in.read(data) != size) throw
				new MCLException("Unable to read complete sentence");

			return(new MCLSentence(type, data));
		} catch (IOException e) {
			throw new MCLException("IO operation failed: " + e.getMessage());
		}
	}

	/**
	 * Closes the input stream associated to this MCLInputStream.
	 */
	public void close() {
		try {
			in.close();
		} catch (IOException e) {
			// ignore
		}
	}

	/**
	 * Makes sure IO resources are released by closing this object.
	 */
	protected void finalize() {
		close();
	}
}
