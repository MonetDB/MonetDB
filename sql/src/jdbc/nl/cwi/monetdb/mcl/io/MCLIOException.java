package nl.cwi.monetdb.mcl.io;

import nl.cwi.monetdb.mcl.*;

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
public class MCLIOException {
	public MCLIOException(String msg) {
		super(msg);
	}
}
