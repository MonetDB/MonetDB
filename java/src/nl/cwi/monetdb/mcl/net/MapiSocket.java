/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.mcl.net;

import java.io.*;
import java.nio.*;
import java.net.*;
import java.util.*;
import java.sql.*;
import java.security.*;
import nl.cwi.monetdb.mcl.*;
import nl.cwi.monetdb.mcl.io.*;
import nl.cwi.monetdb.mcl.parser.*;

/**
 * A Socket for communicating with the MonetDB database in MAPI block
 * mode.
 * <br /><br />
 * The MapiSocket implements the protocol specifics of the MAPI block
 * mode protocol, and interfaces it as a socket that delivers a
 * BufferedReader and a BufferedWriter.  Because logging in is an
 * integral part of the MAPI protocol, the MapiSocket performs the login
 * procedure.  Like the Socket class, various options can be set before
 * calling the connect() method to influence the login process.  Only
 * after a successful call to connect() the BufferedReader and
 * BufferedWriter can be retrieved.
 * <br />
 * For each line read, it is determined what type of line it is
 * according to the MonetDB MAPI protocol.  This results in a line to be
 * PROMPT, HEADER, RESULT, ERROR or UNKNOWN.  Use the getLineType()
 * method on the BufferedMCLReader to retrieve the type of the last
 * line read.
 * <br /><br />
 * For debugging purposes a socket level debugging is implemented where
 * each and every interaction to and from the MonetDB server is logged
 * to a file on disk.<br />
 * Incoming messages are prefixed by "RX" (received by the driver),
 * outgoing messages by "TX" (transmitted by the driver).  Special
 * decoded non-human readable messages are prefixed with "RD" and "TD"
 * instead.  Following this two char prefix, a timestamp follows as the
 * number of milliseconds since the UNIX epoch.  The rest of the line is
 * a String representation of the data sent or received.
 * <br /><br />
 * The general use of this Socket must be seen only in the full context
 * of a MAPI connection to a server.  It has the same ingredients as a
 * normal Socket, allowing for seamless plugging.
 * <pre>
 *    Socket   \     /  InputStream  ----&gt; (BufferedMCL)Reader
 *              &gt; o &lt;
 *  MapiSocket /     \ OutputStream  ----&gt; (BufferedMCL)Writer
 * </pre>
 * The MapiSocket allows to retrieve Streams for communicating.  They
 * are interfaced, so they can be chained in any way.  While the Socket
 * transparently deals with how data is sent over the wire, the actual
 * data read needs to be interpreted, for which a Reader/Writer
 * interface is most sufficient.  In particular the BufferedMCL*
 * implementations of those interfaces supply some extra functionality
 * geared towards the format of the data.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 4.1
 * @see nl.cwi.monetdb.mcl.io.BufferedMCLReader
 * @see nl.cwi.monetdb.mcl.io.BufferedMCLWriter
 */
public final class MapiSocket {
	/** The TCP Socket to mserver */
	private Socket con;
	/** Stream from the Socket for reading */
	private InputStream fromMonet;
	/** Stream from the Socket for writing */
	private OutputStream toMonet;
	/** MCLReader on the InputStream */
	private BufferedMCLReader reader;
	/** MCLWriter on the OutputStream */
	private BufferedMCLWriter writer;
	/** protocol version of the connection */
	private int version;

	/** The database to connect to */
	private String database = null;
	/** The language to connect with */
	private String language = "sql";
	/** The hash methods to use (null = default) */
	private String hash = null;
	/** Whether we should follow redirects */
	private boolean followRedirects = true;
	/** How many redirections do we follow until we're fed up with it? */
	private int ttl = 10;
	/** Whether we are debugging or not */
	private boolean debug = false;
	/** The Writer for the debug log-file */
	private FileWriter log;

	/** The blocksize (hardcoded in compliance with stream.mx) */
	public final static int BLOCK = 8 * 1024 - 2;

	/** A buffer which holds the blocks read */
	private StringBuffer readBuffer;
	/** The number of available bytes to read */
	private short readState = 0;

	/** A short in two bytes for holding the block size in bytes */
	private byte[] blklen = new byte[2];

	/** SO_TIMEOUT value for the underlying java.net.Socket */
	private int sockTimeout = 0;

	/**
	 * Constructs a new MapiSocket.
	 */
	public MapiSocket() {
		readBuffer = new StringBuffer();
		con = null;
	}

	/**
	 * Sets the database to connect to.  If database is null, a
	 * connection is made to the default database of the server.  This
	 * is also the default.
	 *
	 * @param db the database
	 */
	public void setDatabase(String db) {
		this.database = db;
	}

	/**
	 * Sets the language to use for this connection.
	 *
	 * @param lang the language
	 */
	public void setLanguage(String lang) {
		this.language = lang;
	}

	/**
	 * Sets the hash method to use.  Note that this method is intended
	 * for debugging purposes.  Setting a hash method can yield in
	 * connection failures.  Multiple hash methods can be given by
	 * separating the hashes by commas.
	 * DON'T USE THIS METHOD if you don't know what you're doing.
	 *
	 * @param hash the hash method to use
	 */
	public void setHash(String hash) {
		this.hash = hash;
	}

	/**
	 * Sets whether MCL redirections should be followed or not.  If set
	 * to false, an MCLException will be thrown when a redirect is
	 * encountered during connect.  The default bahaviour is to
	 * automatically follow redirects.
	 *
	 * @param r whether to follow redirects (true) or not (false)
	 */
	public void setFollowRedirects(boolean r) {
		this.followRedirects = r;
	}

	/**
	 * Sets the number of redirects that are followed when
	 * followRedirects is true.  In order to avoid going into an endless
	 * loop due to some evil server, or another error, a maximum number
	 * of redirects that may be followed can be set here.  Note that to
	 * disable the following of redirects you should use
	 * setFollowRedirects.
	 *
	 * @see #setFollowRedirects(boolean r)
	 * @param t the number of redirects before an exception is thrown
	 */
	public void setTTL(int t) {
		this.ttl = t;
	}

	/**
	 * Set the SO_TIMEOUT on the underlying Socket.  When for some
	 * reason the connection to the database hangs, this setting can be
	 * useful to break out of this indefinite wait.
	 * This option must be enabled prior to entering the blocking
	 * operation to have effect.
	 *
	 * @param timeout The specified timeout, in milliseconds.  A timeout
	 *        of zero is interpreted as an infinite timeout.
	 */
	public void setSoTimeout(int s) {
		this.sockTimeout = s;
	}

	/**
	 * Connects to the given host and port, logging in as the given
	 * user.  If followRedirect is false, a RedirectionException is
	 * thrown when a redirect is encountered.
	 *
	 * @param host the hostname, or null for the loopback address
	 * @param port the port number
	 * @param user the username
	 * @param pass the password
	 * @return A List with informational (warning) messages, or null
	 *         if there aren't any
	 * @throws IOException if an I/O error occurs when creating the
	 *         socket
	 * @throws MCLParseException if bogus data is received
	 * @throws MCLException if an MCL related error occurs
	 */
	public List connect(String host, int port, String user, String pass)
		throws IOException, MCLParseException, MCLException
	{
		// Wrap around the internal connect that needs to know if it
		// should really make a TCP connection or not.
		return(connect(host, port, user, pass, true));
	}

	private List connect(String host, int port, String user, String pass,
			boolean makeConnection)
		throws IOException, MCLParseException, MCLException
	{
		if (ttl-- <= 0)
			throw new MCLException("Maximum number of redirects reached, aborting connection attempt.  Sorry.");

		if (makeConnection) {
			con = new Socket(host, port);
			// set nodelay, as it greatly speeds up small messages (like we
			// often do)
			con.setTcpNoDelay(true);
			// limit time to wait on blocking operations (0 = indefinite)
			con.setSoTimeout(sockTimeout);

			fromMonet = new BlockInputStream(con.getInputStream());
			toMonet = new BlockOutputStream(con.getOutputStream());
			try {
				reader = new BufferedMCLReader(fromMonet, "UTF-8");
				writer = new BufferedMCLWriter(toMonet, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new AssertionError(e.toString());
			}
			writer.registerReader(reader);
		}

		// do the login ritual, read challenge, send response
		String c = reader.readLine();
		reader.waitForPrompt();
		writer.writeLine(
				getChallengeResponse(
					c,
					user,
					pass,
					language,
					database,
					hash
					)
				);

		// read monet response till prompt
		List redirects = null;
		ArrayList warns = new ArrayList();
		String err = "", tmp;
		int lineType;
		do {
			if ((tmp = reader.readLine()) == null)
				throw new IOException("Read from " +
						con.getInetAddress().getHostName() + ":" +
						con.getPort() + ": End of stream reached");
			if ((lineType = reader.getLineType()) == BufferedMCLReader.ERROR) {
				err += "\n" + tmp.substring(1);
			} else if (lineType == BufferedMCLReader.INFO) {
				warns.add(tmp.substring(1));
			} else if (lineType == BufferedMCLReader.REDIRECT) {
				if (redirects == null)
					redirects = new ArrayList();
				redirects.add(tmp.substring(1));
			}
		} while (lineType != BufferedMCLReader.PROMPT);
		if (err != "") {
			close();
			throw new MCLException(err.trim());
		}
		if (redirects != null) {
			if (followRedirects) {
				// Ok, server wants us to go somewhere else.  The list
				// might have multiple clues on where to go.  For now we
				// don't support anything intelligent but trying the
				// first one.  URI should be in form of:
				// "mapi:monetdb://host:port/database?arg=value&..."
				// or
				// "mapi:merovingian://proxy?arg=value&..."
				// note that the extra arguments must be obeyed in both
				// cases
				String suri = redirects.get(0).toString();
				if (!suri.startsWith("mapi:"))
					throw new MCLException("unsupported redirect: " + suri);

				URI u;
				try {
					u = new URI(suri.substring(5));
				} catch (URISyntaxException e) {
					throw new MCLParseException(e.toString());
				}

				tmp = u.getQuery();
				if (tmp != null) {
					String args[] = tmp.split("&");
					for (int i = 0; i < args.length; i++) {
						int pos = args[i].indexOf("=");
						if (pos > 0) {
							tmp = args[i].substring(0, pos);
							if (tmp.equals("database")) {
								tmp = args[i].substring(pos + 1);
								if (!tmp.equals(database)) {
									warns.add("redirect points to different " +
											"database: " + tmp);
									setDatabase(tmp);
								}
							} else if (tmp.equals("language")) {
								tmp = args[i].substring(pos + 1);
								warns.add("redirect specifies use of different language: " + tmp);
								setLanguage(tmp);
							} else if (tmp.equals("user")) {
								tmp = args[i].substring(pos + 1);
								if (!tmp.equals(user))
									warns.add("ignoring different username '" + tmp + "' set by " +
											"redirect, what are the security implications?");
							} else if (tmp.equals("password")) {
								warns.add("ignoring different password set by redirect, " +
										"what are the security implications?");
							} else {
								warns.add("ignoring unknown argument '" + tmp + "' from redirect");
							}
						} else {
							warns.add("ignoring illegal argument from redirect: " +
									args[i]);
						}
					}
				}

				List w = null;
				if (u.getScheme().equals("monetdb")) {
					// this is a redirect to another (monetdb) server,
					// which means a full reconnect
					// avoid the debug log being closed
					if (debug) {
						debug = false;
						close();
						debug = true;
					} else {
						close();
					}
					tmp = u.getPath();
					if (tmp != null && tmp.length() != 0) {
						tmp = tmp.substring(1).trim();
						if (!tmp.equals("")) {
							if (!tmp.equals(database)) {
								warns.add("redirect points to different " +
										"database: " + tmp);
								setDatabase(tmp);
							}
						}
					}
					int p = u.getPort();
					w = connect(u.getHost(), p == -1 ? port : p,
							user, pass, true);
					warns.add("Redirect by " + host + ":" + port + " to " + suri);
				} else if (u.getScheme().equals("merovingian")) {
					// reuse this connection to inline connect to the
					// right database that Merovingian proxies for us
					w = connect(host, port, user, pass, false);
				} else {
					throw new MCLException("unsupported scheme in redirect: " + suri);
				}
				if (w != null)
					warns.addAll(w);
			} else {
				String msg = "The server sent a redirect for this connection:";
				for (Iterator it = redirects.iterator(); it.hasNext(); ) {
					msg += " [" + it.next().toString() + "]";
				}
				throw new MCLException(msg);
			}
		}

		return(warns.size() == 0 ? null : warns);
	}

	/**
	 * A little helper function that processes a challenge string, and
	 * returns a response string for the server.  If the challenge
	 * string is null, a challengeless response is returned.
	 *
	 * @param chalstr the challenge string
	 * @param username the username to use
	 * @param password the password to use
	 * @param language the language to use
	 * @param database the database to connect to
	 * @param hash the hash method(s) to use, or NULL for all supported
	 *             hashes
	 */
	private String getChallengeResponse(
			String chalstr,
			String username,
			String password,
			String language,
			String database,
			String hash
	) throws MCLParseException, MCLException, IOException {
		String response;
		String algo;

		// parse the challenge string, split it on ':'
		String[] chaltok = chalstr.split(":");
		if (chaltok.length <= 4) throw
			new MCLParseException("Server challenge string unusable!  Challenge contains too few tokens: " + chalstr);

		// challenge string to use as salt/key
		String challenge = chaltok[0];
		String servert = chaltok[1];
		try {
			version = Integer.parseInt(chaltok[2].trim());	// protocol version
		} catch (NumberFormatException e) {
			throw new MCLParseException("Protocol version unparseable: " + chaltok[3]);
		}

		// handle the challenge according to the version it is
		switch (version) {
			default:
				throw new MCLException("Unsupported protocol version: " + version);
			case 9:
				// proto 9 is like 8, but uses a hash instead of the
				// plain password, the server tells us which hash in the
				// challenge after the byteorder

				/* NOTE: Java doesn't support RIPEMD160 :( */
				if (chaltok[5].equals("SHA512")) {
					algo = "SHA-512";
				} else if (chaltok[5].equals("SHA384")) {
					algo = "SHA-384";
				} else if (chaltok[5].equals("SHA256")) {
					algo = "SHA-256";
				/* NOTE: Java doesn't support SHA-224 */
				} else if (chaltok[5].equals("SHA1")) {
					algo = "SHA-1";
				} else if (chaltok[5].equals("MD5")) {
					algo = "MD5";
				} else {
					throw new MCLException("Unsupported password hash: " +
							chaltok[5]);
				}

				try {
					MessageDigest md = MessageDigest.getInstance(algo);
					md.update(password.getBytes("UTF-8"));
					byte[] digest = md.digest();
					password = toHex(digest);
				} catch (NoSuchAlgorithmException e) {
					throw new AssertionError("internal error: " + e.toString());
				} catch (UnsupportedEncodingException e) {
					throw new AssertionError("internal error: " + e.toString());
				}
			case 8:
				// proto 7 (finally) used the challenge and works with a
				// password hash.  The supported implementations come
				// from the server challenge.  We chose the best hash
				// we can find, in the order SHA1, MD5, plain.  Also,
				// the byte-order is reported in the challenge string,
				// which makes sense, since only blockmode is supported.
				// proto 8 made this obsolete, but retained the
				// byteorder report for future "binary" transports.  In
				// proto 8, the byteorder of the blocks is always little
				// endian because most machines today are.
				String hashes = (hash == null ? chaltok[3] : hash);
				// if we deal with merovingian, mask our credentials
				if (servert.equals("merovingian")) {
					username = "merovingian";
					password = "merovingian";
				}
				String pwhash;
				algo = null;
				if (hashes.indexOf("SHA512") != -1) {
					algo = "SHA-512";
					pwhash = "{SHA512}";
				} else if (hashes.indexOf("SHA384") != -1) {
					algo = "SHA-384";
					pwhash = "{SHA384}";
				} else if (hashes.indexOf("SHA256") != -1) {
					algo = "SHA-256";
					pwhash = "{SHA256}";
				} else if (hashes.indexOf("SHA1") != -1) {
					algo = "SHA-1";
					pwhash = "{SHA1}";
				} else if (hashes.indexOf("MD5") != -1) {
					algo = "MD5";
					pwhash = "{MD5}";
				} else if (version == 8 && hashes.indexOf("plain") != -1) {
					pwhash = "{plain}" + password + challenge;
				} else {
					throw new MCLException("no supported password hashes in " + hashes);
				}
				if (algo != null) {
					try {
						MessageDigest md = MessageDigest.getInstance(algo);
						md.update(password.getBytes("UTF-8"));
						md.update(challenge.getBytes("UTF-8"));
						byte[] digest = md.digest();
						pwhash += toHex(digest);
					} catch (NoSuchAlgorithmException e) {
						throw new AssertionError("internal error: " + e.toString());
					} catch (UnsupportedEncodingException e) {
						throw new AssertionError("internal error: " + e.toString());
					}
				}
				// TODO: some day when we need this, we should store
				// this
				if (chaltok[4].equals("BIG")) {
					// byte-order of server is big-endian
				} else if (chaltok[4].equals("LIT")) {
					// byte-order of server is little-endian
				} else {
					throw new MCLParseException("Invalid byte-order: " + chaltok[5]);
				}

				// generate response
				response = "BIG:";	// JVM byte-order is big-endian
				response += username + ":" + pwhash + ":" + language;
				response += ":" + (database == null ? "" : database) + ":";

				return(response);
		}
	}

	/**
	 * Small helper method to convert a byte string to a hexadecimal
	 * string representation.
	 *
	 * @param digest the byte array to convert
	 * @return the byte array as hexadecimal string
	 */
	private static String toHex(byte[] digest) {
		StringBuffer r = new StringBuffer(digest.length * 2);
		for (int i = 0; i < digest.length; i++) {
			// zero out higher bits to get unsigned conversion
			int b = digest[i] << 24 >>> 24;
			if (b < 16) r.append("0");
			r.append(Integer.toHexString(b));
		}
		return(r.toString());
	}

	/**
	 * Returns an InputStream that reads from this open connection on
	 * the MapiSocket.
	 *
	 * @return an input stream that reads from this open connection
	 */
	public InputStream getInputStream() {
		return(fromMonet);
	}

	/**
	 * Returns an output stream for this MapiSocket.
	 *
	 * @return an output stream for writing bytes to this MapiSocket
	 */
	public OutputStream getOutputStream() {
		return(toMonet);
	}

	/**
	 * Returns a Reader for this MapiSocket.  The Reader is a
	 * BufferedMCLReader which does protocol interpretation of the
	 * BlockInputStream produced by this MapiSocket.
	 *
	 * @return a BufferedMCLReader connected to this MapiSocket
	 */
	public BufferedMCLReader getReader() {
		return(reader);
	}

	/**
	 * Returns a Writer for this MapiSocket.  The Writer is a
	 * BufferedMCLWriter which produces protocol compatible data blocks
	 * that the BlockOutputStream can properly translate into blocks.
	 *
	 * @return a BufferedMCLWriter connected to this MapiSocket
	 */
	public BufferedMCLWriter getWriter() {
		return(writer);
	}

	/**
	 * Returns the mapi protocol version used by this socket.  The
	 * protocol version depends on the server being used.  Users of the
	 * MapiSocket should check this version to act appropriately.
	 *
	 * @return the mapi protocol version
	 */
	public int getProtocolVersion() {
		return(version);
	}

	/**
	 * Enables logging to a file what is read and written from and to
	 * the server.  Logging can be enabled at any time.  However, it is
	 * encouraged to start debugging before actually connecting the
	 * socket.
	 *
	 * @param filename the name of the file to write to
	 * @throws IOException if the file could not be opened for writing
	 */
	public void debug(String filename) throws IOException {
		log = new FileWriter(filename);
		debug = true;
	}

	/**
	 * Inner class that is used to write data on a normal stream as a
	 * blocked stream.  A call to the flush() method will write a
	 * "final" block to the underlying stream.  Non-final blocks are
	 * written as soon as one or more bytes would not fit in the
	 * current block any more.  This allows to write to a block to it's
	 * full size, and then flush it explicitly to have a final block
	 * being written to the stream.
	 */
	class BlockOutputStream extends FilterOutputStream {
		private int writePos = 0;
		private byte[] block = new byte[BLOCK];
		private int blocksize = 0;

		/**
		 * Constructs this BlockOutputStream, backed by the given
		 * OutputStream.  A BufferedOutputStream is internally used.
		 */
		public BlockOutputStream(OutputStream out) {
			// always use a buffered stream, even though we know how
			// much bytes to write/read, since this is just faster for
			// some reason
			super(new BufferedOutputStream(out));
		}

		public void flush() throws IOException {
			// write the block (as final) then flush.
			writeBlock(true);
			out.flush();

			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			// it's a bit nasty if an exception is thrown from the log,
			// but ignoring it can be nasty as well, so it is decided to
			// let it go so there is feedback about something going wrong
			if (debug) {
				log.flush();
			}
		}

		/**
		 * writeBlock puts the data in the block on the stream.  The
		 * boolean last controls whether the block is sent with an
		 * indicator to note it is the last block of a sequence or not.
		 *
		 * @param last whether this is the last block
		 * @throws IOException if writing to the stream failed
		 */
		public void writeBlock(boolean last) throws IOException {
			if (last) {
				// always fits, because of BLOCK's size
				blocksize = (short)writePos;
				// this is the last block, so encode least
				// significant bit in the first byte (little-endian)
				blklen[0] = (byte)(blocksize << 1 & 0xFF | 1);
				blklen[1] = (byte)(blocksize >> 7);
			} else {
				// always fits, because of BLOCK's size
				blocksize = (short)BLOCK;
				// another block will follow, encode least
				// significant bit in the first byte (little-endian)
				blklen[0] = (byte)(blocksize << 1 & 0xFF);
				blklen[1] = (byte)(blocksize >> 7);
			}

			out.write(blklen);

			// write the actual block
			out.write(block, 0, writePos);

			if (debug) {
				if (last) {
					logTd("write final block: " + writePos + " bytes");
				} else {
					logTd("write block: " + writePos + " bytes");
				}
				logTx(new String(block, 0, writePos, "UTF-8"));
			}

			writePos = 0;
		}

		public void write(int b) throws IOException {
			if (writePos == BLOCK) {
				writeBlock(false);
			}
			block[writePos++] = (byte)b;
		}

		public void write(byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		public void write(byte[] b, int off, int len) throws IOException {
			int t = 0;
			while (len > 0) {
				t = BLOCK - writePos;
				if (len > t) {
					System.arraycopy(b, off, block, writePos, t);
					off += t;
					len -= t;
					writePos += t;
					writeBlock(false);
				} else {
					System.arraycopy(b, off, block, writePos, len);
					writePos += len;
					break;
				}
			}
		}

		public void close() throws IOException {
			// we don't want the flush() method to be called (default of
			// the FilterOutputStream), so we close manually here
			out.close();
		}
	}


	/**
	 * Inner class that is used to make the data on the blocked stream
	 * available as a normal stream.
	 */
	class BlockInputStream extends FilterInputStream {
		private int readPos = 0;
		private int blockLen = 0;
		private byte[] block = new byte[BLOCK + 3]; // \n.\n

		/**
		 * Constructs this BlockInputStream, backed by the given
		 * InputStream.  A BufferedInputStream is internally used.
		 */
		public BlockInputStream(InputStream in) {
			// always use a buffered stream, even though we know how
			// much bytes to write/read, since this is just faster for
			// some reason
			super(new BufferedInputStream(in));
		}

		public int available() {
			return(blockLen - readPos);
		}

		public boolean markSupported() {
			return(false);
		}

		public void mark(int readlimit) {
			throw new AssertionError("Not implemented!");
		}

		public void reset() {
			throw new AssertionError("Not implemented!");
		}

		/**
		 * Small wrapper to get a blocking variant of the read() method
		 * on the BufferedInputStream.  We want to benefit from the
		 * Buffered pre-fetching, but not dealing with half blocks.
		 * Changing this class to be able to use the partially received
		 * data will greatly complicate matters, while an performance
		 * improvement is debatable given the relatively small size of
		 * our blocks.  Maybe it does speed up on slower links, then
		 * consider this method a quick bug fix/workaround.
		 */
		private boolean _read(byte[] b, int len) throws IOException {
			int s;
			int off = 0;

			while (len > 0) {
				s = in.read(b, off, len);
				if (s == -1) {
					// if we have read something before, we should have been
					// able to read the whole, so make this fatal
					if (off > 0) {
						if (debug) {
							logRd("the following incomplete block was received:");
							logRx(new String(b, 0, off, "UTF-8"));
						}
						throw new IOException("Read from " +
								con.getInetAddress().getHostName() + ":" +
								con.getPort() + ": Incomplete block read from stream");
					}
					return(false);
				}
				len -= s;
				off += s;
			}

			return(true);
		}

		/**
		 * Reads the next block on the stream into the internal buffer,
		 * or writes the prompt in the buffer.
		 *
		 * The blocked stream protocol consists of first a two byte
		 * integer indicating the length of the block, then the
		 * block, followed by another length + block.  The end of
		 * such sequence is put in the last bit of the length, and
		 * hence this length should be shifted to the right to
		 * obtain the real length value first.  We simply fetch
		 * blocks here as soon as they are needed for the stream's
		 * read methods.
		 *
		 * The user-flush, which is an implicit effect of the end of
		 * a block sequence, is communicated beyond the stream by
		 * inserting a prompt sequence on the stream after the last
		 * block.  This method makes sure that a final block ends with a
		 * newline, if it doesn't already, in order to facilitate a
		 * Reader that is possibly chained to this InputStream.
		 *
		 * If the stream is not positioned correctly, hell will break
		 * loose.
		 */
		private void readBlock() throws IOException {
			// read next two bytes (short)
			if (!_read(blklen, 2)) throw
				new IOException("Read from " +
						con.getInetAddress().getHostName() + ":" +
						con.getPort() + ": End of stream reached");

			// Get the short-value and store its value in blockLen.
			blockLen = (short)(
					(blklen[0] & 0xFF) >> 1 |
					(blklen[1] & 0xFF) << 7
					);
			readPos = 0;

			if (debug) {
				if ((blklen[0] & 0x1) == 1) {
					logRd("read final block: " + blockLen + " bytes");
				} else {
					logRd("read new block: " + blockLen + " bytes");
				}
			}

			// sanity check to avoid bad servers make us do an ugly
			// stack trace
			if (blockLen > block.length)
				throw new AssertionError("Server sent a block " +
						"larger than BLOCKsize: " +
						blockLen + " > " + block.length);
			if (!_read(block, blockLen))
				new IOException("Read from " +
						con.getInetAddress().getHostName() + ":" +
						con.getPort() + ": End of stream reached");

			if (debug) {
				logRx(new String(block, 0, blockLen, "UTF-8"));
			}

			// if this is the last block, make it end with a newline and
			// prompt
			if ((blklen[0] & 0x1) == 1) {
				if (blockLen > 0 && block[blockLen - 1] != '\n') {
					// to terminate the block in a Reader
					block[blockLen++] = '\n';
				}
				// insert 'fake' flush
				block[blockLen++] = BufferedMCLReader.PROMPT;
				block[blockLen++] = '\n';
				if (debug)
					logRd("inserting prompt");
			}
		}

		public int read() throws IOException {
			if (available() == 0)
				readBlock();
			if (debug)
				logRx(new String(block, readPos, 1, "UTF-8"));
			return((int)block[readPos++]);
		}

		public int read(byte[] b) throws IOException {
			return(read(b, 0, b.length));
		}

		public int read(byte[] b, int off, int len) throws IOException {
			int t = available();
			boolean hasAvailable = t + super.available() > 0;
			int size = 0;
			while (size < len) {
				if (t == 0) {
					if (hasAvailable || size == 0) {
						// shortcut some instructions, but make sure we
						// always read *something* (block) for a read
						// call, unless size == 0
						readBlock();
						t = available();
					} else {
						// nothing here, nothing waiting return what we
						// have
						break;
					}
				}
				if (len > t) {
					System.arraycopy(block, readPos, b, off, t);
					off += t;
					len -= t;
					readPos += t;
					size += t;
				} else {
					System.arraycopy(block, readPos, b, off, len);
					readPos += len;
					size += len;
					break;
				}
				t = available();
				hasAvailable = t + super.available() > 0;
			}
			return(size);
		}

		public long skip(long n) throws IOException {
			long skip = n;
			int t = 0;
			while (skip > 0) {
				t = available();
				if (skip > t) {
					skip -= t;
					readPos += t;
					readBlock();
				} else {
					readPos += skip;
					break;
				}
			}
			return(n);
		}
	}

	/**
	 * Closes the streams and socket connected to the server if
	 * possible.  If an error occurs during disconnecting it is ignored.
	 */
	public synchronized void close() {
		try {
			if (reader != null) reader.close();
			if (writer != null) writer.close();
			if (fromMonet != null) fromMonet.close();
			if (toMonet != null) toMonet.close();
			if (con != null) con.close();
			if (debug) log.close();
		} catch (IOException e) {
			// ignore it
		}
	}

	/**
	 * Destructor called by garbage collector before destroying this
	 * object tries to disconnect the MonetDB connection if it has not
	 * been disconnected already.
	 */
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}


	/**
	 * Writes a logline tagged with a timestamp using the given string.
	 * Used for debugging purposes only and represents a message that is
	 * connected to writing to the socket.  A logline might look like:
	 * TX 152545124: Hello MonetDB!
	 *
	 * @param message the message to log
	 * @throws IOException if an IO error occurs while writing to the logfile
	 */
	private void logTx(String message) throws IOException {
		log.write("TX " + System.currentTimeMillis() +
			": " + message + "\n");
	}

	/**
	 * Writes a logline tagged with a timestamp using the given string.
	 * Lines written using this log method are tagged as "added
	 * metadata" which is not strictly part of the data sent.
	 *
	 * @param message the message to log
	 * @throws IOException if an IO error occurs while writing to the logfile
	 */
	private void logTd(String message) throws IOException {
		log.write("TD " + System.currentTimeMillis() +
			": " + message + "\n");
	}

	/**
	 * Writes a logline tagged with a timestamp using the given string,
	 * and flushes afterwards.  Used for debugging purposes only and
	 * represents a message that is connected to reading from the
	 * socket.  The log is flushed after writing the line.  A logline
	 * might look like:
	 * RX 152545124: Hi JDBC!
	 *
	 * @param message the message to log
	 * @throws IOException if an IO error occurs while writing to the logfile
	 */
	private void logRx(String message) throws IOException {
		log.write("RX " + System.currentTimeMillis() +
			": " + message + "\n");
		log.flush();
	}

	/**
	 * Writes a logline tagged with a timestamp using the given string,
	 * and flushes afterwards.  Lines written using this log method are
	 * tagged as "added metadata" which is not strictly part of the data
	 * received.
	 *
	 * @param message the message to log
	 * @throws IOException if an IO error occurs while writing to the logfile
	 */
	private void logRd(String message) throws IOException {
		log.write("RD " + System.currentTimeMillis() +
			": " + message + "\n");
		log.flush();
	}
}
