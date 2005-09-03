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

package nl.cwi.monetdb.mcl;

import nl.cwi.monetdb.mcl.messages.*;
import nl.cwi.monetdb.mcl.io.*;

/**
 * The MCLServer is the actual interface to a server process talking to
 * an MCL client.  It takes care of the MCL specific actions such as
 * dereferencing values and sending result sets in parts.  It functions
 * as useful abstraction that allows to retrieve the next MCLMessage
 * that is meant for the server process and send one or more responses
 * to this MCLMessage.
 * <br /><br />
 * The use of this class can schematically be represented like this:
 * <pre>
 *                 getNextMessage()
 *                       |
 *                       V
 *                 startResponse()
 *                       |
 *               isBatch V
 * +-- batchMessage() ---O
 * |                     |
 * |                     |<---------+
 * |            hasError V          |
 * +-- errorMessage() ---O          |
 * |                     |          |
 * |                     V          |
 * |                     X()        |
 * |                     |          |
 * |                     V else     |
 * |                     O----------+
 * |                done |
 * |                     V
 * +-------------> endResponse()
 * </pre>
 * In the diagram above, <tt>X()</tt> can be replaced with one of the
 * methods <tt>successMessage()</tt>, <tt>affectedRowsMessage()</tt>,
 * <tt>resultSet()</tt> and <tt>prepareResult()</tt>.
 */
public class MCLServer {
	private MCLValidator val;
	private MCLInputStream in;
	private MCLOutputStream out;

	/**
	 * Constructs an MCLServer using the given MCLInputStream and
	 * MCLOutputStream.  The caller is responsible for making sure the
	 * in and output streams are solely in use by this MCLServer.  The
	 * login-procedure is started immediately by sending a
	 * ChallengeMessage.
	 *
	 * @param in the MCLInputStream
	 * @param out the MCLOutputStream
	 * @throws MCLException if sending the ChallengeMessage failed
	 */
	public MCLServer(MCLInputStream in, MCLOutputStream out)
		throws MCLException
	{
		this.in = in;
		this.out = out;
		val = new MCLValidator();

		send(new ChallengeMessage());
	}

	/**
	 * Returns the next MCLMessage that is not handled internally by
	 * this MCLServer.
	 *
	 * @return the next MCLMessage available on the stream
	 * @throws MCLException if en error occurred while getting the next
	 * message
	 */
	public MCLMessage getNextMessage() throws MCLException {
		MCLMessage ret = MCLMessage.readFromStream(in);
		if (!val.check(ret)) {
			in.sync();
			throw new MCLException("Protocol violation: received illegal message from client");
		}

		return(ret);
	}

	public void send(MCLMessage m) throws MCLException {
		if (!val.check(m)) {
			throw new MCLException("Protocol violation: message is not allowed at this state");
		}
		m.writeToStream(out);
	}

	public void startResponse() {
	}

	public void endResponse() throws MCLException {
		//out.writePrompt();
	}

	public void errorMessage(String message) throws MCLException {
		// should force endResponse somehow
		if (message == null) {
			send(new ErrorMessage());
		} else {
			send(new ErrorMessage(message));
		}
	}

	public void affectedRowsMessage(int count) throws MCLException {
		send(new AffectedRowsMessage(count));
	}

	public void successMessage(String message) throws MCLException {
		if (message == null) {
			send(new SuccessMessage());
		} else {
			send(new SuccessMessage(message));
		}
	}

	public void batchMessage(int[] values) throws MCLException {
		send(new BatchResultMessage(values));
	}

	public void resultSet(MCLResultSet rs) {
		// do something: store it and send the header
	}

	public void prepareResult(MCLPrepareResult pr) {
		// do something: send the header
	}
}
