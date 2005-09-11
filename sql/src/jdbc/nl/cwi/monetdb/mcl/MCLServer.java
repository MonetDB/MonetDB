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
import java.util.*;

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
 * +-------------> endResponse(false)
 * </pre>
 * In the diagram above, <tt>X()</tt> can be replaced with one of the
 * methods <tt>successMessage()</tt>, <tt>affectedRowsMessage()</tt>,
 * <tt>resultSet()</tt> and <tt>prepareResult()</tt>.
 */
public class MCLServer {
	private MCLValidator val;
	private MCLInputStream in;
	private MCLOutputStream out;
	private List responseQueue;
	private Map resultSets;

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
		responseQueue = new ArrayList();
		resultSets = new HashMap();

		send(new ChallengeMessage());
		sendPrompt();
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

	/**
	 * Checks if the message is valid given the previous message seen,
	 * and sends it over the stream if so.  If the message is not valid,
	 * an MCLException is thrown.
	 *
	 * @param m the MCLMessage to send
	 * @throws MCLException if the message is not valid at this moment
	 * @throws MCLIOException if writing to the stream failed
	 */
	private void send(MCLMessage m) throws MCLException {
		if (!val.check(m)) {
			throw new MCLException("Protocol violation: message is not allowed at this state");
		}
		m.writeToStream(out);
	}

	/**
	 * Sends a prompt over the stream.
	 */
	private void sendPrompt() {
	}

	/**
	 * Sends a 'more' prompt over the stream.
	 */
	private void sendMorePrompt() {
	}

	/**
	 * Initialises a new response to a received message.  The response
	 * can consist of multiple MCLMessages.  Only one response can be
	 * created at a time.
	 *
	 * @throws MCLException if this method is called when there is
	 *         already a response opened
	 */
	public void startResponse() throws MCLException {
		if (responseQueue != null) throw
			new MCLException("Response already started or not closed");
		responseQueue = new ArrayList();
	}

	/**
	 * Adds an MCLMessage to the currently open response.
	 *
	 * @throws MCLException if there is no open response
	 */
	public void addToResponse(MCLMessage m) throws MCLException {
		if (responseQueue == null) throw
			new MCLException("Response not yet started");
		responseQueue.add(m);
	}

	/**
	 * Ends the open response and sends it to the client.  After all
	 * MCLMessages in the response are sent, a prompt is sent and the
	 * response is closed.
	 *
	 * @param more whether or not the prompt should be a 'more' prompt
	 * @throws MCLException if the reponse is not open, there are no
	 *         MCLMessages in the response, or sending the message(s)
	 *         failed.
	 */
	public void endResponse(boolean more) throws MCLException {
		if (responseQueue == null) throw
			new MCLException("Response not yet started");
		if (responseQueue.size() == 0) throw
			new MCLException("No messages in response");

		for (Iterator it = responseQueue.iterator(); it.hasNext(); ) {
			send((MCLMessage)(it.next()));
		}
		if (!more) {
			sendPrompt();
		} else {
			sendMorePrompt();
		}
		responseQueue = null;
	}

	/**
	 * Adds an ErrorMessage to the open response.  If there is no open
	 * response, this method will create a new response and add a new
	 * ErrorMessage to it.  Afterwards this method has been called, the
	 * open response is immediately closed (and sent).
	 *
	 * @param message the optional error message (may be null)
	 * @throws MCLException if sending the message failed
	 */
	public void errorMessage(String message) throws MCLException {
		// create a response if not yet there
		if (responseQueue == null) startResponse();
		
		// add the ErrorMessage to the response
		if (message == null) {
			addToResponse(new ErrorMessage());
		} else {
			addToResponse(new ErrorMessage(message));
		}

		// end and send the response
		endResponse(false);
	}

	/**
	 * Adds an AffectedRowsMessage to the response.  If no response is
	 * open yet, a new one is created, the message is added and closed.
	 * Note that this method does *not* close the response if it was
	 * already open.
	 *
	 * @param count the number of rows affected
	 * @throws MCLException if sending of the message failed
	 */
	public void affectedRowsMessage(int count) throws MCLException {
		// create a response if not yet there
		if (responseQueue == null) {
			startResponse();
			addToResponse(new AffectedRowsMessage(count));
			endResponse(false);
		} else {
			addToResponse(new AffectedRowsMessage(count));
		}
	}

	/**
	 * Adds a SuccessMessage to the response.  If no response is
	 * open yet, a new one is created, the message is added and closed.
	 * Note that this method does *not* close the response if it was
	 * already open.
	 *
	 * @param message the optional information, may be null
	 * @throws MCLException if sending of the message failed
	 */
	public void successMessage(String message) throws MCLException {
		boolean newResponse = false;
		// create a response if not yet there
		if (responseQueue == null) {
			startResponse();
			newResponse = true;
		}
		if (message == null) {
			addToResponse(new SuccessMessage());
		} else {
			addToResponse(new SuccessMessage(message));
		}
		if (newResponse) endResponse(false);
	}

	/**
	 * Adds a BatchMessage to the response.  If no response is
	 * open yet, a new one is created, the message is added and closed.
	 * Note that this method does *not* close the response if it was
	 * already open.
	 *
	 * @param values the update values in the batch
	 * @throws MCLException if sending of the message failed
	 */
	public void batchMessage(int[] values) throws MCLException {
		// create a response if not yet there
		if (responseQueue == null) {
			startResponse();
			addToResponse(new BatchResultMessage(values));
			endResponse(false);
		} else {
			addToResponse(new BatchResultMessage(values));
		}
	}

	/**
	 * Adds the given MCLResultSet to the response.  If no response is
	 * open yet, a new one is created, the message is added and closed.
	 * Note that this method does *not* close the response if it was
	 * already open.
	 *
	 * @param rs the MCLResultSet to add to the response
	 * @throws MCLException if sending of the message failed
	 */
	public void resultSet(MCLResultSet rs) throws MCLException {
		if (rs == null) throw
			new MCLException("MCLResultSet should not be null");

		// create a response if not yet there
		if (responseQueue == null) {
			startResponse();
			addToResponse(rs.getHeaderMessage());
			endResponse(false);
		} else {
			addToResponse(rs.getHeaderMessage());
		}

		resultSets.put(rs.getId(), rs);
	}

	/**
	 * Adds the given MCLPrepareResult to the response.  If no response
	 * is open yet, a new one is created, the message is added and
	 * closed.  Note that this method does *not* close the response if
	 * it was already open.
	 *
	 * @param pr the MCLPrepareResult to add to the response
	 * @throws MCLException if sending of the message failed
	 */
	public void prepareResult(MCLPrepareResult pr) throws MCLException {
		// do something: send the header
		throw new MCLException("TODO: implement it");
	}
}
