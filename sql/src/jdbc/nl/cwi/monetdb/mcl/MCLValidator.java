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

import nl.cwi.monetdb.mcl.io.*;
import nl.cwi.monetdb.mcl.messages.*;

/**
 * The Validator class within MCL is the class that actually checks the
 * validity of the flow of MCLMessages.  Where MCLMessages check the
 * validity on the level of MCLSentences, the MCLValidator checks the
 * MCLMessages.  The MCLValidator checks that MCLMessages are sent and
 * received in the right order, while the Manager offers services such
 * as under the hood support for receiving result sets in fragments.
 * <br /><br />
 * This MCLValidator is implemented as a parser which checks the
 * validity of an MCLMessage based on the previous MCLMessage seen.
 * Hence, it cannot easily suggest or tell what it would like to see,
 * but this is not important for the MCLValidator.
 * <br /><br />
 * This class is thread safe.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MCLValidator {
	private int state;

	/**
	 * Constructs an MCLValidator having an initial state that expects a
	 * new MCL connection to be set up.  Hence, the only MCLMessage
	 * accepted will be the ChallengeMessage.  If this is undesired
	 * behaviour, consider using the MCLValidator(int state)
	 * constructor.
	 *
	 * @see #MCLValidator(int state)
	 */
	public MCLValidator() {
		state = 0;	// means initial state, i.e. we expect a login
	}

	/**
	 * Constructs an MCLValidator that is put in the specified state.
	 * This allows to 'jump into' a MCLConnection, or just skip the
	 * initial login procedure, for example.
	 *
	 * @param state the initial state to assume
	 */
	public MCLValidator(int state) {
		// note: no guarantees on validity of the state here!
		this.state = state;
	}

	/**
	 * Returns whether the given MCLMessage is allowed with respect to
	 * the last MCLMessage seen.  Calling this method for each
	 * MCLMessage sent or received ensures that the MCL protocol is not
	 * violated.  If this method returns false, the internal state is
	 * not changed.  Else, if this method returns true, the internal
	 * state is updated to reflect the state associated with the
	 * MCLMessage checked.
	 *
	 * @param in MCLMessage to check
	 * @return a boolean indicating whether this MCLMessage is allowed
	 *         in the current state
	 */
	public synchronized boolean check(MCLMessage in) {
		/* We implement the validation inline in this method as a big
		 * switch.  This is not correct, as more state needs to be
		 * kept to keep track of relationships between questions and
		 * answers.  However, we stick to it for now.
		 */
		int newState = in.getType();

		switch (newState) {
			case ChallengeMessage.identifier:
				if (state != 0)
					return(false);
			break;
			case ChallengeResponseMessage.identifier:
				if (state != ChallengeMessage.identifier)
					return(false);
			break;
			case ExportRequestMessage.identifier:
			case DereferenceRequestMessage.identifier:
			case CloseMessage.identifier:
			case QueryMessage.identifier:
			case BatchMessage.identifier:
			case PrepareMessage.identifier:
			case ExecutePreparedMessage.identifier:
				if (
						state == ChallengeMessage.identifier ||
						state == ExportRequestMessage.identifier ||
						state == DereferenceRequestMessage.identifier ||
						state == CloseMessage.identifier ||
						state == QueryMessage.identifier ||
						state == BatchMessage.identifier ||
						state == PrepareMessage.identifier ||
						state == ExecutePreparedMessage.identifier
					)
				{
					return(false);
				}
			break;
			case HeaderMessage.identifier:
				if (
						state != QueryMessage.identifier &&
						state != ExecutePreparedMessage.identifier
					)
				{
					return(false);
				}
			break;
			case DataMessage.identifier:
				if (
						state != ExportRequestMessage.identifier
					)
				{
					return(false);
				}
			break;
			case RawResultMessage.identifier:
				if (
						state != DereferenceRequestMessage.identifier
					)
				{
					return(false);
				}
			break;
			case PrepareResultMessage.identifier:
				if (
						state != PrepareMessage.identifier
					)
				{
					return(false);
				}
			break;
			case AffectedRowsMessage.identifier:
				if (
						state != QueryMessage.identifier
					)
				{
					return(false);
				}
			break;
			case BatchResultMessage.identifier:
				if (
						state != BatchMessage.identifier
					)
				{
					return(false);
				}
			break;
			case SuccessMessage.identifier:
				if (
						state != QueryMessage.identifier &&
						state != CloseMessage.identifier
					)
				{
					return(false);
				}
			break;
			case ErrorMessage.identifier:
				if (
						state != ChallengeMessage.identifier &&
						state != ExportRequestMessage.identifier &&
						state != DereferenceRequestMessage.identifier &&
						state != CloseMessage.identifier &&
						state != QueryMessage.identifier &&
						state != BatchMessage.identifier &&
						state != PrepareMessage.identifier &&
						state != ExecutePreparedMessage.identifier
					)
				{
					return(false);
				}
			break;
		}
		state = newState;
		return(true);
	}
}
