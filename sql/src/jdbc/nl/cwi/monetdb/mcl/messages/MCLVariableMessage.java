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

package nl.cwi.monetdb.mcl.messages;

import java.util.*;
import nl.cwi.monetdb.mcl.*;
import nl.cwi.monetdb.mcl.io.*;

/**
 * A Message object represents one of the messages present in the MCL
 * protocol.  A Message has some default functionality, which is defined
 * in this abstract class, and some required functionality which each
 * implementing class is required to fill in.  In comparison with the
 * MCLMessage, the MCLVariableMessage has an upfront unknown number of
 * Sentences, next to the fixed Sentences as used in MCLMessage.
 * Typical Messages that need this behaviour are Messages that store
 * data such as queries or their results.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public abstract class MCLVariableMessage extends MCLMessage {
	protected List variableSentences;
	
	/**
	 * Returns an Iterator over all Sentences contained within this
	 * Message.  The StartOfMessage and Prompt Sentences are not
	 * included in the Iterator.
	 *
	 * @return an Iterator over all Sentences
	 */
	public Iterator getSentences() {
		return(new Iterator() {
			boolean hasNext = true;
			int pos = 0;

			/**
			 * Returns true if the iteration has more elements. (In
			 * other words, returns true if next would return an element
			 * rather than throwing an exception.)
			 *
			 * @return true if the iterator has more elements.
			 */
			public boolean hasNext() {
				return(hasNext);
			}

			/**
			 * Returns the next element in the iteration.
			 *
			 * @return the next element in the iteration.
			 * @throws NoSuchElementException iteration has no more
			 * elements.
			 */
			public Object next() throws NoSuchElementException {
				if (!hasNext) throw new NoSuchElementException();
				
				if (pos < sentences.length) {
					return(sentences[pos]);
				} else {
					if (variableSentences.size() == 0 ||
							variableSentences.size() - 1 == pos - sentences.length)
						hasNext = false;

					return(variableSentences.get(pos - sentences.length));
				}
			}

			/**
			 * Remove is not supported.
			 */
			public void remove() throws UnsupportedOperationException {
				throw new UnsupportedOperationException();
			}
		});
	}
}
