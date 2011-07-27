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

package nl.cwi.monetdb.util;

import java.io.*;
import java.util.*;

/**
 * This Class prompts the user for a password and attempts to mask input
 * with "*" characters.
 */
public class PasswordField {
	/**
	 * @param in stream to be used (e.g. System.in)
	 * @param prompt The prompt to display to the user.
	 * @return The password as entered by the user.
	 */
	public static final char[] getPassword(InputStream in, String prompt)
		throws IOException
	{
		MaskingThread maskingthread = new MaskingThread(prompt);
		Thread thread = new Thread(maskingthread);
		thread.start();

		char[] lineBuffer;
		char[] buf;

		buf = lineBuffer = new char[128];

		int room = buf.length;
		int offset = 0;
		int c;

		boolean finished = false;
		while (!finished) {
			switch (c = in.read()) {
				case -1:
				case '\n':
					finished = true;
					continue;

				case '\r':
					int c2 = in.read();
					if ((c2 != '\n') && (c2 != -1)) {
						if (!(in instanceof PushbackInputStream)) {
							in = new PushbackInputStream(in);
						}
						((PushbackInputStream)in).unread(c2);
					} else {
						finished = true;
						continue;
					}

				default:
					if (--room < 0) {
						buf = new char[offset + 128];
						room = buf.length - offset - 1;
						System.arraycopy(lineBuffer, 0, buf, 0, offset);
						Arrays.fill(lineBuffer, ' ');
						lineBuffer = buf;
					}
					buf[offset++] = (char) c;
				break;
			}
		}
		maskingthread.stopMasking();
		if (offset == 0) {
			return(null);
		}
		char[] ret = new char[offset];
		System.arraycopy(buf, 0, ret, 0, offset);
		Arrays.fill(buf, ' ');
		return(ret);
	}
}

class MaskingThread extends Thread {
	private volatile boolean stop = false;

	/**
	 *@param prompt The prompt displayed to the user
	 */
	public MaskingThread(String prompt) {
		System.err.print(prompt);
	}


	/**
	 * Begin masking until asked to stop.
	 */
	public void run() {
		int priority = Thread.currentThread().getPriority();
		Thread.currentThread().setPriority(Thread.MAX_PRIORITY);

		try {
			while (!stop) {
				System.err.print("\010 ");
				System.err.flush();
				try {
					// attempt masking at this rate
					Thread.sleep(1);
				} catch (InterruptedException iex) {
					Thread.currentThread().interrupt();
					return;
				}
			}
			System.out.print("\010" + "  \010\010");
		} finally {
			// restore the original priority
			Thread.currentThread().setPriority(priority);
		}
	}


	/**
	 * Instruct the thread to stop masking.
	 */
	public void stopMasking() {
		stop = true;
	}
}
