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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.merovingian;

import java.util.*;

/**
 * Implementation of the Sabaoth C-struct as Java object.
 * <br /><br />
 * This Class implements a parser for the string representation of a
 * sabaoth information struct as returned by monetdbd.
 * <br />
 * Currently this class implements version 1 and 2 of the sabdb
 * serialisation.
 *
 * @version 2.0
 */
public class SabaothDB {
	/** The name of the database */
	private String dbname;
	/** The URI how to connect to this database, or null if not
	 * shared */
	private String uri;
	/** Whether or not the database is under maintenance */
	private boolean locked;
	/** The state of this database, one of SABdbState */
	private SABdbState state;
	/** A list of Strings representing the available scenarios of this
	 * database */
	private String[] scenarios;
	/** The number of times this database was started */
	private int startCounter;
	/** The number of times this database was stopped */
	private int stopCounter;
	/** The number of times this database has crashed */
	private int crashCounter;
	/** The number of seconds this database was running on average */
	private long avgUptime;
	/** The maximum number of seconds this database was running */
	private long maxUptime;
	/** The minimum number of seconds this database was running */
	private long minUptime;
	/** The last time this database crashed, null if never */
	private Date lastCrash;
	/** The last time this database was started, null if never */
	private Date lastStart;
	/** The last time this database was stopped, null if never */
	private Date lastStop;
	/** Whether the last start was a crash */
	private boolean crashAvg1;
	/** Average of crashes in the last 10 start attempts */
	private double crashAvg10;
	/** Average of crashes in the last 30 start attempts */
	private double crashAvg30;

	/** The serialised format header */
	private final String sabdbhdr = "sabdb:";

	/** Sabaoth state enumeration */
	public enum SABdbState {
		SABdbIllegal (0),
		SABdbRunning (1),
		SABdbCrashed (2),
		SABdbInactive(3),
		SABdbStarting(4);

		private final int cValue;

		SABdbState(int cValue) {
			this.cValue = cValue;
		}

		public int getcValue() {
			return(cValue);
		}

		static SABdbState getInstance(int val) throws IllegalArgumentException {
			/* using this syntax because I can't find examples that do
			 * it without it */
			for (SABdbState s : SABdbState.values()) {
				if (s.getcValue() == val) {
					return(s);
				}
			}
			throw new IllegalArgumentException("No such state with value: "
					+ val);
		}
	}

	
	/**
	 * Constructs a new SabaothDB object from a String.
	 *
	 * @param sabdb the serialised sabdb C-struct
	 */
	public SabaothDB(String sabdb)
		throws IllegalArgumentException
	{
		if (sabdb == null)
			throw new IllegalArgumentException("String is null");
		if (!sabdb.startsWith(sabdbhdr))
			throw new IllegalArgumentException("String is not a Sabaoth struct");
		String[] parts = sabdb.split(":", 3);
		if (parts.length != 3)
			throw new IllegalArgumentException("String seems incomplete, " +
					"expected 3 components, only found " + parts.length);
		int protover;
		try {
			protover = Integer.parseInt(parts[1]);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Illegal protocol version: " +
					parts[1]);
		}
		if (protover != 1 && protover != 2)
			throw new IllegalArgumentException("Unsupported protocol version: " + protover);
		sabdb = parts[2];
		parts = sabdb.split(",", -2);
		if (protover == 1 && parts.length != 16 || protover == 2 && parts.length != 17)
			throw new IllegalArgumentException("String seems wrong, " +
					"unexpected number of components: " + parts.length);

		/* Sabaoth sabdb struct */
		int pc = 0;
		this.dbname = parts[pc++];
		if (protover == 1) {
			this.uri = "<unknown>";
			int lastslash = this.dbname.lastIndexOf('/');
			if (lastslash == -1)
				throw new IllegalArgumentException("invalid path (needs slash): " + this.dbname);
			this.dbname = this.dbname.substring(lastslash + 1);
		} else {
			this.uri = parts[pc++];
		}
		this.locked = parts[pc++].equals("1") ? true : false;
		this.state = SABdbState.getInstance(Integer.parseInt(parts[pc++]));
		this.scenarios = parts[pc++].split("'");
		if (protover == 1)  /* skip connections */
			pc++;
		/* Sabaoth sabuplog struct */
		this.startCounter = Integer.parseInt(parts[pc++]);
		this.stopCounter = Integer.parseInt(parts[pc++]);
		this.crashCounter = Integer.parseInt(parts[pc++]);
		this.avgUptime = Long.parseLong(parts[pc++]);
		this.maxUptime = Long.parseLong(parts[pc++]);
		this.minUptime = Long.parseLong(parts[pc++]);
		long t = Long.parseLong(parts[pc++]);
		if (t == -1) {
			this.lastCrash = null;
		} else {
			this.lastCrash = new Date(t * 1000);
		}
		t = Long.parseLong(parts[pc++]);
		if (t == -1) {
			this.lastStart = null;
		} else {
			this.lastStart = new Date(t * 1000);
		}
		if (protover != 1) {
			t = Long.parseLong(parts[pc++]);
			if (t == -1) {
				this.lastStop = null;
			} else {
				this.lastStop = new Date(t * 1000);
			}
		} else {
			this.lastStop = null;
		}
		this.crashAvg1 = parts[pc++].equals("1") ? true : false;
		this.crashAvg10 = Double.parseDouble(parts[pc++]);
		this.crashAvg30 = Double.parseDouble(parts[pc++]);
	}

	public String getName() {
		return(dbname);
	}

	public String getURI() {
		return(uri);
	}

	public boolean isLocked() {
		return(locked);
	}

	public SABdbState getState() {
		return(state);
	}

	public String[] getScenarios() {
		return(scenarios);
	}

	public int getStartCount() {
		return(startCounter);
	}

	public int getStopCount() {
		return(stopCounter);
	}

	public int getCrashCount() {
		return(crashCounter);
	}

	public long getAverageUptime() {
		return(avgUptime);
	}

	public long getMaximumUptime() {
		return(maxUptime);
	}

	public long getMinimumUptime() {
		return(minUptime);
	}

	public Date lastCrashed() {
		return(lastCrash);
	}

	public Date lastStarted() {
		return(lastStart);
	}

	public Date lastStopped() {
		return(lastStop);
	}

	public boolean lastStartWasSuccessful() {
		return(crashAvg1);
	}

	public double getCrashAverage10() {
		return(crashAvg10);
	}

	public double getCrashAverage30() {
		return(crashAvg30);
	}
}
