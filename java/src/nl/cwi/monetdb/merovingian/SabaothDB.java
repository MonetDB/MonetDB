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

package nl.cwi.monetdb.merovingian;

import java.util.*;

/**
 * Implementation of the Sabaoth C-struct as Java object.
 * <br /><br />
 * XXXX
 * <br />
 * XXX
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 * @version 1.0
 */
public class SabaothDB {
	/** The physical path at the server side */
	private String path;
	/** Whether or not the database is under maintenance */
	private boolean locked;
	/** The state of this database, one of SABdbState */
	private SABdbState state;
	/** A list of Strings representing the available scenarios of this
	 * database */
	private String[] scenarios;
	/** A list of Strings representing the available connections to this
	 * database */
	private String[] connections;
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
	/** Whether the last start was a crash */
	private boolean crashAvg1;
	/** Average of crashes in the last 10 start attempts */
	private double crashAvg10;
	/** Average of crashes in the last 30 start attempts */
	private double crashAvg30;

	/** The serialised format version of a Sabaoth struct we support */
	private final String sabdbver = "sabdb:1:";

	/** Sabaoth state enumeration */
	public enum SABdbState {
		SABdbIllegal (0),
		SABdbRunning (1),
		SABdbCrashed (2),
		SABdbInactive(3);

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
		if (!sabdb.startsWith(sabdbver))
			throw new IllegalArgumentException("String is not a " +
					sabdbver + " Sabaoth struct");
		String[] parts = sabdb.substring(sabdbver.length()).split(",", -2);
		if (parts.length != 16)
			throw new IllegalArgumentException("String seems incomplete, " +
					"expected 16 components, only found " + parts.length);

		/* Sabaoth sabdb struct */
		this.path = parts[0];
		this.locked = parts[1].equals("1") ? true : false;
		this.state = SABdbState.getInstance(Integer.parseInt(parts[2]));
		this.scenarios = parts[3].split("'");
		this.connections = parts[4].split("'");
		/* Sabaoth sabuplog struct */
		this.startCounter = Integer.parseInt(parts[5]);
		this.stopCounter = Integer.parseInt(parts[6]);
		this.crashCounter = Integer.parseInt(parts[7]);
		this.avgUptime = Long.parseLong(parts[8]);
		this.maxUptime = Long.parseLong(parts[9]);
		this.minUptime = Long.parseLong(parts[10]);
		long t = Long.parseLong(parts[11]);
		if (t == -1) {
			this.lastCrash = null;
		} else {
			this.lastCrash = new Date(t * 1000);
		}
		t = Long.parseLong(parts[12]);
		if (t == -1) {
			this.lastCrash = null;
		} else {
			this.lastStart = new Date(t * 1000);
		}
		this.crashAvg1 = parts[1].equals("1") ? true : false;
		this.crashAvg10 = Double.parseDouble(parts[14]);
		this.crashAvg30 = Double.parseDouble(parts[15]);
	}

	public String getName() {
		return(path.substring(path.lastIndexOf('/') + 1));
	}

	public String getPath() {
		return(path);
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

	public String[] getConnections() {
		return(connections);
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
