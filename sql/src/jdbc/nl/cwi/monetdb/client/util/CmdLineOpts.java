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

package nl.cwi.monetdb.client.util;

import java.util.*;
import java.io.*;

public class CmdLineOpts {
	/** the arguments we handle */
	private Map opts = new HashMap();

	/** no arguments */
	public static int CAR_ZERO		= 0;
	/** always one argument */
	public static int CAR_ONE		= 1;
	/** zero or one argument */
	public static int CAR_ZERO_ONE	= 2;
	/** zero or many arguments */
	public static int CAR_ZERO_MANY	= 3;
	/** one or many arguments */
	public static int CAR_ONE_MANY	= 4;
	
	public CmdLineOpts() {
	}

	public void addOption(
			String shorta,
			String longa,
			int type,
			String defaulta)
		throws OptionsException
	{
		OptionContainer oc = new OptionContainer(shorta, longa, type, defaulta);
		if (shorta != null) opts.put(shorta, oc);
		if (longa != null) opts.put(longa, oc);
	}

	public void removeOption(String name) {
		OptionContainer oc = null;
		if ((oc = (OptionContainer)(opts.get(name))) != null) {
			opts.remove(oc.shorta);
			opts.remove(oc.longa);
		}
	}

	public OptionContainer getOption(String key) throws OptionsException {
		OptionContainer ret = (OptionContainer)(opts.get(key));
		if (ret == null) throw
			new OptionsException("No such option: " + key);

		return(ret);
	}

	public void processFile(File file) throws OptionsException {
		// the file is there, parse it and set its settings
		Properties prop = new Properties();
		try {
			FileInputStream in = new FileInputStream(file);
			prop.load(in);
			in.close();

			for (Enumeration e = prop.propertyNames(); e.hasMoreElements(); ) {
				String key = (String)(e.nextElement());
				OptionContainer option = ((OptionContainer)opts.get(key));
				if (option == null) throw
					new OptionsException("Unknown option: " + key);
				option.resetArguments();
				option.addArgument(prop.getProperty(key));
			}
		} catch (IOException e) {
			// well... then forget about it
		}
	}

	public void processArgs(String args[]) throws OptionsException {
		// parse and set the command line arguments
		OptionContainer option = null;
		int quant = -1;
		int qcount = 0;
		boolean moreData = false;
		for (int i = 0; i < args.length; i++) {
			if (option == null) {
				if (args[i].charAt(0) != '-') throw
					new OptionsException("Unexpected value: " + args[i]);

				// see what kind of argument we have
				if (args[i].length() == 1)
					throw new OptionsException("Illegal argument: '-'");
				if (args[i].charAt(1) == '-') {
					// we have a long argument
					// since we don't accept inline values we can take
					// everything left in the string as argument
					option = (OptionContainer)(opts.get(args[i].substring(2)));
					moreData = false;
				} else if (args[i].charAt(1) == 'X') {
					// extra argument, same as long argument
					option = (OptionContainer)(opts.get(args[i].substring(1)));
					moreData = false;
				} else {
					// single char argument
					option = (OptionContainer)(opts.get("" + args[i].charAt(1)));
					// is there more data left in the argument?
					moreData = args[i].length() > 2 ? true : false;
				}

				if (option != null) {
					// make sure we overwrite previously set arguments
					option.resetArguments();
					int card = option.getCardinality();
					if (card == CAR_ONE) {
						if (moreData) {
							option.addArgument(args[i].substring(2));
							option = null;
						} else {
							quant = 1;
						}
					} else if (card == CAR_ZERO_ONE) {
						option.setPresent();
						qcount = 1;
						quant = 2;
						if (moreData) {
							option.addArgument(args[i].substring(2));
							option = null;
						}
					} else if (card == CAR_ZERO_MANY) {
						option.setPresent();
						qcount = 1;
						quant = -1;
						if (moreData) {
							option.addArgument(args[i].substring(2));
							qcount++;
						}
					} else if (card == CAR_ZERO) {
						option.setPresent();
						option = null;
					}
				} else {
					throw new OptionsException("Unknown argument: " + args[i]);
				}
			} else {
				// store the `value'
				option.addArgument(args[i]);
				if (++qcount == quant) {
					quant = 0;
					qcount = 0;
					option = null;
				}
			}
		}
	}

	public class OptionContainer {
		int cardinality;
		String shorta;
		String longa;
		List values;
		String name;
		String defaulta;
		boolean present;

		public OptionContainer(
				String shorta, 
				String longa,
				int cardinality,
				String defaulta)
			throws IllegalArgumentException
		{
			this.cardinality = cardinality;
			this.shorta = shorta;
			this.longa = longa;
			this.defaulta = defaulta;
			this.present = false;

			if (cardinality != CAR_ZERO &&
					cardinality != CAR_ONE &&
					cardinality != CAR_ZERO_ONE &&
					cardinality != CAR_ZERO_MANY &&
					cardinality != CAR_ONE_MANY)
				throw new IllegalArgumentException("unknown cardinality");
			if (shorta != null && shorta.length() != 1) throw
				new IllegalArgumentException("short option should consist of exactly one character");
			if (shorta == null && longa == null) throw
				new IllegalArgumentException("either a short or long argument should be given");
			if ((cardinality == CAR_ZERO ||
					cardinality == CAR_ZERO_ONE ||
					cardinality == CAR_ZERO_MANY) &&
					defaulta != null)
			{
				throw new IllegalArgumentException("cannot specify a default for a (possible) zero argument option");
			}

			values = new ArrayList();

			if (longa != null) {
				name = longa;
			} else {
				name = shorta;
			}
		}

		public void resetArguments() {
			values.clear();
		}

		public void addArgument(String val) throws OptionsException {
			if (cardinality == CAR_ZERO) {
				throw new OptionsException("option " + name + " does not allow arguments");
			} else if ((cardinality == CAR_ONE ||
					cardinality == CAR_ZERO_ONE) &&
					values.size() >= 1)
			{
				throw new OptionsException("option " + name + " does at max allow only one argument");
			}
			// we can add it
			values.add(val);
			setPresent();
		}

		public void setPresent() {
			present = true;
		}

		public boolean isPresent() {
			return(present);
		}

		public int getCardinality() {
			return(cardinality);
		}

		public int getArgumentCount() {
			return(values.size());
		}

		public String getArgument() {
			String ret = getArgument(1);
			if (ret == null) ret = defaulta;
			return(ret);
		}

		public String getArgument(int index) {
			String[] args = getArguments();
			if (index < 1 || index > args.length) return(null);
			return(args[index - 1]);
		}

		public String[] getArguments() {
			return((String[])(values.toArray(new String[values.size()])));
		}
	}
}
