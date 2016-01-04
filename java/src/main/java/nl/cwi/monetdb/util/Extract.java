/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


/**
 * This file contains a function to extract files from its including Jar
 * package.
 *
 * @author Ying Zhang <Y.Zhang@cwi.nl>
 * @version 0.1
 */

public class Extract {
	private static final int DEFAULT_BUFSIZE = 16386;

    public Extract() {}

    /**
     * Extracts a file from the Jar package which includes this class to
     * the given destination
     * @param fromFile The file to extract, including it absolute path
     * inside its including Jar package.
     * @param toFile Destination for the extracted file
     * @throws FileNotFoundException If the file to extract can not be
     * found in its including Jar package.
     * @throws IOException If any error happens during
     * creating/reading/writing files.
     */
	public static void extractFile(String fromFile, String toFile)
		throws FileNotFoundException, IOException
	{
		char[] cbuf = new char[DEFAULT_BUFSIZE];
		int ret = 0;

		InputStream is = new Extract().getClass().getResourceAsStream(fromFile);

		if(is == null) {
			throw new FileNotFoundException("File " + fromFile +
					" does not exist in the JAR package.");
		}

		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		FileWriter writer = new FileWriter(toFile, false);

		ret = reader.read(cbuf, 0, DEFAULT_BUFSIZE);
		while(ret > 0){
			writer.write(cbuf, 0, ret);
			ret = reader.read(cbuf, 0, DEFAULT_BUFSIZE);
		}
		reader.close();
		writer.close();
	}
}
