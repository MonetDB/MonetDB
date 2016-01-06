/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.cwi.monetdb.mcl.parser.MCLParseException;

/**
 * Use this class to restore an SQL dump file.
 */
public class SQLRestore {

	private final String _host;
	private final int _port;
	private final String _user;
	private final String _password;
	private final String _dbName;
	
	public SQLRestore(String host, int port, String user, String password, String dbName) throws IOException {
		if (host == null || user == null || password == null || dbName == null)
			throw new NullPointerException();
		_host = host;
		_port = port;
		_user = user;
		_password = password;
		_dbName = dbName;
	}
	
	private static class ServerResponseReader implements Runnable {
		private final BufferedMCLReader _is;
		private final AtomicBoolean _errorState = new AtomicBoolean(false);
		private String _errorMessage = null; 
		
		ServerResponseReader(BufferedMCLReader is) {			
			_is = is;
		}
		
		public void run() {
			try {
				while (true) {
					String line = _is.readLine();
					if (line == null)
						break;
					int result = _is.getLineType();
					switch (result) { 
					case BufferedMCLReader.ERROR:
						_errorMessage = line;
						_errorState.set(true);
						return;
					default:
						// do nothing...
					}
				}
				
			} catch (IOException e) {
				_errorMessage = e.getMessage();
				_errorState.set(true);
			} finally {
				try {
					_is.close();
				} catch (IOException e) {
					// ignore errors
				}
			}
		}
		
		/**
		 * @return whether the server has responded with an error. Any
		 *         error is regarded as fatal.
		 */
		public boolean inErrorState() {
			return _errorState.get();
		}
		
		/**
		 * @return the error message if inErrorState() is true. Behaviour is 
		 * 		   not defined if called before inErrorState is true.
		 */
		public String getErrorMessage() {
			return _errorMessage;
		}
		
	}
	
	/**
	 * Restores a given SQL dump to the database.
	 * 
	 * @param source
	 * @throws IOException
	 */
	public void restore(File source) throws IOException {
		MapiSocket ms = new MapiSocket();
		try {
			ms.setLanguage("sql");
			ms.setDatabase(_dbName);
			ms.connect(_host, _port, _user, _password);
			
			BufferedMCLWriter os = ms.getWriter();
			BufferedMCLReader reader = ms.getReader();
			
			ServerResponseReader srr = new ServerResponseReader(reader);

			Thread responseReaderThread = new Thread(srr);
			responseReaderThread.start();
			try {
				// FIXME: we assume here that the dump is in system's default encoding
				BufferedReader sourceData = new BufferedReader(new FileReader(source));
				try { 
					os.write('s'); // signal that a new statement (or series of) is coming
					while(!srr.inErrorState()) {
						char[] buf = new char[4096];
						int result = sourceData.read(buf);
						if (result < 0) 
							break;				
						os.write(buf, 0, result);
					}
					
					os.flush(); // mark the end of the statement (or series of)
					os.close();
				} finally {
					sourceData.close();
				}
			} finally {
				try {
					responseReaderThread.join();
				} catch (InterruptedException e) {
					throw new IOException(e.getMessage());
				}
				
				// if the server signalled an error, we should respect it... 
				if (srr.inErrorState()) {
					throw new IOException(srr.getErrorMessage());
				}
			}
		} catch (MCLException e) {
			throw new IOException(e.getMessage());
		} catch (MCLParseException e) {
			throw new IOException(e.getMessage());
		} finally {
			ms.close();
		}
	}
	
	public void close() {
		// do nothing at the moment...
	}
	
	
	public static void main(String[] args) throws IOException {
		if (args.length != 6) {
			System.err.println("USAGE: java " + SQLRestore.class.getName() + 
					" <host> <port> <user> <password> <dbname> <dumpfile>");
			System.exit(1);
		}
		
		// parse arguments
		String host = args[0];
		int port = Integer.parseInt(args[1]); // FIXME: catch NumberFormatException
		String user = args[2];
		String password = args[3];
		String dbName = args[4];
		File dumpFile = new File(args[5]);
		
		// check arguments
		if (!dumpFile.isFile() || !dumpFile.canRead()) {
			System.err.println("Cannot read: " + dumpFile);
			System.exit(1);
		}
		
		SQLRestore md = new SQLRestore(host, port, user, password, dbName);
		try {
			System.out.println("Start restoring " + dumpFile);
			long duration = -System.currentTimeMillis();
			md.restore(dumpFile);
			duration += System.currentTimeMillis();
			System.out.println("Restoring took: " + duration + "ms");
		} finally {
			md.close();
		}
	}
}
