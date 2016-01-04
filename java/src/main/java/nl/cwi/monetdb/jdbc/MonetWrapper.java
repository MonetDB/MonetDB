/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc;

import java.sql.*;

/**
 * A Wrapper class that implements nothing.
 * 
 * This Class is used to simply provide JDBC4 Wrapper functions, for as
 * long as we don't really understand that they are for and what they
 * are supposed to do.  Hence the implementations are very stupid and
 * non-useful, ignoring any argument and claiming stuff doesn't work, or
 * won't work out.
 *
 * @author Fabian Groffen
 * @version 1.0
 */
public class MonetWrapper implements Wrapper {
	/**
	 * Returns true if this either implements the interface argument or
	 * is directly or indirectly a wrapper for an object that does.
	 * Returns false otherwise. If this implements the interface then
	 * return true, else if this is a wrapper then return the result of
	 * recursively calling isWrapperFor on the wrapped object. If this
	 * does not implement the interface and is not a wrapper, return
	 * false. This method should be implemented as a low-cost operation
	 * compared to unwrap so that callers can use this method to avoid
	 * expensive unwrap calls that may fail. If this method returns true
	 * then calling unwrap with the same argument should succeed.
	 *
	 * @param iface a Class defining an interface.
	 * @return true if this implements the interface or directly or
	 *         indirectly wraps an object that does.
	 * @throws SQLException if an error occurs while determining
	 *         whether this is a wrapper for an object with the given
	 *         interface.
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	/**
	 * Returns an object that implements the given interface to allow
	 * access to non-standard methods, or standard methods not exposed
	 * by the proxy. If the receiver implements the interface then the
	 * result is the receiver or a proxy for the receiver. If the
	 * receiver is a wrapper and the wrapped object implements the
	 * interface then the result is the wrapped object or a proxy for
	 * the wrapped object. Otherwise return the the result of calling
	 * unwrap recursively on the wrapped object or a proxy for that
	 * result. If the receiver is not a wrapper and does not implement
	 * the interface, then an SQLException is thrown.
	 *
	 * @param iface A Class defining an interface that the result must
	 *        implement.
	 * @return an object that implements the interface. May be a proxy
	 *         for the actual implementing object.
	 * @throws SQLException If no object found that implements the
	 *         interface
	 */
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("No object found (not implemented)", "0A000");
	}
}
