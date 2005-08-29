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

package nl.cwi.monetdb.xmldb.modules;

import org.xmldb.api.base.*;
import org.xmldb.api.modules.*;
import nl.cwi.monetdb.xmldb.base.*;

// XML parsing stuff
import javax.xml.parsers.*;
import org.w3c.dom.*;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

// needed for streams for XML parsers
import java.io.*;

/**
 * Provides access to XML resources stored in the database.  An
 * XMLResource can be accessed either as text XML or via the DOM or SAX
 * APIs.<br />
 * <br />
 * The default behaviour for getContent and setContent is to work with
 * XML data as text so these methods work on String content.<br />
 * <br />
 * A MonetDBXMLResource is immutable and based on a String of XML data.
 * Any attempt to change the content of this Resource will result in an
 * XMLDBException with error code ErrorCodes.VENDOR_ERROR and message
 * "This Resource is immutable".  In contrast with the ResourceSet,
 * updates to the contents of this object are not allowed to stress the
 * fact that the results are read-only.
 * 
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MonetDBXMLResource implements XMLResource {
	private final String xml;
	private final Collection parent;

	/** SAX parser to be used.  Needs to be known upfront because
	 * features can be set for it. */
	private final XMLReader saxparser;

	/**
	 * Constructor for a String based XMLResource.  Internally the XML
	 * is kept as string, supplied as argument to this constructor.
	 *
	 * @param xml the XML contents of this Resource as String
	 */
	public MonetDBXMLResource(String xml, MonetDBCollection parent)
		throws XMLDBException
	{
		// assign the blank finals
		this.xml = xml;
		this.parent = parent;
		try {
			this.saxparser = XMLReaderFactory.createXMLReader();
		} catch (SAXException e) {
			// include Exception name
			throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.toString());
		}
	}
	
	//== interface org.xmldb.api.base.Resource

	/**
	 * Returns the Collection instance that this resource is associated
	 * with.  All resources must exist within the context of a
	 * Collection.
	 *
	 * @return the Collection associated with this Resource
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public Collection getParentCollection() throws XMLDBException {
		// should return MonetDBCollection thingher
		return(parent);
	}

	/**
	 * Returns the unique id for this Resource or null if the Resource
	 * is anonymous.  The Resource will be anonymous if it is obtained
	 * as the result of a query.
	 *
	 * @return the id for the Resource or null if no id exists.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public String getId() throws XMLDBException {
		// our results are always obtained as result of a query
		return(null);
	}

	/**
	 * Returns the resource type for this Resource.<br />
	 * <br />
	 * XML:DB defined resource types are:<br />
	 * XMLResource - all XML data stored in the database
	 * BinaryResource - Binary blob data stored in the database
	 *
	 * @return the resource type for the Resource.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public String getResourceType() throws XMLDBException {
		// RESOURCE_TYPE is defined in the interface XMLResource
		return(RESOURCE_TYPE);
	}

	/**
	 * Retrieves the content from the resource.  The type of the content
	 * varies depending what type of resource is being used.
	 *
	 * @return the content of the resource as XML String
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public Object getContent() throws XMLDBException {
		// we return as String
		return(xml);
	}

	/**
	 * Sets the content for this resource.  The type of content that can
	 * be set depends on the type of resource being used.
	 *
	 * @param value the content value to set for the Resource
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public void setContent(Object value) throws XMLDBException {
		throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "This Resource is immutable");
	}

	//== end interface Resource
	
	//== Interface org.xmldb.api.modules.XMLResource
	
	/**
	 * Returns the unique id for the parent document to this Resource
	 * or null if the Resource does not have a parent document.
	 * getDocumentId() is typically used with Resource instances
	 * retrieved using a query.  It enables accessing the parent
	 * document of the Resource even if the Resource is a child node of
	 * the document.  If the Resource was not obtained through a query
	 * then getId() and getDocumentId() will return the same id.
	 *
	 * @return the id for the parent document of this Resource or null
	 *         if there is no parent document for this Resource.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public String getDocumentId() throws XMLDBException {
		// hmmm... oops... I dunno...  it should return an id, but...
		return(null);
	}

	/**
	 * Returns the content of the Resource as a DOM Node.
	 *
	 * @return The XML content as a DOM Node
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public Node getContentAsDOM() throws XMLDBException {
		Document doc = null;
		try {
			DocumentBuilder builder =
				DocumentBuilderFactory.newInstance().newDocumentBuilder();
			// there are only parsers for streams :(
			// this means we need a Thread here to push the String data
			// into a PipedOutputStream...
			PipedOutputStream out = new PipedOutputStream();
			PipedInputStream in = new PipedInputStream(out);
			// the PipeThread can only write the contents of the xml
			// variable to the output stream, so we only give it the
			// stream to write to
			PipeThread piper = new PipeThread(out);
			doc = builder.parse(in);
		} catch (Exception e) {
			// this includes: ParserConfigurationException,
			// SAXException, IOException
			throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.toString());
		}

		return(doc);
	}

	/**
	 * Sets the content of the Resource using a DOM Node as the source.
	 *
	 * @param content The new content value
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.INVALID_RESOURCE if the content value provided is
	 *  null.<br />
	 *  ErrorCodes.WRONG_CONTENT_TYPE if the content provided in not a
	 *  valid DOM Node.
	 */
	public void setContentAsDOM(org.w3c.dom.Node content) throws XMLDBException {
		if (content == null) throw
			new XMLDBException(ErrorCodes.INVALID_RESOURCE, "(null)");

		// we can't do anything yet
		throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "This Resource is immutable");
	}

	/**
	 * Allows you to use a ContentHandler to parse the XML data from the
	 * database for use in an application.
	 *
	 * @param handler the SAX ContentHandler to use to handle the
	 *                Resource content.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.INVALID_RESOURCE if the ContentHandler value provided
	 *  is null.
	 */
	public void getContentAsSAX(org.xml.sax.ContentHandler handler)
		throws XMLDBException
	{
		if (handler == null) throw
			new XMLDBException(ErrorCodes.INVALID_RESOURCE, "(null)");

		try {
			saxparser.setContentHandler(handler);
			// there are only parsers for streams :(
			// this means we need a Thread here to push the String data into
			// a PipedOutputStream...
			PipedOutputStream out = new PipedOutputStream();
			PipedInputStream in = new PipedInputStream(out);
			// the PipeThread can only write the contents of the xml
			// variable to the output stream, so we only give it the stream
			// to write to
			PipeThread piper = new PipeThread(out);
			saxparser.parse(new InputSource(in));
		} catch (Exception e) {
			// deliberately include exception name (SAXException or
			// IOException)
			throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.toString());
		}
	}

	/**
	 * Sets the content of the Resource using a SAX ContentHandler.
	 *
	 * @return a SAX ContentHandler that can be used to add content into
	 *         the Resource.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public org.xml.sax.ContentHandler setContentAsSAX() throws XMLDBException {
		throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "This Resource is immutable");
	}

	/**
	 * Sets a SAX feature that will be used when this XMLResource  is
	 * used to produce SAX events (through the getContentAsSAX()
	 * method).
	 *
	 * @param feature Feature name. Standard SAX feature names are
	 *                documented at http://sax.sourceforge.net/.
	 * @param value Set or unset feature
	 * @throws org.xml.sax.SAXNotRecognizedException
	 * @throws org.xml.sax.SAXNotSupportedException
	 */
	public void setSAXFeature(String feature, boolean value)
		throws org.xml.sax.SAXNotRecognizedException,
				org.xml.sax.SAXNotSupportedException
	{
		// a simple pass through the SAX parser
		saxparser.setFeature(feature, value);
	}

	/**
	 * Returns current setting of a SAX feature that will be used when
	 * this XMLResource is used to produce SAX events (through the
	 * getContentAsSAX() method)
	 *
	 * @param feature Feature name. Standard SAX feature names are
	 *                documented at http://sax.sourceforge.net/.
	 * @return whether the feature is set
	 * @throws org.xml.sax.SAXNotRecognizedException
	 * @throws org.xml.sax.SAXNotSupportedException
	 */
	public boolean getSAXFeature(String feature)
		throws org.xml.sax.SAXNotRecognizedException,
				org.xml.sax.SAXNotSupportedException
	{
		return(saxparser.getFeature(feature));
	}

	//== end interface XMLResource
	
	/**
	 * A PipeThread will write the current contents of the xml variable
	 * to the given PipedOutputStream.  The thread will immediately be
	 * started and after it has written all data, disconnect and die.
	 */
	private class PipeThread extends Thread {
		private OutputStream out;
		
		public PipeThread(PipedOutputStream out) {
			this.out = out;
			this.start();
		}

		public void run() {
			try {
				out.write(xml.getBytes());
				out.flush();
				out.close();
			} catch (IOException e) {
				// just die
			}
		}
	}
}

