
package Mapi;

/*
This exception is thrown by Mapi indicating an error in the Monet/java
communication. 
MapiException can be constructed with or without an exception message.
*/

public
class MapiException extends Exception {
    public MapiException() {
        super();
    }

    public MapiException(String s) {
        super(s);
    }

    public String toString() {
        String message = getMessage();
        return (message != null) ? (message) : "";
    }
}

