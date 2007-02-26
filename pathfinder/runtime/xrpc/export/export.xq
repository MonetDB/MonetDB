module namespace export = 'http://monetdb.cwi.nl/XQuery/export/';

(: Standard EXPORT module, by Ying Zhang (zhang@cwi.nl)
 :
 : - makes it possible to retrieve an XML document from the database
 :   using a URL of the form:
                    http://<host>[:port]/xrpc/<name>.xml
 : - standard part of the MonetDB/XQuery distribution 
 : - can only be called from IPs listed in xrpc_trusted (MonetDB.conf option)
 :)

(: =================== HTTP =================== :)

declare function export:GET($uri as xs:string) 
{ doc($uri) };
