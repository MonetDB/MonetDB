/* the main function you want to use: */
function XRPC(posturl,    /* Your XRPC server. Usually: "http://yourhost:yourport/xrpc" */ 
              module,     /* module namespace (logical) URL. Must match XQuery module definition! */
              moduleurl,  /* module (physical) at-hint URL. Module file must be here! */
              method,     /* method name (matches function name in module) */
              call,       /* one or more XRPC_CALL() requests (concatenated strings) */ 
              callback)   /* callback function to call with the XML response */
{
    clnt.sendReceive(posturl, method, XRPC_REQUEST(module,moduleurl,method,call), callback);
}

/**********************************************************************
          functions to construct valid XRPC soap requests
 ***********************************************************************/

function XRPC_REQUEST(module, moduleurl, method, body) {
    var r = '<?xml version="1.0" encoding="utf-8"?>\n' +
           '<env:Envelope ' +
           'xmlns:env="http://www.w3.org/2003/05/soap-envelope" ' +
           'xmlns:xrpc="http://monetdb.cwi.nl/XQuery" ' +
           'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ' +
           'xsi:schemaLocation="http://monetdb.cwi.nl/XQuery http://monetdb.cwi.nl/XQuery/XRPC.xs" ' +
           'xmlns:xs="http://www.w3.org/2001/XMLSchema">' +
           '<env:Body>' +
               '<xrpc:request xrpc:module="' + module + '" ' +
                'xrpc:location="' + moduleurl + '" '+
                'xrpc:method="' + method + '">' + 
           body 
           + '</xrpc:request></env:Body></env:Envelope>';
    return r;
}

/* a body consist of one or more calls */
function XRPC_CALL(parameters) {
   if (parameters == null || parameters == "") return '<xrpc:call/>' 
   return '<xrpc:call>'+ parameters + '</xrpc:call>';
}

/* the call parameters are sequences, separated by a ',' */
function XRPC_SEQ(sequence) {
    if (sequence == null || sequence == "") return '<xrpc:sequence/>' 
    return  '<xrpc:sequence>' + sequence + '</xrpc:sequence>';
}

/* sequence values are either atomics of a xs:<TYPE> or elements */
function XRPC_ATOM(type, value) {
    return  '<xrpc:atomic-value xsi:type="xs:' + type + '">' + value + '</xrpc:atomic-value>';
}
function XRPC_ELEMENT(type, value) {
    return  '<xrpc:element>' + value + '</xrpc:element>';
}
/* omitted: document, attribute, comment and PI-typed values (*is* also possible!)*/


/**********************************************************************
          functions to shield from different browser flavors
 ***********************************************************************/

function serializeXML(xml) {
    try {
        return xml.xml;
    } catch(e){
        try {
            var xmlSerializer = new XMLSerializer();
            return xmlSerializer.serializeToString(xml);
        } catch(e){
            alert("Failed to create xmlSerializer or to serialize XML document:\n" + e);
        }
    }
}

XRPCWebClient = function () {
    if (window.XMLHttpRequest) {
        this.xmlhttp = new XMLHttpRequest();
    } else if (window.ActiveXObject) {
        try {
            this.xmlhttp = new ActiveXObject("Msxml2.XMLHTTP");
        } catch(e) {
            this.xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
        }
    }
}

XRPCWebClient.prototype.sendReceive = function(posturl, method, request, callback) {
    try {
        this.xmlhttp.open("POST", posturl, true);
        this.xmlhttp.send(request);

        var app = this;
        this.xmlhttp.onreadystatechange = function() {
            if (app.xmlhttp.readyState == 4 ) {
                if (app.xmlhttp.status == 200 &&
                    app.xmlhttp.responseText.indexOf("!ERROR") < 0 && 
                    app.xmlhttp.responseText.indexOf("<env:Fault>") < 0) 
                {
                    callback(app.xmlhttp.responseXML);
                } else {
                    var errmsg =
                        '!ERROR: "' + method + ' execution failed at the remote side"\n\n' +
                        '!ERROR: HTTP/1.1 ' + app.xmlhttp.status + '\n' +
                        '!ERROR: HTTP Response:\n\n\t' + app.xmlhttp.responseText;
                    alert(errmsg);
                    return null;
                }
            }
        };
    } catch (e) {
        alert('sendRequest('+method+'): '+e);
    }
}
