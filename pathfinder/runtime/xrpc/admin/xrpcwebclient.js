/* the main function you want to use: */
function XRPC(posturl,    /* Your XRPC server. Usually: "http://yourhost:yourport/xrpc" */ 
              module,     /* module namespace (logical) URL. Must match XQuery module definition! */
              moduleurl,  /* module (physical) at-hint URL. Module file must be here! */
              method,     /* method name (matches function name in module) */
              arity,      /* arity of the method */
              call,       /* one or more XRPC_CALL() parameter specs (concatenated strings) */ 
              callback)   /* callback function to call with the XML response */
{
    clnt.sendReceive(posturl, method, XRPC_REQUEST(module,moduleurl,method,arity,call), callback);
}

/**********************************************************************
          functions to construct valid XRPC soap requests
 ***********************************************************************/

function XRPC_REQUEST(module, moduleurl, method, arity, body) {
    var r = '<?xml version="1.0" encoding="utf-8"?>\n' +
           '<env:Envelope ' +
           'xmlns:env="http://www.w3.org/2003/05/soap-envelope" ' +
           'xmlns:xrpc="http://monetdb.cwi.nl/XQuery" ' +
           'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ' +
           'xsi:schemaLocation="http://monetdb.cwi.nl/XQuery http://monetdb.cwi.nl/XQuery/XRPC.xsd" ' +
           'xmlns:xs="http://www.w3.org/2001/XMLSchema">' +
           '<env:Body>' +
               '<xrpc:request xrpc:module="' + module + '" ' +
                'xrpc:location="' + moduleurl + '" ' +
                'xrpc:method="' + method + '" ' +
                'xrpc:arity="' + arity + '">' + 
           body 
           + '</xrpc:request></env:Body></env:Envelope>';
    return r;
}

/* a body consists of one or more calls */
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
    if (type == 'string') value = value.xmlEscape(1);
    return  '<xrpc:atomic-value xsi:type="xs:' + type + '">' + value + '</xrpc:atomic-value>';
}
function XRPC_ELEMENT(value) {
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

/**********************************************************************
        for XRPC string parameters, we need to perform some escaping 
 ***********************************************************************/

String.prototype.xmlEscape = function(direction)
{
  var nums = new Array (         '\001', '\002', '\003', '\004', '\005', '\006', '\007',
                         '\010', '\011', '\012', '\013', '\014', '\015', '\016', '\017',
                         '\020', '\021', '\022', '\023', '\024', '\025', '\026', '\027',
                         '\030', '\031', '\032', '\033', '\034', '\035', '\036', '\037');
  var octals = new Array (            '\\001', '\\002', '\\003', '\\004', '\\005', '\\006', '\\007',
                             '\\010', '\\011', '\\012', '\\013', '\\014', '\\015', '\\016', '\\017',
                             '\\020', '\\021', '\\022', '\\023', '\\024', '\\025', '\\026', '\\027',
                             '\\030', '\\031', '\\032', '\\033', '\\034', '\\035', '\\036', '\\037');


  var chars = new Array ('&','à','á','â','ã','ä','å','æ','ç','è','é',
                         'ê','ë','ì','í','î','ï','ð','ñ','ò','ó','ô',
                         'õ','ö','ø','ù','ú','û','ü','ý','þ','ÿ','À',
                         'Á','Â','Ã','Ä','Å','Æ','Ç','È','É','Ê','Ë',
                         'Ì','Í','Î','Ï','Ð','Ñ','Ò','Ó','Ô','Õ','Ö',
                         'Ø','Ù','Ú','Û','Ü','Ý','Þ','€','\"','ß','<',
                         '>','¢','£','¤','¥','¦','§','¨','©','ª','«',
                         '¬','­','®','¯','°','±','²','³','´','µ','¶',
                         '·','¸','¹','º','»','¼','½','¾');

  var entities = new Array ('amp','agrave','aacute','acirc','atilde','auml','aring',
                            'aelig','ccedil','egrave','eacute','ecirc','euml','igrave',
                            'iacute','icirc','iuml','eth','ntilde','ograve','oacute',
                            'ocirc','otilde','ouml','oslash','ugrave','uacute','ucirc',
                            'uuml','yacute','thorn','yuml','Agrave','Aacute','Acirc',
                            'Atilde','Auml','Aring','AElig','Ccedil','Egrave','Eacute',
                            'Ecirc','Euml','Igrave','Iacute','Icirc','Iuml','ETH','Ntilde',
                            'Ograve','Oacute','Ocirc','Otilde','Ouml','Oslash','Ugrave',
                            'Uacute','Ucirc','Uuml','Yacute','THORN','euro','quot','szlig',
                            'lt','gt','cent','pound','curren','yen','brvbar','sect','uml',
                            'copy','ordf','laquo','not','shy','reg','macr','deg','plusmn',
                            'sup2','sup3','acute','micro','para','middot','cedil','sup1',
                            'ordm','raquo','frac14','frac12','frac34');

  newString = this;
  if (direction == 1) {
    for (var i = 0; i < nums.length; i++) {
      var myRegExp = new RegExp();
      myRegExp.compile(nums[i],'g');
      newString = newString.replace (myRegExp, octals[i]);
    }
    for (var i = 0; i < chars.length; i++) {
      var myRegExp = new RegExp();
      myRegExp.compile(chars[i],'g');
      newString = newString.replace (myRegExp, '&' + entities[i] + ';');
    }
  } else {
    for (var i = nums.length - 1; i >= 0; i--) {
      var myRegExp = new RegExp();
      myRegExp.compile(octals[i],'g');
      newString = newString.replace (myRegExp, nums[i]);
    }
    for (var i = chars.length - 1; i >= 0; i--) {
      var myRegExp = new RegExp();
      myRegExp.compile('&' + entities[i] + ';','g');
      newString = newString.replace (myRegExp, chars[i]);
    }
  }
  return newString;
}
