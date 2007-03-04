var ADMIN_CALLBACK = "mxqadmin";
var XRPC_SERVER_ADDR = (new String(document.location)).replace("admin",ADMIN_CALLBACK);

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

XRPCWebClient.prototype.sendReceive = function(method, request, callback) {
    try {
        this.xmlhttp.open("POST", XRPC_SERVER_ADDR, true);
        this.xmlhttp.send(request);

        var app = this;
        this.xmlhttp.onreadystatechange = function() {
            if (app.xmlhttp.readyState == 4 ) {
                if (app.xmlhttp.status == 200) {
                    callback(app.xmlhttp.responseXML);
                } else {
                    var errmsg =
                        '!ERROR: failed to execute "' + method + '"\n\n' +
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
