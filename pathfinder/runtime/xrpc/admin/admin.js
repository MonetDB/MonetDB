var MODULE = "http://monetdb.cwi.nl/XQuery/admin/";
var LOCATION = document.location + "admin.xq";
var REQ_HEADER = '<?xml version="1.0" encoding="utf-8"?>\n' +
                   '<env:Envelope ' +
                       'xmlns:env="http://www.w3.org/2003/05/soap-envelope" ' +
                       'xmlns:xrpc="http://monetdb.cwi.nl/XQuery" ' +
                       'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ' +
                       'xsi:schemaLocation="http://monetdb.cwi.nl/XQuery ' +
                                           'http://monetdb.cwi.nl/XQuery/XRPC.xsd" ' +
                       'xmlns:xs="http://www.w3.org/2001/XMLSchema">' +
                     '<env:Body>' +
                       '<xrpc:request xrpc:module="' + MODULE + '" ' +
                                     'xrpc:location="' + LOCATION + '" ';
var REQ_FOOTER = '</xrpc:request></env:Body></env:Envelope>';
var ATOM_STR = '<xrpc:atomic-value xsi:type="xs:string">';
var ATOM_END = '</xrpc:atomic-value>';

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

/**********************************************************************
          Callback functions of the admin functions in admin.xq
***********************************************************************/
function doCollectionsCallback(response) {
    if(response == null){
        alert('Execution of "collections()" failed');
        return;
    }

    var cols = response.getElementsByTagName("collection");
    var cTable = top.content.document.getElementById("div1");
    var cTableBody = "";


    if(cols.length == 0){
        cTable.innerHTML =
            '<h3>No (documents) collections in the database</h3>\n';
        return;
    }

    var i;
    for(i = 0; i < cols.length; i++){
        var colName = cols[i].firstChild.nodeValue;
        var updatable = cols[i].getAttribute("updatable");
        var size = cols[i].getAttribute("size");
        var numDocs = cols[i].getAttribute("numDocs");

        if(updatable == null) updatable = "UNKNOWN";
        if(size == null) size = "UNKNOWN";
        if(numDocs == null) numDocs = "UNKNOWN";

        cTableBody += '<tr>\n';
        cTableBody += '<td>'+colName+'</td>\n';
        cTableBody += '<td>'+updatable+'</td>\n';
        cTableBody += '<td>'+size+'</td>\n';
        cTableBody += '<td>'+numDocs+'</td>\n';
        cTableBody +=
            '<td>'+
            '<input type="button" name="viewCol" value="view" onclick="top.doDocuments(\''+ colName + '\')" />'+
            '</td>\n';
        cTableBody +=
            '<td>'+
            '<input type="button" name="delCol" value="delete" onclick="top.doDelCol(\'' + colName+ '\')" />'+
            '</td>\n';
        cTableBody += '</tr>\n';
    }

    cTable.innerHTML = 
        '<h2>All collections in the database:</h2>\n' +
        '<table width="30%" border="1">\n' +
        '<tr>\n' +
        '<th>Name</th>\n' +
        '<th>Updatable</th>\n' +
        '<th>Size</th>\n' +
        '<th>#Docs</th>\n' +
        '<th>&nbsp;</th>\n' +
        '<th>&nbsp;</th>\n' +
        '</tr>\n' +
        cTableBody + response.xml + 
        '</table>\n';
    top.content.document.getElementById("div2").innerHTML = "";
}

function doAllDocumentsCallback(response) {
    if(response == null){
        alter('Execution of "documents()" failed');
        return;
    }

    var docs = response.getElementsByTagName("document");
    var dTableBody = "";

    var i;
    for(i = 0; i < docs.length; i++){
        var docName   = docs[i].firstChild.nodeValue;
        var cName     = docs[i].getAttribute("collection");
        var updatable = docs[i].getAttribute("updatable");
        var url       = docs[i].getAttribute("url");

        if(cName == null) cName = "UNKNOWN";
        if(updatable == null) updatable = "UNKNOWN";
        if(url == null) url = "UNKNOWN";

        dTableBody += '<tr>\n';
        dTableBody += '<td>'+docName+'</td>\n';
        dTableBody += '<td>'+cName+'</td>\n';
        dTableBody += '<td>'+updatable+'</td>\n';
        dTableBody += '<td>'+url+'</td>\n';
        dTableBody +=
            '<td>'+
            '<input type="button" name="viewDoc" value="view" onclick="open(\'/'+docName+'\',\'_blank\',\'directories=no,location=no,titlebar=no,toolbar=no\')"/>'+
            '</td>\n';
        dTableBody +=
            '<td>'+
            '<input type="button" name="delDoc" value="delete" onclick="top.doDelDoc(\''+docName+'\')" />'+
            '</td>\n';
        dTableBody += '</tr>\n';
    }

    var dTable = top.content.document.getElementById("div1");
    dTable.innerHTML = 
        '<h2>All documents in the database:</h2>\n' +
        '<table width="30%" border="1">\n' +
        '<tr>\n' +
        '<th>Name</th>\n' +
        '<th>Collection</th>\n' +
        '<th>Updatable</th>\n' +
        '<th>URL</th>\n' +
        /*
        '<th>&nbsp;</th>\n' +
        */
        '<th>&nbsp;</th>\n' +
        '</tr>\n' +
        dTableBody +
        '</table>\n';

    top.content.document.getElementById("div2").innerHTML = "";
}

function doDocumentsCallback(response) {
    if(response == null){
        alter('Execution of "documents()" failed');
        return;
    }

    var docs = response.getElementsByTagName("document");
    var cName = docs[0].getAttribute("collection");
    var dTableBody = "";

    var i;
    for(i = 0; i < docs.length; i++){
        var docName   = docs[i].firstChild.nodeValue;
        var updatable = docs[i].getAttribute("updatable");
        var url       = docs[i].getAttribute("url");

        if(updatable == null) updatable = "UNKNOWN";
        if(url == null) url = "UNKNOWN";

        dTableBody += '<tr>\n';
        dTableBody += '<td>'+docName+'</td>\n';
        dTableBody += '<td>'+updatable+'</td>\n';
        dTableBody += '<td>'+url+'</td>\n';
        dTableBody +=
            '<td>'+
            '<input type="button" name="viewDoc" value="view" onclick="open(\'/'+docName+'\',\'_blank\',\'directories=no,location=no,titlebar=no,toolbar=no\')"/>'+
            '</td>\n';
        dTableBody +=
            '<td>'+
            '<input type="button" name="delDoc" value="delete" onclick="top.doDelDoc(\''+docName+'\')" />'+
            '</td>\n';
        dTableBody += '</tr>\n';
    }

    var dTable = top.content.document.getElementById("div2");
    dTable.innerHTML = 
        '<h2>Documents in the collection \"' + cName + '\"</h2>\n' +
        '<table width="30%" border="1">\n' +
        '<tr>\n' +
        '<th>Name</th>\n' +
        '<th>Updatable</th>\n' +
        '<th>URL</th>\n' +
        '<th>&nbsp;</th>\n' +
        '<th>&nbsp;</th>\n' +
        '</tr>\n' +
        dTableBody +
        '</table>\n';
}

function doAddDocCallback(response){
    if(response != null) {
        alert("Shred document DONE!\n"+
              "Please reload the collections list to view result");
    }
}

function doDelDocCallback(response){
    if(response != null) {
        alert("Delete document DONE!\n"+
              "Please reload the documents list to view result");
    }
}

function doDelColCallback(response){
    if(response != null) {
        alert("Delete collection DONE!\n"+
              "Please reload the collections list to view result");
    }
}

function doBackupColCallback(response){
    alert("Sorry, function doBackupColCallback() not implemented yet");
}

function doRestoreColCallback(response){
    alert("Sorry, function doRestoreColCallback() not implemented yet");
}

function doBackupCallback(response){
    if(response != null) {
        alert("Backup DONE!\n");
    } 
}

function doRestoreCallback(response){
    if(response != null) {
        alert("Restore DONE!\n");
    } 
}

function doDbStatsCallback(response){
    if(response == null){
        alter('Execution of "db-stats()" failed');
        return;
    }

    var buns = response.getElementsByTagName("bun");
    var dTableBody = "";

    var i;
    for(i = 0; i < buns.length; i++){
        var statName = buns[i].getAttribute("head");
        var statVal = buns[i].getAttribute("tail");

        if( statName != null && statVal != null) {
            if(statName == "") statName = "&nbsp;";
            if(statVal == "") statVal = "&nbsp;";
            dTableBody += '<tr>\n';
            dTableBody += '<td>'+statName+'</td>\n';
            dTableBody += '<td>'+statVal+'</td>\n';
            dTableBody += '</tr>\n';
        }
    }

    var dTable = top.content.document.getElementById("div1");
    dTable.innerHTML = 
        '<h2>Database statistics:</h2>\n' +
        '<table width="30%" border="1">\n' +
        '<tr>\n' +
        '<th>Name</th>\n' +
        '<th>Value</th>\n' +
        '</tr>\n' +
        dTableBody +
        '</table>\n';
    top.content.document.getElementById("div2").innerHTML = "";
}

function doDbEnvCallback(response){
    if(response == null){
        alter('Execution of "db-env()" failed');
        return;
    }

    var buns = response.getElementsByTagName("bun");
    var dTableBody = "";

    var i, configPath;
    for(i = 0; i < buns.length; i++){
        var envVar = buns[i].getAttribute("head");
        var envVarVal = buns[i].getAttribute("tail");

        if( envVar != null && envVarVal != null) {
            if(envVar == "") envVar = "&nbsp;";
            if(envVarVal == "") envVarVal = "&nbsp;";
            if(envVar == "config") configPath = envVarVal;
            dTableBody += '<tr>\n';
            dTableBody += '<td>'+envVar+'</td>\n';
            dTableBody += '<td>'+envVarVal+'</td>\n';
            dTableBody += '</tr>\n';
        }
    }

    var dTable = top.content.document.getElementById("div1");
    dTable.innerHTML = 
        '<h2>MonetDB environment variables:</h2>\n' +
        '<p>The complete MonetDB configuration file "MonetDB.conf" ' +
           'can be found on the servers local system via the path "' +
           configPath +
        '"</p>\n' +
        '<table width="30%" border="1">\n' +
        '<tr>\n' +
        '<th>Variable Name</th>\n' +
        '<th>Value</th>\n' +
        '</tr>\n' +
        dTableBody +
        '</table>\n';
    top.content.document.getElementById("div2").innerHTML = "";
}

function doDbFlushCallback(response){
    if(response == null){
        alter('Execution of "db-flush()" failed');
        return;
    }

    var dTable = top.content.document.getElementById("div1");
    dTable.innerHTML = serializeXML(response);
    top.content.document.getElementById("div2").innerHTML = "";
}

function doDbRestartCallback(response){
    if(response == null){
        alter('Execution of "db-restart()" failed');
        return;
    }

    var dTable = top.content.document.getElementById("div1");
    dTable.innerHTML = serializeXML(response);
    top.content.document.getElementById("div2").innerHTML = "";
}

function doDbCheckpointCallback(response){
    if(response == null){
        alter('Execution of "db-checkpoint()" failed');
        return;
    }

    var dTable = top.content.document.getElementById("div1");
    dTable.innerHTML = serializeXML(response);
    top.content.document.getElementById("div2").innerHTML = "";
}

/**********************************************************************
          Implementation of the functions defined in admin.xq
***********************************************************************/
function doCollections() {
    var xrpcRequest = REQ_HEADER +
                      'xrpc:method="collections"><xrpc:call/>'
                      + REQ_FOOTER;
    clnt.sendReceive("collections", xrpcRequest, doCollectionsCallback);
}

function doAllDocuments() {
    var xrpcRequest = REQ_HEADER +
        'xrpc:method="documents">' +
        '<xrpc:call/>' +
        REQ_FOOTER;
    clnt.sendReceive("documents", xrpcRequest, doAllDocumentsCallback);
}

function doDocuments(colName) {
    var xrpcRequest = REQ_HEADER +
        'xrpc:method="documents">' +
        '<xrpc:call><xrpc:sequence>' +
        ATOM_STR + '"'+ colName + '"'+ ATOM_END +
        '</xrpc:sequence></xrpc:call>' +
        REQ_FOOTER;
    clnt.sendReceive("documents", xrpcRequest, doDocumentsCallback);
}

function doAddDoc() {
    var url = top.content.document.getElementById("newURL").value;
    var name = top.content.document.getElementById("newName").value;
    var col = top.content.document.getElementById("newCol").value;
    var perct = top.content.document.getElementById("newFree").value;

    var xrpcRequest = REQ_HEADER+
        'xrpc:method="add-doc">'+
        '<xrpc:call>'+
          '<xrpc:sequence>'+
            '<xrpc:atomic-value xsi:type="xs:string">"'+url+'"</xrpc:atomic-value>'+
          '</xrpc:sequence>'+
          '<xrpc:sequence>'+
            '<xrpc:atomic-value xsi:type="xs:string">"'+name+'"</xrpc:atomic-value>'+
          '</xrpc:sequence>'+
          '<xrpc:sequence>'+
            '<xrpc:atomic-value xsi:type="xs:string">"'+col+'"</xrpc:atomic-value>'+
          '</xrpc:sequence>'+
          '<xrpc:sequence>'+
            '<xrpc:atomic-value xsi:type="xs:integer">'+perct+'</xrpc:atomic-value>'+
          '</xrpc:sequence>'+
        '</xrpc:call>'+
        REQ_FOOTER;
alert(xrpcRequest);
    clnt.sendReceive("add-doc", xrpcRequest, doAddDocCallback);
}

function doDelDoc(docName) {
    var confirmMsg =
        'Are you sure you want to delete the document "' +
        docName + '"?';
    if(!window.confirm(confirmMsg)) return;

    var xrpcRequest = REQ_HEADER +
        'xrpc:method="del-doc">' +
        '<xrpc:call><xrpc:sequence>'+
            ATOM_STR + '"' + docName + '"' + ATOM_END +
        '</xrpc:sequence></xrpc:call>' +
        REQ_FOOTER;
    clnt.sendReceive("del-doc", xrpcRequest, doDelDocCallback);
}

function doDelCol(colName) {
    var confirmMsg =
        'Are you sure you want to delete the collection "' +
        colName + '" and all documents in it?';
    if(!window.confirm(confirmMsg)) return;

    var xrpcRequest = REQ_HEADER +
        'xrpc:method="del-col">' +
        '<xrpc:call>' +
          '<xrpc:sequence>'+
            '<xrpc:atomic-value xsi:type="xs:string">"'+colName+'"</xrpc:atomic-value>'+
          '</xrpc:sequence>'+
        '</xrpc:call>' +
        REQ_FOOTER;
    clnt.sendReceive("del-col", xrpcRequest, doDelColCallback);
}

function doBackupCol(){
    alert("Sorry, function doBackupCol() not implemented yet");
}

function doRestoreCol(){
    alert("Sorry, function doRestoreCol() not implemented yet");
}

function doBackup(){
    var id = top.content.document.getElementById("ID").value;

    var xrpcRequest = REQ_HEADER+
        'xrpc:method="backup">'+
        '<xrpc:call>'+
          '<xrpc:sequence>'+
            '<xrpc:atomic-value xsi:type="xs:string">"'+id+'"</xrpc:atomic-value>'+
          '</xrpc:sequence>'+
        '</xrpc:call>'+
        REQ_FOOTER;
    clnt.sendReceive("backup", xrpcRequest, doBackupCallback);
}

function doRestore(){
    var id = top.content.document.getElementById("ID").value;
    var perct = top.content.document.getElementById("dfltFree").value;

    var xrpcRequest = REQ_HEADER+
        'xrpc:method="restore">'+
        '<xrpc:call>'+
          '<xrpc:sequence>'+
            '<xrpc:atomic-value xsi:type="xs:string">"'+id+'"</xrpc:atomic-value>'+
          '</xrpc:sequence>'+
          '<xrpc:sequence>'+
            '<xrpc:atomic-value xsi:type="xs:integer">'+perct+'</xrpc:atomic-value>'+
          '</xrpc:sequence>'+
        '</xrpc:call>'+
        REQ_FOOTER;
    clnt.sendReceive("restore", xrpcRequest, doRestoreCallback);
}

function doDbStats(){
    var xrpcRequest = REQ_HEADER +
                      'xrpc:method="db-stats"><xrpc:call/>'
                      + REQ_FOOTER;
    clnt.sendReceive("db-stats", xrpcRequest, doDbStatsCallback);
}

function doDbEnv(){
    var xrpcRequest = REQ_HEADER +
                      'xrpc:method="db-env"><xrpc:call/>'
                      + REQ_FOOTER;
    clnt.sendReceive("db-env", xrpcRequest, doDbEnvCallback);
}

function doDbFlush(){
    var xrpcRequest = REQ_HEADER +
                      'xrpc:method="db-flush"><xrpc:call/>'
                      + REQ_FOOTER;
    clnt.sendReceive("db-flush", xrpcRequest, doDbFlushCallback);
}

function doDbRestart(){
    var xrpcRequest = REQ_HEADER +
                      'xrpc:method="db-restart"><xrpc:call/>'
                      + REQ_FOOTER;
    clnt.sendReceive("db-restart", xrpcRequest, doDbRestartCallback);
}

function doDbCheckpoint(){
    var xrpcRequest = REQ_HEADER +
                      'xrpc:method="db-checkpoint"><xrpc:call/>'
                      + REQ_FOOTER;
    clnt.sendReceive("db-checkpoint", xrpcRequest, doDbCheckpointCallback);
}

function doPUT() {
    alert("Sorry, function not implemented yet");
}

function makeAddDocForm(){
    var dTable = top.content.document.getElementById("div1");

    dTable.innerHTML =
        '<h2>Shred an XML document into the database:</h2>\n' +
        '<table>\n' +
          '<tr>\n' +
            '<td>Original URL:</td>\n' +
            '<td><input type="file" id="newURL" value=""/></td>\n' +
          '</tr>\n' +
          '<tr>\n' +
            '<td>New Name:</td>\n' +
            '<td><input type="text" id="newName" value=""/></td>\n' +
          '</tr>\n' +
          '<tr>\n' +
            '<td>Collection:</td>\n' +
            '<td><input type="text" id="newCol" value=""/></td>\n' +
          '</tr>\n' +
          '<tr>\n' +
            '<td>Free %:</td>\n' +
            '<td><input type="text" id="newFree" value="0"/></td>\n' +
          '</tr>\n' +
          '<tr>\n' +
            '<td colspan="2" align="center">\n' +
              '<input type="button" name="addDoc" value="Add" onClick="top.doAddDoc()"/>\n' +
            '</td>\n' +
          '</tr>\n' +
        '</table>\n';

    top.content.document.getElementById("div2").innerHTML = "";
}

function makeBackupForm(){
    var dTable = top.content.document.getElementById("div1");

    dTable.innerHTML =
        '<h2>Backup The Database</h2>\n' +
        '<table>\n' +
          '<tr>\n' +
            '<td>Backup ID</td>\n' +
            '<td><input type="text" id="ID" value=""/></td>\n' +
          '</tr>\n' +
          '<tr>\n' +
            '<td colspan="2" align="center">\n' +
              '<input type="button" name="backup" value="Backup" onClick="top.doBackup()"/>\n' +
            '</td>\n' +
          '</tr>\n' +
        '</table>\n';
    top.content.document.getElementById("div2").innerHTML = "";
}

function makeRestoreForm(){
    var dTable = top.content.document.getElementById("div1");

    dTable.innerHTML =
        '<h2>Restore The Database</h2>\n' +
        '<table>\n' +
          '<tr>\n' +
            '<td>Backup ID</td>\n' +
            '<td><input type="text" id="ID" value=""/></td>\n' +
          '</tr>\n' +
          '<tr>\n' +
            '<td>Default Free %:</td>\n' +
            '<td><input type="text" id="dfltFree" value="10"/></td>\n' +
          '</tr>\n' +
          '<tr>\n' +
            '<td colspan="2" align="center">\n' +
              '<input type="button" name="restore" value="Restore" onClick="top.doRestore()"/>\n' +
            '</td>\n' +
          '</tr>\n' +
        '</table>\n';
    top.content.document.getElementById("div2").innerHTML = "";
}
