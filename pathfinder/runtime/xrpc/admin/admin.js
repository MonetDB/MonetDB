/* set the proper URLs for the admin GUI */
var protocol= "file:";
if (document.location.protocol != null && document.location.protocol != "")
   protocol = document.location.protocol

var port    = "";
if (document.location.port != null && document.location.port != "")
   port = ":" + document.location.port

var href    = protocol + "//" + document.location.hostname + port; 
var mod     = "http://monetdb.cwi.nl/XQuery/admin/";
var modurl  = href + "/admin/admin.xq";
var posturl = href + "/xrpc/admin";

function myXRPC(method,arity,call,callback) {
    XRPC(posturl,mod,modurl,method,arity,call,callback);
}

/**********************************************************************
          Callback functions of the admin functions in admin.xq
***********************************************************************/

function doCollectionsCallback(response) {
    var cols = response.getElementsByTagName("collection");
    var cTable = top.content.document.getElementById("div1");
    var cTableBody = "";

    if(cols.length == 0){
        cTable.innerHTML = '<h3>No (documents) collections in the database</h3>\n';
        return;
    } else if( (cols.length == 1) &&
               (cols[0].getAttribute("numDocs") == null) ){
        cTable.innerHTML = '<h3>No (documents) collections in the database</h3>\n';
        return;
    }

    var i;
    for(i = 0; i < cols.length; i++){
        var colName = cols[i].firstChild.nodeValue;
        var updatable = cols[i].getAttribute("updatable");
        var size = cols[i].getAttribute("size");
        var numDocs = cols[i].getAttribute("numDocs");

        if(numDocs != null) {
            if(updatable == null) updatable = "UNKNOWN";
            if(size == null) size = "UNKNOWN";

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
        cTableBody +
        '</table>\n';
}

function doAllDocumentsCallback(response) {
    var docs = response.getElementsByTagName("document");
    var dTable = top.content.document.getElementById("div1");
    var dTableBody = "";

    if(docs == null || docs.length == 0){
        dTable.innerHTML = '<h3>No documents in the database</h3>\n';
        return;
    }

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
            '<input type="button" name="viewDoc" value="view" onclick="open(\'/xrpc/doc/'+docName+'\',\'_blank\',\'scrollbars=yes,resizable=yes,directories=no,location=no,titlebar=no,toolbar=no\')"/>'+
            '</td>\n';
        dTableBody +=
            '<td>'+
            '<input type="button" name="delDoc" value="delete" onclick="top.doDelDoc(\''+docName+'\')" />'+
            '</td>\n';
        dTableBody += '</tr>\n';
    }

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
}

function doDocumentsCallback(response) {
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
            '<input type="button" name="viewDoc" value="view" onclick="open(\'/xrpc/doc/'+docName+'\',\'_blank\',\'scrollbars=yes,resizable=yes,directories=no,location=no,titlebar=no,toolbar=no\')"/>'+
            '</td>\n';
        dTableBody +=
            '<td>'+
            '<input type="button" name="delDoc" value="delete" onclick="top.doDelDoc(\''+docName+'\')" />'+
            '</td>\n';
        dTableBody += '</tr>\n';
    }

    var dTable = top.content.document.getElementById("div1");
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

function doDbStatsCallback(response){
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
}

function doDbEnvCallback(response){
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
        '<table width="30%" border="1">\n' +
        '<tr>\n' +
        '<th>Variable Name</th>\n' +
        '<th>Value</th>\n' +
        '</tr>\n' +
        dTableBody +
        '</table>\n';
}

function doCollections()  { 
    XRPC(posturl, mod, modurl, 'collections',  0, XRPC_CALL(), doCollectionsCallback); 
}

/* methods expected to take 'long' (use popup window) */
function asyncCallback(response, method) { 
    alert(method + " DONE!\n"); 
    top.doCollections();
}
function doAddDocCallback(response)      { asyncCallback(response, "Add Document"); }
function doDelDocCallback(response)      { asyncCallback(response, "Delete Document"); }
function doDelColCallback(response)      { asyncCallback(response, "Delete Collection"); }
function doBackupColCallback(response)   { asyncCallback(response, "Backup Collection"); }
function doRestoreColCallback(response)  { asyncCallback(response, "Restore Collection"); }
function doBackupCallback(response)      { asyncCallback(response, "Backup"); }
function doRestoreCallback(response)     { asyncCallback(response, "Restore"); }

/* expected to return 'immediately' */
function reportCallback(response, method) { 
    var dTable = top.content.document.getElementById("div1");
    dTable.innerHTML = serializeXML(response);
}


/**********************************************************************
          Implementation of the functions defined in admin.xq
***********************************************************************/

function doDocuments(colName) 
                          { myXRPC('documents',  1, XRPC_CALL(XRPC_SEQ(XRPC_ATOM('string', colName))), doDocumentsCallback); }
function doAllDocuments() { myXRPC('documents',  0, XRPC_CALL(), doAllDocumentsCallback); }
function doDbStats()      { myXRPC('db-stats',   0, XRPC_CALL(), doDbStatsCallback); }
function doDbEnv()        { myXRPC('db-env',     0, XRPC_CALL(), doDbEnvCallback); }

function doBackupCol()    { alert("Sorry, function doBackupCol() not implemented yet"); }
function doRestoreCol()   { alert("Sorry, function doRestoreCol() not implemented yet"); }

function doAddDoc() {
    var url =   top.content.document.getElementById("newURL").value;
    var name =  top.content.document.getElementById("newName").value;
    var col =   top.content.document.getElementById("newCol").value;
    var perct = top.content.document.getElementById("newFree").value;
    myXRPC('add-doc', 4, XRPC_CALL(XRPC_SEQ(XRPC_ATOM('string', url)) +
                                   XRPC_SEQ(XRPC_ATOM('string', name)) +
                                   XRPC_SEQ(XRPC_ATOM('string', col)) +
                                   XRPC_SEQ(XRPC_ATOM('integer', perct))), doAddDocCallback);
    var dTable = top.content.document.getElementById("div1");
    dTable.innerHTML = dTable.innerHTML + '<b>Started shredding '+ url + '...</b>';
}

function doDelDoc(docName) {
    if (!window.confirm('Are you sure you want to delete the document "' + docName + '"?')) return;
    myXRPC('del-doc', 1, XRPC_CALL(XRPC_SEQ(XRPC_ATOM('string', docName))), doDelDocCallback);
}

function doDelCol(colName) {
    if (!window.confirm('Are you sure you want to delete the collection "' + colName + '" and all documents in it?')) return;
    myXRPC('del-col', 1, XRPC_CALL(XRPC_SEQ(XRPC_ATOM('string', colName))), doDelColCallback);
}

function doBackup(){
    var id = top.content.document.getElementById("ID").value;
    myXRPC('backup', 1, XRPC_CALL(XRPC_SEQ(XRPC_ATOM('string', id))), doBackupCallback);
    top.content.document.getElementById("div1").innerHTML = "Backup in progress..";
}

function doRestore(){
    var id =    top.content.document.getElementById("ID").value;
    var perct = top.content.document.getElementById("dfltFree").value;

    myXRPC('restore', 2, XRPC_CALL(XRPC_SEQ(XRPC_ATOM('string', id)) + 
                                XRPC_SEQ(XRPC_ATOM('integer', perct))), doRestoreCallback);
    top.content.document.getElementById("div1").innerHTML = "Restore in progress..";
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
}
