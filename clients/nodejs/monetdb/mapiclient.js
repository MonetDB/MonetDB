var net    = require('net');
var crypto = require('crypto');

function MonetDBConnection(options, conncallback) {
	this.state = 'new';
	this.options = options;	
	this.read_leftover = 0;
	this.read_final = false;
	this.read_str = '';
	this.read_callback = undefined;
	this.conn_callback = conncallback;
	this.mapi_blocksize = 8192;

	this.queryqueue = [];
	var thizz = this;
	this.socket = net.connect(options.port, options.host, function() {
		thizz.state = 'connected';
	});
	this.socket.on('data', function(data) {
		thizz.handleInput(data);
	});
	this.socket.on('end', function() {
	  	thizz.state = 'disconnected';
	});
	this.socket.on('error', function(x) {
		if (conncallback != undefined)
			conncallback({'success':false, 'message':x.toString()});
	});
	/* some setup */
	this.request('Xreply_size -1', undefined, true);
	this.request('Xauto_commit 1', undefined, true);
	/* get server environment into connector */
	this.request('SELECT * FROM env()', function(x) {
		thizz.env = {};
		x.data.forEach(function(l) { 
			thizz.env[l.name] = l.value;
		 });
	});
	this.request('SELECT 42', function(x) {
		if (this.conn_callback != undefined)
			this.conn_callback({'success':true, 'message':'ok'});
	});
}

MonetDBConnection.prototype.request = 
MonetDBConnection.prototype.query = function(message, callback, raw) {
	if (!raw) {
		message = 's'+message+';';
	}
	this.queryqueue.push({'message' : message , 'callback' : callback})
}


MonetDBConnection.prototype.handleMessage = function(message) {
	if (this.options.debug)
		console.log('RX ['+this.state+']: '+message);

	/* prompt, good */
	if (message == '') {
		this.state = 'ready';
		this.nextOp();
		return;
	}

	/* monetdbd redirect, ignore. We will get another challenge soon */
	if (message.charAt(0) == '^') {
		return;
	}

	if (this.state == 'connected') {
		/* error message during authentication? */
		if (message.charAt(0) == '!') {
			message = 'Error: '+message.substring(1,message.length-1);
			if (this.conn_callback != undefined)
				this.conn_callback({'success':false, 'message':message});
			return;
		}

		// means we get the challenge from the server
		var authch = message.split(':');
		var salt   = authch[0];
		var dbname = authch[1];
		var pwhash = __sha512(__sha512(this.options.password) + salt)
		var response = 'LIT:' + this.options.user + ':{SHA512}' + pwhash + ':' +
			this.options.language + ':' + this.options.dbname + ':';
		this.sendMessage(response);
		return;
	}

	var response = {};

	/* error message */
	if (message.charAt(0) == '!') {
		response.success = false;
		response.error = message.substring(1);
	}

	/* query result */
	if (message.charAt(0) == '&') {
		response = _parseresponse(message);
		response.success = true;
	}

	if (this.read_callback != undefined) {
		this.read_callback(response);
		this.read_callback = undefined;
	}
	this.nextOp();	
}


MonetDBConnection.prototype.nextOp = function() {
	if (this.queryqueue.length < 1) {
		return;
	}
	var op = this.queryqueue.shift();
	this.sendMessage(op.message);
	this.read_callback = op.callback;
}	

MonetDBConnection.prototype.handleInput = function(data) {
	/* we need to read a header obviously */
	if (this.read_leftover == 0) {
		var hdr = data.readUInt16LE(0);
		this.read_leftover = (hdr >> 1);
		this.read_final = (hdr & 1) == 1;
		data = data.slice(2);
	}
	if (this.options.debug) 
		console.log('reading ' + this.read_leftover + ' bytes, final=' + this.read_final);

	/* what is in the buffer is not necessary the entire block */
	var read_cnt = Math.min(data.length, this.read_leftover);
	this.read_str = this.read_str + data.toString('utf8', 0, read_cnt);
	this.read_leftover -= read_cnt;

	/* if there is something left to read, we will be called again */
	if (this.read_leftover > 0) {
		return;
	}

	/* pass on reassembled messages */
	if (this.read_leftover == 0 && this.read_final) {
		this.handleMessage(this.read_str);
		this.read_str = '';
	}

	/* also, the buffer might contain more blocks or parts thereof */
	if (data.length > read_cnt) {
		var leftover = new Buffer(data.length - read_cnt);
		data.copy(leftover, 0, read_cnt, data.length);
		this.handleInput(leftover);
	}

};

MonetDBConnection.prototype.sendMessage = function(message) {
	if (this.options.debug) 
		console.log('TX: '+message);

	var buf = new Buffer(message, 'utf8');
	var final = 0;
	while (final == 0) {
		var bs = Math.min(buf.length, this.mapi_blocksize - 2)
		var sendbuf = buf.slice(0, bs);
		buf = buf.slice(bs + 1);
		if (buf.length == 0) {
			final = 1;
		}

		if (this.options.debug)
			console.log('writing ' + bs + ' bytes, final=' + final);

		var hdrbuf = new Buffer(2);
		hdrbuf.writeInt16LE((bs << 1) | final, 0);
		this.socket.write(Buffer.concat([hdrbuf, sendbuf]));
	}
}

/* In theory, the server tells us which hashes he likes. 
   In practice, we know he always likes sha512 , so... */
function __sha512(str) {
	return crypto.createHash('sha512').update(str).digest('hex');
}


function _parsetuples(names, types, lines) {
	var state = 'INCRAP';
	var resultarr = [];
	for (li in lines) {
		var line = lines[li];
		var resultline = {};
		var tokenStart = 2;
		var endQuote = 0;
		var valPtr = '';
		var cCol = 0;

		/* mostly adapted from clients/R/MonetDB.R/src/mapisplit.c */
		for (var curPos = tokenStart; curPos < line.length - 1; curPos++) {
			var chr = line.charAt(curPos);
			switch (state) {
			case 'INCRAP':
				if (chr != '\t' && chr != ',') {
					tokenStart = curPos;
					if (chr == '"') {
						state = 'INQUOTES';
						tokenStart++;
					} else {
						state = 'INTOKEN';
					}
				}
				break;
			case 'INTOKEN':
				if (chr == ',' || curPos == line.length - 2) {
					var tokenLen = curPos - tokenStart - endQuote;
					valPtr = line.substring(tokenStart, tokenStart + tokenLen);
					if (tokenLen < 1 || valPtr == 'NULL') {
						resultline[names[cCol]] = undefined;

					} else {
						switch(types[cCol]) {
							case 'boolean':
								resultline[names[cCol]] = valPtr == 'true';
								break;
							case 'tinyint':
							case 'smallint':
							case 'int':
							case 'wrd':
							case 'bigint':
								resultline[names[cCol]] = parseInt(valPtr);
								break
							case 'real':
							case 'double':
							case 'decimal':
								resultline[names[cCol]] = parseFloat(valPtr);
								break
							default:
								resultline[names[cCol]] = valPtr;
								break;
						}
					}
					cCol++;
					endQuote = 0;
					state = 'INCRAP';
				}
				break;
			case 'ESCAPED':
				state = 'INQUOTES';
				break;
			case 'INQUOTES':
				if (chr == '"') {
					state = 'INTOKEN';
					endQuote++;
					break;
				}
				if (chr == '\\') {
					state = 'ESCAPED';
					break;
				}
				break;
			}
		}
		resultarr.push(resultline);
	}
	return resultarr;
}

function _hdrline(line) {
	return line.substr(2, line.indexOf('#')-3).split(',\t'); 
}

function _parseresponse(msg) {
	var lines = msg.split('\n');
	var resp = {};
	var tpe = lines[0].charAt(1);

	/* table result, we only like Q_TABLE for now */
	if (tpe == 1) { 
		var hdrf = lines[0].split(" ");

		resp.type='table';
		resp.queryid   = parseInt(hdrf[1]);
		resp.rows = parseInt(hdrf[2]);
		resp.cols = parseInt(hdrf[3]);

		var table_names  = _hdrline(lines[1]);
		var column_names = _hdrline(lines[2]);
		var column_types = _hdrline(lines[3]);
		var type_lengths = _hdrline(lines[4]);

		resp.structure = [];
		for (var i = 0; i < table_names.length; i++) {
			resp.structure.push({
				table : table_names[i],
				column : column_names[i],
				type : column_types[i],
				typelen : parseInt(type_lengths[i])
			});
		}
		resp.data = _parsetuples(column_names, column_types, lines.slice(5, lines.length-1));
	}
	return resp;
}

MonetDBConnection.prototype.close = function() {
	var thizz = this;
	/* kills the connection after the query has been processed (will also wait for all others) */
	this.request('SELECT 42', function(x) {
		thizz.socket.destroy();
	});
}

exports.connect = exports.open = function() {
  return new MonetDBConnection(getConnectArgs(arguments[0]), arguments[1]);
}

function __check_arg(options, argname, type, dflt) {
	var argval = options[argname];
	options[argname] = dflt;
	
	if (typeof argval === 'undefined') {
		return;
	}
	if (typeof argval !== type) {
		console.warn('parameter ' + argname + ' should be ' + type + ' but is ' + typeof argval);
		return;
	}

	if (type == 'string') {
		if (typeof argval != 'string') {
			return;
		}
		argval = argval.trim();
		if (argval == '') {
			console.warn('parameter ' + argname + ' is empty');
			return;
		}
		options[argname] = argval;
	} else {
		options[argname] = argval;
	}
}

function getConnectArgs(options) {
	__check_arg(options, 'dbname'  , 'string' , 'demo');
	__check_arg(options, 'user'    , 'string' , 'monetdb');
	__check_arg(options, 'password', 'string' , 'monetdb');
	__check_arg(options, 'host'    , 'string' , 'localhost');
	__check_arg(options, 'port'    , 'int'    , 50000);
	__check_arg(options, 'language', 'string' , 'sql');
	__check_arg(options, 'debug'   , 'boolean', false);
  return options;
}

