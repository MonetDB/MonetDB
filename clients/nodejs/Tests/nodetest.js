var monetdb = require('../monetdb');
var assert = require('assert');

var dbport = parseInt(process.argv[2]);
var dbname = process.argv[3];

/* lets first check some failing connection attempts */
monetdb.connect({host:'veryinvalidhostnamethathopefullyresolvesnowhere'}, function(resp) {
	assert.equal(false,resp.success);
	assert(resp.message.trim().length > 0);
});

monetdb.connect({dbname:'nonexist', port:dbport}, function(resp) {
	assert.equal(false,resp.success);
	assert(resp.message.trim().length > 0);
});

monetdb.connect({dbname:dbname, user:'nonexist', port:dbport}, function(resp) {
	assert.equal(false,resp.success);
	assert(resp.message.trim().length > 0);
});

/* now actually connect */
var conn = monetdb.connect({dbname:dbname, port:dbport}, function(resp) {
	assert.equal(true,resp.success);
});


/* some querying */
conn.query('start transaction');

conn.query('create table foo(a int, b float, c clob)');
conn.query("insert into foo values (42,4.2,'42'),(43,4.3,'43'),(44,4.4,'44'),(45,4.5,'45')");

conn.query('select * from foo', function(res) {
	assert.equal(true, res.success);

	assert.equal('table', res.type);
	assert.equal(4, res.rows);
	assert.equal(3, res.cols);

	assert.equal(3, res.structure.length);

	assert.equal('a', res.structure[0].column);
	assert.equal('int', res.structure[0].type);

	assert.equal(4, res.data.length);

	assert.equal(42, res.data[0][res.structure[0].index]);
	assert.equal(4.3, res.data[1][1]);
	assert.equal('44', res.data[2][2]);
});

conn.query('delete from foo; drop table foo; rollback');

/* query that will force multi-block operations */
function rep(str,n) {
	ret = '';
	for (var i = 0; i< n; i++) {
		ret += str;
	}
	return ret;
}
var longstr = rep('ABCDEFGHIJKLMNOP',10000);

conn.query("SELECT '"+longstr+"'", function(res) {
	assert.equal(true, res.success);
	assert.equal(longstr,res.data[0][0]);

});

/* failing query */
conn.query('MEHR BIER', function(res) {
	assert.equal(false,res.success);
	assert(res.message.trim().length > 0);
});

conn.close();