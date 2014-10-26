var monetdb = require('../monetdb');
var assert = require('assert');

var dbport = parseInt(process.argv[2]);
var dbname = process.argv[3];

/* lets first check some failing connection attempts */
monetdb.connect({host:'veryinvalidhostnamethathopefullyresolvesnowhere'}, function(err) {
	assert(err);
});

monetdb.connect({dbname:'nonexist', port:dbport}, function(err) {
	assert(err);
});

monetdb.connect({dbname:dbname, user:'nonexist', port:dbport}, function(err) {
	assert(err);
});

/* now actually connect */
var conn = monetdb.connect({dbname:dbname, port:dbport, debug: false}, function(err) {
	assert.equal(null, err);
	assert.equal(conn.env.gdk_dbname, dbname);
});


/* some querying, call chaining */
conn.query('start transaction').
	query('create table foo(a int, b float, c clob)').
	query("insert into foo values (42,4.2,'42'),(43,4.3,'43'),(44,4.4,'44'),(45,4.5,'45')");

conn.query('select * from foo', function(err, res) {
	assert.equal(null, err);

	assert.equal('table', res.type);
	assert.equal(4, res.rows);
	assert.equal(3, res.cols);

	assert.equal(3, res.structure.length);

	assert.equal('a', res.structure[0].column);
	assert.equal('int', res.structure[0].type);

	assert.equal(4, res.data.length);

	assert.equal(42, res.data[0][res.structure[0].index]);
	assert.equal(4.3, res.data[1][res.col['b']]);
	assert.equal('44', res.data[2][2]);
});

/* we can also just put the queries in one .query call */
conn.query('delete from foo; drop table foo; rollback');

/* query that will stress multi-block operations */
function rep(str,n) {
	ret = '';
	for (var i = 0; i< n; i++) {
		ret += str;
	}
	return ret;
}

var longstr = rep('ABCDEFGHIJKLMNOP', 100000);
conn.query("SELECT '" + longstr + "'", function(err, res) {
	assert.equal(null, err);
	assert.equal(longstr,res.data[0][0]);
});

/* failing query */
conn.query('MEHR BIER', function(err, res) {
	assert(err);
});

/* prepared statements */
conn.query('MEHR BIER',
	['connections', 0, false], function(err, res) {
		assert(err);
}); 


/* fire-and-forget query with parameters */
conn.query('SELECT id from tables where name=? and type=? and readonly=?',
	['connections', 0, false], function(err, res) {
		assert.equal(null, err);
		assert(res.rows > 0);

}); 

/* prepared statements can also be re-used  */
conn.prepare('SELECT id from tables where name=? and type=? and readonly=?', function(err, res){
	assert.equal(null, err);

	/* parameters can also be given as array */
	res.exec(['connections', 0, false], function(err, res) {
		assert.equal(null, err);
		assert(res.rows > 0);
	});

	/* this fails due to missing param */
	res.exec([0, false], function(err, res) {
		assert(err);
	});

	res.release();
});

conn.close();



