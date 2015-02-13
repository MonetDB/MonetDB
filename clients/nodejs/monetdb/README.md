# monetdb
This package defines a mapiclient that allows easy communication between a NodeJS application and a running MonetDB server process.

# Installation
npm install [-g] monetdb

# Example usage:

```
var options = {
	host     : 'localhost', 
	port     : 50000, 
	dbname   : 'mydb', 
	user     : 'monetdb', 
	password : 'monetdb'
};

var conn = require('monetdb').connect(options , function(err) {
	if (!err) console.log('connected');
});

conn.query('SELECT 1', function(err, res) {
	if (!err) console.log(res);
});

conn.query('SELECT id FROM tables WHERE name=? and readonly=?', 
	['connections', false], function(err, res) {
	if (!err) console.log(res);
});

conn.close();
```

# API

#### <a name="connect"></a>connect(options, fn)
Obtain a MonetDBConnection object that tries to connect to a MonetDB server process using the provided options.

- options [object]: Options to use for this connection. Possible options are:
	- host [string]: hostname to connect to (default: localhost)
	- port [integer]: port on which the MonetDB server process is listening (default: 50000)
	- dbname [string]: name of the database to use (default: demo)
	- user [string]: username to use for logging in (default: monetdb)
	- password [string]: password to use for logging in (default: monetdb)
	- language [string]: query language to use (default: sql)
	- debug [boolean]: whether or not to initialize the connection in debug node (default: false)
	- q [function]: in case you want to use promises, you should pass in the result of require('q') in here. See [Q Integration](#q) for more information on our Q integration.
- fn [function]: Callback function to call whenever the connection succeeds or fails. This function receives one argument, which contains an error string on failure or null on success. Note: this function might be called multiple times when the MonetDB connection fails after it first succeeded. In that case, the callback will first be called indicating success, and later again with an error message.

Returns an instance of a MonetDBConnection object. Note that thanks to a queueing system, this object can immediately receive queries when it is returned, even if the connection is not established yet.

---

#### <a name="query"></a>MonetDBConnection.query(query, [params], [raw], fn)
Run a query against this MonetDBConnection

- query [string]: A query string in the chosen language, where values may be replaced by question marks, in which case the params array must be set (note: when the params array is given, the MonetDBConnection.prepare will be used under the hood).
- params [array]: In case question marks are given for some values in the query, this array supplies the query method with the values to fill in. (default: null)
- raw [boolean]: If set to true, a raw query is expected, including an initial 's' and a trailing ';'. (default: false)
- fn [function]: Callback function that will be called when the query completes. It then receives two arguments:
	- err: Error string if an error occurred, null if the query succeeded
	- result: The [query result](#result)

---

#### <a name="result"></a>Query result
A query returns a query result object. This object has the following properties:

- data [array]: When a SELECT query was done, this array contains the resulting data from the database. It is passed as an array of arrays, where every inner array represents one data tuple.
- col [object]: Object that maps column names to query result indices. So if you for example did SELECT a, b FROM ... you can access b in a tuple array by issuing tuple[result.col.b], which in this case would resolve to tuple[1].
- rows [integer]: The number of rows in the result set
- cols [integer]: The number of columns in the result set
- structure [array]: An array containing an object for every column, with information about the column (like from which table it came, what its name is etc.)
- queryid [integer]: A unique identifier for this query
- type: The type of the result (currently only 'table' is supported)

---

#### MonetDBConnection.prepare(query, fn)
Prepares a query for repeated execution.

- query [string]: The query that has to be prepared. In this query, actual values can be replaced by question marks.
- fn [function]: Callback function that will be called when the preparation finished. It gets passed two arguments. The first argument is an error string or null on success, the second argument is an object with the following properties:
	- prepare: The [query result](#result) of the prepare statement
	- exec: A function that you can call if you want to execute your prepared statement. It takes two parameters, the first one is an array with the query parameters and the second is a callback that gets executed when the execution of the prepared statement finishes (same as callback to [MonetDBConnection.query](#query)).
	- release: A function that you can call when you want to free the resources used by your prepared statement. After calling this function, calls to the exec function will fail.

---

#### MonetDBConnection.close(fn)
Empty the query queue and then close the connection. Note that whenever queries arrive after the close has been issued, these queries will also be processed before the connection is closed. The connection really is not closed before it encounters an empty message queue.
- fn [function]: Callback function that will be called when the connection is closed. Since failure never happens, it will always be called with its first argument set to null. The second argument is never given, since there is no result associated with closing the connection.



# <a name="log"></a>Query logging
Every time a query result is returned from the database, the callback function *conn.log_callback*, which defaults to null, will be executed right before the query callback function is executed (conn refers to a connection object returned by the [connect function](#connect)). If you have some general behavior that you want to be executed every time a query finishes, you can set *conn.log_callback* to a function with the following signature:

function(query, err, res)
- query: the SQL query that lead to this result (note: if you pass a params array to the [query function](#query), multiple queries are fired to the database, hence the log callback will be called multiple times)
- err, res: Same as in the callback for the [query function](#query)


# <a name="q"></a>Q Integration
Due to the huge popularity of the [Q module for NodeJS](https://www.npmjs.org/package/q), we decided to add native Q support, that wraps our API in a promise returning API that exists on top of the original API, so you can use both interchangeably. 

We did not want to add Q as a dependency of our module and therefore, if you want to use the promise returning functions, you have to pass in the result of require('q') to the connection options when calling the [connect function](#connect).

All of the promise returning functions take the same arguments as the original functions they wrap, except for the callback argument.

The promise returning functions are:
- connectQ: Gets resolved with the connection object when the connection has been successfully initialized and gets rejected if the connection could not be established. Note that, since promises can by definition only be resolved once, failure of the connection after the promise has been resolved can not be detected using this function (you can detect this with the [original connect function](#connect).
- queryQ
- prepareQ
- closeQ

### Example usage of Q integration
```
var conn = null;
monetdb.connectQ({dbname:"blaeu"}).then(function(c) {
    conn = c;
    return conn.queryQ("CREATE TABLE coords (x INT, y INT)").then(function() {
        return conn.queryQ("INSERT INTO coords VALUES (1, 1), (2, 5), (9, 1)");
    }).then(function() {
        return conn.queryQ("SELECT * FROM coords WHERE x < ? AND y < ?", [4, 5]);
    }).then(function(data) {
        console.log(data);
    });
}).fail(function(err) {
    console.log("Something went wrong: "+err);
}).fin(function() {
    conn && conn.closeQ().then(function() {
    	console.log("Connection successfully closed!");
    });
}).done();
```
