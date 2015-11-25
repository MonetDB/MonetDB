# tests rubytest.rb & gemtest.rb are (and shoudl be kept!) identical,
# except the following two lines:

#require_relative '$RELSRCDIR/../lib/MonetDB' # rubytest.rb.in
 require                            'MonetDB' #  gemtest.rb

class MapiRubyInterfaceTestCase

	def test1(query_results)
		result = query_results.fetch_all_as_column_hash == {"id"=>[1, 2], "stringtest"=>["foo", "bar baz"], "integertest"=>[42, 43]}
		return result, "fetch all result items in a hash"
	end

	def test2(query_results)
		result = query_results.fetch_hash == {"id"=>1, "stringtest"=>"foo", "integertest"=>42}
		return result, "fetch a result record in a hash"
	end

	def test3(query_results)
		result_collection = []
		query_results.each_record_as_hash { |record| result_collection.push(record) }
		result = result_collection == [{ "id" => 1, "stringtest" => "foo", "integertest" => 42 }, { "id" => 2, "stringtest" => "bar baz", "integertest" => 43 }]

		return result, "iterate through all records in hash"
	end

	def test4(query_results)
		result = query_results.fetch == [1, 42, "foo"]
		return result, "fetch a result record"
	end

	def test5(query_results)
		result = query_results.fetch_all == [[1, 42, "foo"], [2, 43, "bar baz"]]
		return result, "fetch all records"
	end

	def test6(query_results)
		result = query_results.fetch_all == [[1, 42, "foo"], [2, 43, "bar baz"]] 
		return result, "count the number of resulting rows from a query"
	end

	def test7(query_results)
		result = query_results.num_fields == 3
		return result, "count the number of resulting fields from a query"
	end

	def test8(query_results)
		result = query_results.name_fields == ["id", "integertest", "stringtest"]
		return result, "get the name of fields from a query"
	end

	def test9(query_results)
		result = query_results.type_fields == {"id"=>"int", "stringtest"=>"clob", "integertest"=>"int"}
		return result, "can get the type of fields from a query"
	end

	def test10(query_results)
		result = query_results.type_fields == {"id"=>"int", "stringtest"=>"clob", "integertest"=>"int"}
		return result, "get the type of fields from a query"
	end

	def test11(query_results)
		result = query_results.fetch_by_column_name("id") == [1, 2]
		return result, "get all values by column name"
	end

	def test12(query_results)
		result_collection = []
		query_results.each_record { |record| result_collection.push(record) }
		result = result_collection == [[1, 42, "foo"], [2, 43, "bar baz"]]

		return result, "iterate through all records"
	end
end

class Tester

	def initialize(portnumber, dbname, testcase)
		@testcase = testcase
		@error_messages = []
		@database_connection = MonetDB.new
		@database_connection.connect(user = "monetdb", passwd = "monetdb", lang = "sql", host="127.0.0.1", port = portnumber, database_connection_name = dbname, auth_type = "SHA1")
	end

	def start
		print_initialize
		run_tests
		print_results
	end

	# ====================
			private 
	# ====================

	def run_tests
		@testcase.public_methods(false).each { |test| run_test(test) }
	end

	def run_test(test)
		create_database
		query_results = run_query
		result, message = @testcase.send(test, query_results)
		add_error(result, message)
		remove_database
	end

	def run_query
		return @database_connection.query("SELECT * FROM testtable t");
	end

	def create_database
		@database_connection.query("CREATE TABLE testtable (id INTEGER, integertest INTEGER, stringtest CHARACTER LARGE OBJECT)")
		@database_connection.query("INSERT INTO testtable (id, integertest, stringtest) VALUES (1, 42, 'foo')");
		@database_connection.query("INSERT INTO testtable (id, integertest, stringtest) VALUES (2, 43, 'bar baz')");
	end

	def remove_database
		@database_connection.query("DROP TABLE testtable");
	end

	def add_error(result, message="")
		@error_messages.push(message) unless result
	end

	def print_initialize
		puts ""
		puts "# Running tests"
		puts ""
	end

	def print_results
		puts "Tests Failed: #{@error_messages.size}"
		puts ""

		if( @error_messages.any? )
			puts "Errors:" 
			@error_messages.each_with_index { |error_message, index| puts "#{index + 1}: #{error_message}" }
		end
	end
end

portnumber = ARGV[0] || 50000
dbname = ARGV[1] || "mTests_."

tester = Tester.new(portnumber, dbname, MapiRubyInterfaceTestCase.new)
tester.start
