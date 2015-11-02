gem "minitest"
require 'minitest/autorun'
require_relative '../lib/MonetDB'

describe MonetDB do
	before do
		portnumber = ARGV[0] || 50000
		dbname = ARGV[1] || "mTests_."

		@database_connection = MonetDB.new
		@database_connection.connect(user = "monetdb", passwd = "monetdb", lang = "sql", host="127.0.0.1", port = portnumber, database_connection_name = dbname, auth_type = "SHA1")

		@database_connection.query("CREATE TABLE testtable (id INTEGER, integertest INTEGER, stringtest CHARACTER LARGE OBJECT)")
		@database_connection.query("INSERT INTO testtable (id, integertest, stringtest) VALUES (1, 42, 'foo')");
		@database_connection.query("INSERT INTO testtable (id, integertest, stringtest) VALUES (2, 43, 'bar baz')");
		
		@query_result = @database_connection.query("SELECT * FROM testtable t");
	end

	after do
		@database_connection.query("DROP TABLE testtable");
	end

	describe "Monet DB interface" do
		it " can fetch all result items from a query in a hash" do
			@query_result.fetch_all_as_column_hash.must_equal({"id"=>[1, 2], "stringtest"=>["foo", "bar baz"], "integertest"=>[42, 43]})
		end

		it " can fetch a record from a query in a hash" do
			@query_result.fetch_hash.must_equal({"id"=>1, "stringtest"=>"foo", "integertest"=>42})
		end

		it " can iterate through all records in hash" do
			result_collection = []
			@query_result.each_record_as_hash { |record| result_collection.push(record) }
			result_collection.must_equal([{ "id" => 1, "stringtest" => "foo", "integertest" => 42 }, { "id" => 2, "stringtest" => "bar baz", "integertest" => 43 }])
		end

		it " can fetch a result record" do
			@query_result.fetch.must_equal([1, 42, "foo"])
		end

		it " can fetch all records" do
			@query_result.fetch_all.must_equal([[1, 42, "foo"], [2, 43, "bar baz"]])
		end

		it " can count the number of resulting rows from a query" do
			@query_result.num_rows.must_equal(2)
		end

		it " can count the number of resulting fields from a query" do
			@query_result.num_fields.must_equal(3)
		end

		it " can get the name of fields from a query" do
			@query_result.name_fields.must_equal(["id", "integertest", "stringtest"])
		end

		it " can get the type of fields from a query" do
			@query_result.type_fields.must_equal({"id"=>"int", "stringtest"=>"clob", "integertest"=>"int"})
		end

		it " can get all values by column name" do
			@query_result.fetch_by_column_name("id").must_equal([1, 2])
		end

		it " can iterate through all records" do
			result_collection = []
			@query_result.each_record { |record| result_collection.push(record) }
			result_collection.must_equal([[1, 42, "foo"], [2, 43, "bar baz"]])
		end
	end
end

# Overwrite some methods in minitest to prevent tests from failing over difference in time and seed reports
module Minitest
	class SummaryReporter
		def start
			super

			io.puts "# Running:"

			self.sync = io.respond_to? :"sync=" # stupid emacs
			self.old_sync, io.sync = io.sync, true if self.sync
		end

		def statistics
			# do nothing to prevent tests from failing over difference in time reports
		end
	end
end
