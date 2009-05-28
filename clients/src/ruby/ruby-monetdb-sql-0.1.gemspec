
Gem::Specification.new do |s|
   s.required_ruby_version = '>= 1.8.0'
   s.name = %q{ruby-monetdb-sql}
   s.version = "0.1"
   s.date = %q{2009-04-27}
   s.authors = ["G Modena"]
   s.email = %q{gm@cwi.nl}
   s.summary = %q{Pure Ruby database driver for monetdb5/sql}
   s.homepage = %q{http://monetdb.cwi.nl/}
   s.description = %q{Pure Ruby database driver for monetdb5/sql}
   s.files = Dir["README"] + Dir['lib/MonetDB.rb'] + Dir['lib/MonetDBConnection.rb'] + Dir['lib/MonetDBData.rb'] + Dir['lib/MonetDBExceptions.rb'] + Dir['lib/hasher.rb']
   s.has_rdoc = true
   s.require_path = Dir['./lib']
end
