Gem::Specification.new do |s|
   s.required_ruby_version = '>= 1.8.0'
   s.name = %q{activerecord-monetdb-adapter}
   s.version = "0.1"
   s.date = %q{2009-05-18}
   s.authors = ["G Modena"]
   s.email = "info@monetdb.org"
   s.licenses = ["MPL-2.0"]
   s.summary = %q{ActiveRecord Connector for MonetDB}
   s.homepage = %q{http://www.monetdb.org/}
   s.description = %q{ActiveRecord Connector for MonetDB built on top of the pure Ruby database driver}
   s.files = [ "lib/active_record/connection_adapters/monetdb_adapter.rb" ]
   s.has_rdoc = true
   s.require_path = 'lib'
   s.add_dependency(%q<activerecord>, [">= 2.3.2"])
   s.add_dependency(%q<ruby-monetdb-sql>, [">= 0.1"])
   # placeholder project to avoid warning about not having a rubyforge_project
   s.rubyforge_project = "nowarning"
end
