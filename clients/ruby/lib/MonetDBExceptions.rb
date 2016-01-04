# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

# Exception classes for the ruby-monetdb driver

class MonetDBQueryError < StandardError
  def initialize(e)
    $stderr.puts e
  end
end

class MonetDBDataError < StandardError
  def initialize(e)
    $stderr.puts e
  end
end

class MonetDBCommandError < StandardError
  def initialize(e)
    $stderr.puts e
  end
end

class MonetDBConnectionError < StandardError
  def initialize(e)
    $stderr.puts e
  end
end


class MonetDBSocketError < StandardError
  def initialize(e)
    $stderr.puts e
  end
end

class MonetDBProtocolError < StandardError
   def initialize(e)
     $stderr.puts e
   end
end
