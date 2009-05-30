# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2009 MonetDB B.V.
# All Rights Reserved.

# Formats, handles and executes SQL/XQUERY statements.

class MonetDBStatement
  
  def initialize(connection = nil, lang = 'sql', version = '5')
    @connection = connection
    @lang = lang
    @version = version
  end
  
  # Send a query to a monetdb5 server
  def real_query(q)
    if @connection != nil
      @connection.send(format_command("reply size 1"))
      @connection.send(format_query(q))
      query = @connection.receive()
    else 
      raise MonetDBConnectionError, "Failed to perform query; connection not available"
    end
  end
  
  private
  # Formats a query <i>string</i> so that it can be parsed by the server
  def format_query(q)
    if @lang == @@LANG_SQL
        return "s" + q + ";\n"
    else
      raise LanguageNotSupported
    end
  end
  
  # Formats a <i>command</i> string so that it can be parsed by the server
  def format_command(x)
    return "X" + x + "\nX"
  end
end