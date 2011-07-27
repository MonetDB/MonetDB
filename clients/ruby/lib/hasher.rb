# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
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
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

require 'digest/md5'
require 'digest/sha1'
require 'digest/sha2' 

class Hasher
	# Constructor
	# method = "SHA1" or "MD5"
	# pwd = Password
	def initialize(method, pwd)
          if (method.upcase == "SHA1")
                  @hashfunc = Digest::SHA1.new
                  @hashname = method.upcase
          elsif (method.upcase == "SHA256") 
            @hashfunc = Digest::SHA256.new
            @hashname = method.upcase
          elsif (method.upcase == "SHA384")
            @hashfunc = Digest::SHA384.new
            @hashname = method.upcase
          elsif (method.upcase == "SHA512")
            @hashfunc = Digest::SHA512.new
            @hashname = method.upcase
          else
            # default to MD5
                  @hashfunc = Digest::MD5.new
                  @hashname = "MD5"
          end
          @pwd = pwd
  end
  

	def hashname
		@hashname
	end

	# Compute hash code
	def hashsum
		return @hashfunc.hexdigest(@pwd)
	end
end
