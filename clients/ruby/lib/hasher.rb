# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

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
