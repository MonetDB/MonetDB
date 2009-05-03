require 'digest/md5'
require 'digest/sha1'

class Hasher
	# Constructor
	# method = "SHA1" or "MD5"
	# pwd = Password
	def initialize(method, pwd)
		if (method.upcase == "SHA1")
			@hashfunc = Digest::SHA1.new
			@hashname = "SHA1"
		else
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
