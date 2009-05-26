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

# Information relative to a MonetDB connection

require 'socket'
require 'time'
require 'hasher'
require 'MonetDBExceptions'


class MonetDBConnection
  MSG_REDIRECT          = '^' # redirect through merovingian
  MSG_INFO              = '!'
  
  MAX_AUTH_ITERATION    = 10  # maximum number of atuh iterations (thorough merovingian) allowed
  
  # enable debug output
  @@DEBUG               = false

  # hour in seconds, used for timezone calculation
  @@HOUR                = 3600

  # maximum size (in bytes) for a monetdb message to be sent
  @@MAX_FB_SIZE         = 100
  @@MAX_SB_SIZE         = 0
  @@MAX_MESSAGE_SIZE    = @@MAX_FB_SIZE + @@MAX_SB_SIZE
  
  # endianness of a message sent to the server
  @@CLIENT_ENDIANNESS   = "BIG"
  
  # MAPI protocols supported by the driver
  @@SUPPORTED_PROTOCOLS = [ 8 ]
  
  attr_reader :socket
  
  # Instantiates a new MonetDBConnection object
  # * user: username (default is monetdb)
  # * passwd: password (default is monetdb)
  # * lang: language (default is sql) 
  # * host: server hostanme or ip  (default is localhost)
  # * port: server port (default is 50000)
  
  def initialize(user = "monetdb", passwd = "monetdb", lang = "sql", host="127.0.0.1", port = 50000)
    @user = user
    @passwd = passwd
    @lang = lang.downcase
    @host = host
    @port = port

    @client_endianness = @@CLIENT_ENDIANNESS
    
    @auth_iteration = 0
    @connection_established = false
  end
  
  
  
  # Connect to the database, creates a new socket
  def connect(db_name = 'demo', auth_type = 'SHA1')
    @database = db_name
    @auth_type = auth_type
    @socket = TCPSocket.new(@host, @port)  
    real_connect
  end
  
  
  def real_connect
    server_challenge = retrieve_server_challenge()
#    puts @database
    if server_challenge != ""
      salt = server_challenge.split(':')[0]
      @server_name = server_challenge.split(':')[1]
      @protocol = server_challenge.split(':')[2].to_i
      @supported_auth_types = server_challenge.split(':')[3].split(',')
      @server_endianness = server_challenge.split(':')[4]
      if @protocol == 9
        @pwhash = server_challenge.split(':')[5]
      end
    end
    
    # The server supports only RIPMED168 or crypt as an authentication hash function, but the driver does not.
    if @supported_auth_types.length == 1
      auth = @supported_auth_types[0]
      if auth.upcase == "RIPEMD160" or auth.upcase == "CRYPT"
        raise MonetDBConnectionError, auth.upcase + " " + ": algorithm not supported by ruby-monetdb."
      end
    end
    
    
    # If the server protocol version is not 8: abort and notify the user.
    if @@SUPPORTED_PROTOCOLS.include?(@protocol) == false
      raise MonetDBProtocolError, "Protocol not supported. The current implementation of ruby-monetdb works with MAPI protocols #{@@SUPPORTED_PROTOCOLS} only."
    
    elsif mapi_proto_v8?
      reply = build_auth_string_v8(@auth_type, salt, @database)
    elsif mapi_proto_v9?
      reply = build_auth_string_v9(@auth_type, salt, @database)
    end
             
    if @socket != nil
      @connection_established = true

      encode_message(reply).each do |msg|
        @socket.write(msg)
      end

      f, monetdb_auth = recv_decode_hdr
            
      if monetdb_auth == 0
        # auth succedeed, now set the timezone and proceed
        set_timezone
      else
        block = @socket.recv(monetdb_auth)
#         puts "Block:" + block
        if block[0].chr == MSG_REDIRECT
        #redirection
          if merovingian?
            if @auth_iteration <= 10
              @auth_iteration += 1
              real_connect
            else
              raise MonetDBConnectionError, "Merovingian: too many iterations while proxying."
            end
          elsif mserver?
          # reinitialize a connection
            @socket.close
            connect(@database, @auth_type)
          else
            @connection_established = false
            raise MonetDBQueryError, @socket.recv(monetdb_auth)
          end
        elsif block[0].chr == MSG_INFO
          raise MonetDBConnectionError, block
        end
      end
    end
  end

  # Disconnect from server
  def disconnect()
    if  @connection_established 
      begin
        @socket.close
      rescue => e
        $stderr.print e
      end
    else
      $stderr.print "No connection established."
    end
  end
  
  # send a message to monetdb5 server
  def send(msg)
    encode_message(msg).each do |m|
      @socket.write(m)
    end
    is_final, chunk_size = recv_decode_hdr
    
    return chunk_size
  end
  
  # receive a response from monetdb5 server
  def receive(is_final, chunk_size)
    response = @socket.recv(chunk_size)
    
    while is_final != true
      is_final, chunk_size = recv_decode_hdr
      response +=  @socket.recv(chunk_size)
    end
    
    return response
  end
  
  # receive a response from monetdb5 server one line at a time (lines are terminated by '\n')
  def receive_line()
    return @socket.readline
  end
  
  #
  # Builds and authentication string given the parameters submitted by the user (MAPI protocol v8).
  # 
  def build_auth_string_v8(auth_type, salt, db_name)
    # seed = password + salt
    if (auth_type.upcase == "MD5" or auth_type.upcase == "SHA1") and @supported_auth_types.include?(auth_type.upcase)
      auth_type = auth_type.upcase
      digest = Hasher.new(auth_type, @passwd+salt)
      hashsum = digest.hashsum
    elsif auth_type.downcase == "plain" or not  @supported_auth_types.include?(auth_type.upcase)
      auth_type = 'plain'
      hashsum = @passwd + salt
      
    elsif auth_type.downcase == "crypt"
      auth_type =  @supported_auth_types[@supported_auth_types.index(auth_type)+1]
      $stderr.print "The selected hashing algorithm is not supported by the Ruby driver. #{auth_type} will be used instead."
      digest = Hasher.new(auth_type, @passwd+salt)
      hashsum = digest.hashsum
    else
      # The user selected an auth type not supported by the server.
      raise MonetDBConnectionError, "#{auth_type} not supported by the server. Please choose one from #{@supported_auth_types}"
      
    end    
    # Build the reply message with header
    reply = @client_endianness + ":" + @user + ":{" + auth_type + "}" + hashsum + ":" + @lang + ":" + db_name 
  end

  #
  # Builds and authentication string given the parameters submitted by the user (MAPI protocol v9).
  # 
  def build_auth_string_v9(auth_type, salt, db_name)
    if (auth_type.upcase == "MD5" or auth_type.upcase == "SHA1") and @supported_auth_types.include?(auth_type.upcase)
      auth_type = auth_type.upcase
      # Hash the password
      pwhash = Hasher.new(@pwhash, @passwd)
      
      digest = Hasher.new(auth_type, pwhash.hashsum + salt)
      hashsum = digest.hashsum
        
    elsif auth_type.downcase == "plain" # or not  @supported_auth_types.include?(auth_type.upcase)
      # Keep it for compatibility with merovingian
      auth_type = 'plain'
      hashsum = @passwd + salt
    elsif @supported_auth_types.include?(auth_type.upcase)
      if auth_type.upcase == "RIPEMD160"
        auth_type =  @supported_auth_types[@supported_auth_types.index(auth_type)+1]
        $stderr.print "The selected hashing algorithm is not supported by the Ruby driver. #{auth_type} will be used instead."
      end
      # Hash the password
      pwhash = Hasher.new(@pwhash, @passwd)
        
      digest = Hasher.new(auth_type, pwhash.hashsum + salt)
      hashsum = digest.hashsum  
    else
      # The user selected an auth type not supported by the server.
      raise MonetDBConnectionError, "#{auth_type} not supported by the server. Please choose one from #{@supported_auth_types}"
    end    
    # Build the reply message with header
    reply = @client_endianness + ":" + @user + ":{" + auth_type + "}" + hashsum + ":" + @lang + ":" + db_name 
  end


  # builds a message to be sent to the server
  def encode_message(query = "")
    message = Array.new
   
    if @client_endianness == @@CLIENT_ENDIANNESS
      
      message_size = query.length    
      data_delimiter_begin = 0
      if message_size <= @@MAX_MESSAGE_SIZE
        data_delimiter_end = message_size
      else
        data_delimiter_end = @@MAX_MESSAGE_SIZE
      end
      
      is_final = false
      
      if @@DEBUG
      puts "q.lenght" + message_size.to_s
      end
      i = 0
      while is_final != true
        if message_size <= @@MAX_MESSAGE_SIZE 
        # the query fits one packet
          if message_size < @@MAX_FB_SIZE
            fb =  message_size
            sb = 0
          else
            fb = @@MAX_FB_SIZE
            sb = message_size - fb   
          end
          fb = fb << 1
          fb = fb | 00000001
         
          data_delimiter_end = data_delimiter_begin + message_size
          message << fb.chr + sb.chr + query[data_delimiter_begin...data_delimiter_end]
        
      
          is_final = true

          if @@DEBUG
            puts "fits\n"
            puts "iter" + i.to_s
            puts "begin" + data_delimiter_begin.to_s
            puts "end" + data_delimiter_end.to_s
            puts "msg" + message_size.to_s
          end
        else
          
          fb = @@MAX_FB_SIZE
          sb = @@MAX_SB_SIZE
          
          fb = (fb << 1) | 00000000
          if @@DEBUG
            puts "bigger than max \n"
            puts "iter" + i.to_s
            puts "begin" + data_delimiter_begin.to_s
            puts "end" + data_delimiter_end.to_s
            puts "msg" + message_size.to_s
          end
          
          message << fb.chr + sb.chr + query[data_delimiter_begin...data_delimiter_end]
          data_delimiter_begin = data_delimiter_end
          data_delimiter_end = data_delimiter_begin + @@MAX_MESSAGE_SIZE
         
          message_size -= @@MAX_MESSAGE_SIZE 
        end  
        i += 1
      end
    end
    
    return message.freeze
  end

  # Used as the first step in the authentication phase; retrives a challenge string from the server.
  def retrieve_server_challenge()
    server_challenge = ""
    is_final = false
    while (!is_final)
      is_final, chunk_size = recv_decode_hdr()
      # retrieve the server challenge string.
      server_challenge += @socket.recv(chunk_size)
    end
    return server_challenge
  end
  
  # reads and decodes the header of a server message
  def recv_decode_hdr()
    if @socket != nil
      fb = @socket.recv(1)
      sb = @socket.recv(1)
      chunk_size = 0
      is_final = false
      if ( (fb[0].to_i & 1) == 1 )
        is_final = true
      end

      # return the size of the chunk (in bytes)
      
      chunk_size = (sb[0].to_i << 7) | (fb[0].to_i >> 1)
      return is_final, chunk_size  
    else  
        raise MonetDBSocketError
    end 
  end
  
  # Sets the time zone according to the Operating System settings
  def set_timezone()
    tz = Time.new
    tz_offset = tz.gmt_offset / @@HOUR
      
    if tz_offset <= 9 # verify minute count!
      tz_offset = "'+0" + tz_offset.to_s + ":00'"
    else
      tz_offset = "'+" + tz_offset.to_s + ":00'"
    end
      
    query_tz = "SET TIME ZONE INTERVAL " + tz_offset + " HOUR TO MINUTE;"
    
    # Perform the query directly within the method
    encode_message(query_tz).each { |msg|
      @socket.write(msg)
    }
    
    is_final, chunk_size = recv_decode_hdr
    
    if chunk_size > 0
      monetdb_tz = @socket.recv(chunk_size)
      if !is_final
        while !is_final
          is_final, chunk_size = recv_decode_hdr
          monetdb_tz += @socket.recv(chunk_size)
        end 
      end
      
#      raise MonetDBQueryError, monetdb_tz
    end
  end
  
  # Check if monetdb is running behind the merovingian proxy and forward the connection in case
  def merovingian?
    if @server_name.downcase == 'merovingian'
      true
    else
      false
    end
  end
  
  def mserver?
    if @server_name.downcase == 'monetdb'
      true
    else
      false
    end
  end
  
  # Check which protocol is spoken by the server
  def mapi_proto_v8?
    if @protocol == 8
      true
    else
      false
    end
  end
  
  def mapi_proto_v9?
    if @protocol == 9
      true
    else
      false
    end
  end
  
end


# TOD: Handle the authentication procedure for protocols 8 and 9.
#class MonetDBAuthentication
  
#  attr_reader :server_challenge, :challenge_reply
  
#  def initialize(proto = 8) :nodoc
#    @proto = proto
#  end
  
  # Takes as input a server challenge and credentials and returns 
  # a response to autenticat to the mserver
#  def login(challenge = '', account = [])
#     @server_challenge = challenge
#    
#    if proto == 8
#      build_challenge_reply8(account)
#    elsif proto == 9
#      build_challenge_reply9(account)
#    end
#  end

#  private
  # Takes as imput a server challenge and credentials and returns 
  # a response to autenticat to mserver with MAPI protocol 8
#  def build_challenge_reply8(account = [])
#    @challenge_reply = ''
#  end
  
  # Takes as imput a server challenge and credentials and returns 
  # a response to autenticat to mserver with MAPI protocol 9
#  def build_challenge_reply9(account = [])
#    @challenge_reply = ''
#  end
#end
