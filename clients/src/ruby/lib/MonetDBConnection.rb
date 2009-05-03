# Information relative to a MonetDB connection

require 'socket'
require 'time'
require 'hasher'
require 'MonetDBExceptions'

class MonetDBConnection
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
  
    @connection_established = false
  end
  
  # Connect to the database, creates a new socket
  def real_connect(db_name = "demo", auth_type = "SHA1")
      @socket = TCPSocket.new(@host, @port)  
       server_challenge = retrieve_server_challenge()

       if server_challenge != ""
         salt = server_challenge.split(':')[0]
         @server_name = server_challenge.split(':')[1]
         @protocol = server_challenge.split(':')[2]
         @supported_auth_types = server_challenge.split(':')[3].split(',')
         @server_endianness = server_challenge.split(':')[4]

         reply = build_auth_string(auth_type, salt, db_name)

       end
       
        if @socket != nil
           @connection_established = true

           encode_message(reply).each { |msg|
             @socket.write(msg)
           }

           f, monetdb_auth = recv_decode_hdr

           if monetdb_auth > 0
             @connection_established = false
             raise MonetDBQueryError, monetdb_auth
           else
               # set timezone  
                set_timezone
               #  msg = encode_message("Xexport_size 1;\n")
               #  @socket.write(msg)

           end
        else
          raise MonetDBSocketError
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
    encode_message(msg).each { |m|
      @socket.write(m)
    }
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
  
  
  # Builds and authentication string given the parameters submitted by the user.
  # 
  def build_auth_string(auth_type, salt, db_name)
    
    # seed = password + salt
    if auth_type.upcase == "MD5" or auth_type.upcase == "SHA1"
      auth_type = auth_type.upcase
      digest = Hasher.new(auth_type, @user+salt)
      hashsum = digest.hashsum
    end
    
    if auth_type.downcase == "plain"
      auth_type = auth_type.downcase
      hashsum = @user + salt
    end    
    # Build the reply message with header
    reply = @client_endianness + ":" + @user + ":{" + auth_type + "}" + hashsum + ":" + @lang + ":" + db_name + ":" 
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
      if ( (fb[0] & 1) == 1 )
        is_final = true
      end

      # return the size of the chunk (in bytes)
      
      chunk_size = (sb[0] << 7) | (fb[0] >> 1)
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
  
end