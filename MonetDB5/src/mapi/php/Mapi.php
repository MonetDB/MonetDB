<?php

class Mapi {
	
  Function init ($server, $port, $user) {
    $this->server = $server;
    $this->port = $port;
    $this->user = $user;
    $this->socket = fsockopen( $server, $port );

    socket_set_blocking($this->socket, false);

    $this->buf = "";
    $this->prompt = "";
    $this->cmd_intern("$user\n");
    $this->result();
    return $this;
  }

  Function mapiport_intern () {
    $mapiport = 'localhost:50000';
    if (getenv("MAPIPORT")) $mapiport = getenv("MAPIPORT"); 
    return $mapiport;
  }

  Function hostname () {
    ereg("([a-zA-Z_]*)(:[0-9]*|$)", $this->mapiport_intern(), $res);
    $hostname = $res[1];
    if ( $hostname == '' ) $hostname = 'localhost';
    return $hostname;
  }

  Function portnr () {
    ereg("([a-zA-Z_]*)(:[0-9]*|$)", $this->mapiport_intern(), $res);
    $portnr = $res[2];
    if ( $portnr == '' ) $portnr = 50001;
    else $portnr = substr($portnr,1, strlen($portnr) - 1);
    return $portnr;
  }

  Function disconnect () {
    $this->cmd_intern("quit;\n");
    close($this->socket);
  }

  Function cmd_intern ($cmd) {
    fputs($this->socket, $cmd, strlen($cmd) ); 
  }

  Function getstring () {
    while( ($index = strpos( $this->buf, "\001" )) === false){ 
	$this->buf .= fgets( $this->socket, 4096 ); 
    }
    $result = substr( $this->buf, 0, $index);
    $this->buf = substr( $this->buf, $index + 1, strlen($this->buf)); 
    return $result;
  }

  Function result () {
    $res = $this->getstring();
    $this->getprompt();
    return $res;
  }

  Function getprompt () {
    $prompt = $this->getstring();
  }

  Function cmd ( $cmd ) {
    $cmd = "$cmd\n";
    $this->cmd_intern($cmd);
    return $this->result();
  }
}
?>
