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
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

#
# The Tcl Mapi client
# author Niels Nes & Menzo Windhouwer

package provide mapi 1.0

namespace eval ::mapi:: {

	set ::mapi::state(socket)	nil
	set ::mapi::state(trace)	0
	set ::mapi::state(host)		"[info hostname]"
	set ::mapi::state(port)		"50000"
	set ::mapi::state(buffer)	""
	set ::mapi::state(prompt)	">"

	namespace export hostname
	namespace export portnr

	namespace export connect
	namespace export connected
	namespace export disconnect

	namespace export command
	namespace export query

}

proc ::mapi::connect { server port user } {
	set out ""
	set err [ catch { set ::mapi::state(socket) [ socket $server $port ] } out ]
	if { $err } {
		puts "!ERROR: mapi::connect: $out"
		set ::mapi::state(socket) "nil"
		return 1
	}

	fconfigure $::mapi::state(socket) -translation binary -encoding utf-8
	fconfigure $::mapi::state(socket) -blocking no

	mapi::cmd_intern "$(user)::mil:line"
	mapi::result

	if { [ eof $::mapi::state(socket) ] } {
      		puts "monet refuses access"
		set ::mapi::state(socket) "nil"
		return 1
  	} 
   	if { $::mapi::state(trace) } {
   		puts "connected $::mapi::state(socket)"
  	}
	return 0
}

proc ::mapi::connected { } {
	if { $::mapi::state(socket) == "nil" } {
		return 0
	}
	return 1
}

proc ::mapi::disconnect { } {
	set result [ mapi::cmd_intern "quit;"]
	close $::mapi::state(socket)
	set ::mapi::state(socket) "nil"
}

proc ::mapi::command { cmd } {
	set cmd [ string trim $cmd ]
	if { [ string index $cmd [ expr [ string length $cmd ] - 1 ] ] != ";" } {
		append cmd ";"
	}
	mapi::cmd_intern $cmd 
	return [ mapi::result ]
}

proc ::mapi::query { query { cleanup 0 } } {
	mapi::cmd_intern $query
	set result [ mapi::getstring "\1" ]
	set lines [ split $result "\n" ]
	set l {}
	if { $cleanup } {
		foreach line $lines {
			lappend l [ mapi::clean_up_result $line ]
		}
	} else {
		set l $lines
	}
	mapi::getprompt
	if { $::mapi::state(trace) } {
		puts $result
   	}
   	return $l
}

proc ::mapi::cmd_intern { cmd } {
	puts $::mapi::state(socket) $cmd
	flush $::mapi::state(socket)
	if { $::mapi::state(trace) } {
   		puts "monet_cmd $cmd"
   	}
}

proc ::mapi::result { } {
	set result [ mapi::getstring "\1" ]
	mapi::getprompt
	if { $::mapi::state(trace) } {
      		puts $result
   	}
   	return $result
}

proc ::mapi::clean_up_result { res } {
   	if { $::mapi::state(trace) } {
		puts $res
   	}
        set s [ string first "\[ \"" $res ]
        set e [ string first "\" ]" $res ]
        incr s 3
        incr e -1
	return [ string range $res $s $e ]
}

proc ::mapi::getstring { endchr } { 
	set idx 0
	set str ""
	while { [ set idx [ string first $endchr $::mapi::state(buffer) ]] == -1 } {
		append str $::mapi::state(buffer)
		set ::mapi::state(buffer) [ read $::mapi::state(socket) 8096 ]
	}
	append str [ string range $::mapi::state(buffer) 0 [ expr $idx - 1 ] ]
	set ::mapi::state(buffer) [ string range $::mapi::state(buffer) [ expr $idx + 1 ] end ] 
    return [encoding convertfrom utf-8 $str]
}
   	
proc ::mapi::getprompt { } {
   	set ::mapi::state(prompt) [ mapi::getstring "\1" ]
}

proc ::mapi::dumpstate { } {
	set maxl 0
	foreach v [ array names ::mapi::state ] {
		if { [ string length $v ] > $maxl } {
			set maxl [ string length $v ]
		}
	}
	puts "Mapi state variables:"
	foreach v [ lsort [ array names ::mapi::state ] ] {
		puts [ format "- %-*s = %s" $maxl $v $::mapi::state($v) ]
	}
}
