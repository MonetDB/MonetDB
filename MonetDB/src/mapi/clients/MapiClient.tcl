lappend auto_path [pwd]
package require mapi 1.0

set ::mapi::state(trace)        0

proc usage { } {
	puts "usage: tclsh MapiClient.tcl"
	puts "\[-h or --host hostname \]"
        puts "\[-p or --port portnr   \]"
	puts "\[-d or --debug         \]"
	puts "\[-? or --help          \]"
}

proc handle_args { argc argv } {
	if { $argc != 0 } {
		set a 0
		while { [ expr $a < $argc ] } {
			set arg [ lindex $argv $a ]
			switch -regexp -- $arg {
			"^--?ho?s?t?$" {
				set v $a
				incr v
				if { $v >= $argc } {
					puts "!ERROR: missing argument for --host option!"
					usage
					exit 1
				}
				set var [ lindex $argv $v ]
				if { [ regexp "^--?(.*)$" $var ] } {
					puts "!ERROR: missing argument for --host option!"
					usage
					exit 1
				}
				set mapi::state(host) $var
				set a $v
			}
			"^--?po?r?t?$" {
				set v $a
				incr v
				if {  $v >= $argc } {
					puts "!ERROR: missing argument for --port option!"
					usage
					exit 1
				}
				set var [ lindex $argv $v ]
				if { [ regexp "^--?(.*)$" $var ] } {
					puts "!ERROR: missing argument for --port option!"
					usage
					exit 1
				}
				set mapi::state(port) $var
				set a $v
			}
			"^--?de?b?u?g?$" {
				set ::mapi::state(trace) 1
			}
			"^--?he?l?p?$" {
				usage
				exit 0
			}
			{^--?\?$} {
				usage
				exit 0
			}
			}
			incr a	
		}
	}
}

handle_args $argc $argv

if { $mapi::state(trace) } {
	mapi::dumpstate
}

mapi::connect $mapi::state(host) $mapi::state(port) $env(USER)

if { ! [ mapi::connected ] } {
	puts "!ERROR: MapiClient (tcl) couldn't connected to $mapi::state(host):$mapi::state(port) as $env(USER)"
	exit 1
}

puts "#MapiClient (tcl) connected to $mapi::state(host):$mapi::state(port) as $env(USER)"

while { 1 } {
	puts -nonewline $mapi::state(prompt)
	flush file1
	set s [ gets file0 ]
	if { ![ string compare "quit;" $s ] } {
		break;
	}
	puts [ mapi::command $s ]
}

mapi::disconnect
