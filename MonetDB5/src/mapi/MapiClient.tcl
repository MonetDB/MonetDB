
lappend auto_path [pwd]
package require mapi 1.0

set ::mapi::state(trace)        0

mapi::connect [mapi::hostname] [mapi::portnr] $env(USER)

puts "#MapiClient (tcl) connected to [mapi::hostname]:[mapi::portnr] as $env(USER)"

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
