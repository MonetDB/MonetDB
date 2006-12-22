CC = cl

all: winservice.exe MserverService.exe

winservice.exe: main.obj
	"$(CC)" -GF -W3 -wd4273 -wd4102 -MD -nologo -Zi -G6 -Fewinservice.exe main.obj /link kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /subsystem:console /NODEFAULTLIB:LIBC

main.obj: main.cpp service.h
	"$(CC)" -GF -W3 -wd4273 -wd4102 -MD -nologo -Zi -G6 -EHsc -c main.cpp

MserverService.exe: service.obj
	"$(CC)" -GF -W3 -wd4273 -wd4102 -MD -nologo -Zi -G6 -FeMserverService.exe service.obj /link kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib /subsystem:console /NODEFAULTLIB:LIBC

service.obj: service.cpp service.h
	"$(CC)" -GF -W3 -wd4273 -wd4102 -MD -nologo -Zi -G6 -EHsc -c service.cpp
