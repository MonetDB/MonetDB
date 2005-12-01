/accept$/;/^state/,$g/error$/kb\
?^state?ka\
'a+;/^$/d\
'a+,'b-s/	\(.*\)  shift.*/\1 or /\
'b-s/or //\
'as/state /    case /\
'as/$/:	fprintf(fd,"would a /\
'a,'b-j!\
'b-s/$/be missing?\\n"); break;/
v/case/d
1i
extern int	yylineno;

myerror(fd, state)
FILE *fd;
int state;
{
    switch(state) {
    default:    fprintf(fd,"You did something wrong. Can't help you here, sorry.\n"); break;
.
$a
    };
}
.
w! yyerror.h
q
@
