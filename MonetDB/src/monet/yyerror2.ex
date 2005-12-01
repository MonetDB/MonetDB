%s/TOK_ASSIGNMENT/:=/
%s/TOK_COMMAND/command/
%s/TOK_IDENT/identifier/
%s/TOK_BUILTIN/builtin/
%s/TOK_PROC/procedure/
%s/TOK_ATOM/atom/
%s/TOK_ON/keyword 'on'/
%s/TOK_TRG/trigger/
%s/TOK_OPERATOR/operator/
%s/TOK_ITERATOR/iterator/
%s/TOK_TEMPLATE/\$x/
%s/TOK_ITERATION/@/
%s/TOK_OBJECT/object/
%s/TOK_BACK/\.\.\//
w! yyerror.h 
