--
-- CREATE_OPERATOR
--

CREATE OPERATOR ## ( 
   leftarg = string,
   rightarg = string,
   procedure = path_inter,
   commutator = ## 
);

CREATE OPERATOR <% (
   leftarg = string,
   rightarg = widget,
   procedure = pt_in_widget,
   commutator = >% ,
   negator = >=% 
);

CREATE OPERATOR @#@ (
   rightarg = bigint,		-- left unary 
   procedure = numeric_fac 
);

CREATE OPERATOR #@# (
   leftarg = bigint,		-- right unary
   procedure = numeric_fac
);

CREATE OPERATOR #%# ( 
   leftarg = bigint,		-- right unary 
   procedure = numeric_fac 
);

-- Test comments



