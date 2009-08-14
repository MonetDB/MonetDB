create table blabla(id integer);
explain alter table blabla add constraint dada unique (id);
explain alter table blabla add constraint dada unique (id);
alter table blabla drop constraint dada;
explain alter table blabla add constraint dada unique (id);
