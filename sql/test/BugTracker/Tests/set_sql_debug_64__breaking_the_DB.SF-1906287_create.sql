CREATE USER "skyserver" WITH PASSWORD 'skyserver' NAME 'sky server' SCHEMA
"sys";
create schema "sky" authorization "skyserver";
alter user "skyserver" set schema "sky";
