
create table netcdf_files(
      file_id int,
      location char(256));
-- For NetCDF4/HDF5 files we may need additional table describing groups

create table netcdf_dims(
    dim_id int,
    file_id int,
    name varchar(64),
    length int);

create table netcdf_vars(
    var_id int,
    file_id int,
    name varchar(64),
    vartype varchar(64),
    ndim int,
    coord_dim_id int);
-- coordinate variables have coord_dim_id set to the dimension they describe

create table netcdf_vardim(
    var_id int,
    dim_id int,
    file_id int,
    dimpos int);

create table netcdf_attrs(
    obj_name varchar(256),
    att_name varchar(256),
    att_type varchar(64),
    value text,
    file_id int,
    gr_name varchar(256));
-- gr_name is "GLOBAL" or "ROOT" for classic NetCDF files
-- used for groups in HDF5 files
-- global attributes have obj_name=""

-- create function netcdfvar (fname varchar(256)) 
--	returns int external name netcdf.test;

create procedure netcdf_attach(fname varchar(256))
    external name netcdf.attach;
create procedure netcdf_importvar(fid integer, varnname varchar(256))
    external name netcdf.importvariable;

