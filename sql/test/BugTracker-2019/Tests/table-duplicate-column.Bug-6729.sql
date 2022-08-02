CREATE TABLE products ("id" int not null primary key, "name" varchar(99) not null, "price" decimal(7,2) not null);
CREATE TABLE products_new ("id" serial, like products); --error, column id already exists
CREATE TABLE products_new2 (like products, like products); --error, column id already exists
DROP TABLE products;
