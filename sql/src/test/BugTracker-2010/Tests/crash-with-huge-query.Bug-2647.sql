--
-- Table structure for table payments
--

CREATE TABLE payments (
  id integer,
  order_id integer default NULL,
  created_at timestamp default NULL,
  updated_at timestamp default NULL,
  amount decimal(8,2) NOT NULL default '0.00',
  creditcard_id integer default NULL,
  paytype varchar(255) default NULL
);

--
-- Table structure for table addresses
--

CREATE TABLE addresses (
  id integer,
  firstname varchar(255) default NULL,
  lastname varchar(255) default NULL,
  address1 varchar(255) default NULL,
  address2 varchar(255) default NULL,
  city varchar(255) default NULL,
  state_id integer default NULL,
  zipcode varchar(255) default NULL,
  country_id integer default NULL,
  phone varchar(255) default NULL,
  created_at timestamp default NULL,
  updated_at timestamp default NULL,
  state_name varchar(255) default NULL
);

--
-- Table structure for table countries
--

CREATE TABLE countries (
  id integer,
  iso_name varchar(255) default NULL,
  iso varchar(255) default NULL,
  name varchar(255) default NULL,
  iso3 varchar(255) default NULL,
  numcode integer default NULL
);

--
-- Table structure for table line_items
--

CREATE TABLE line_items (
  id integer,
  order_id integer default NULL,
  variant_id integer default NULL,
  quantity integer NOT NULL,
  price decimal(8,2) NOT NULL,
  created_at timestamp default NULL,
  updated_at timestamp default NULL
);

--
-- Table structure for table orders
--

CREATE TABLE orders (
  id integer default NULL,
  user_id integer default NULL,
  number varchar(255) default NULL,
  ship_amount decimal(8,2) NOT NULL default '0.00',
  tax_amount decimal(8,2) NOT NULL default '0.00',
  item_total decimal(8,2) NOT NULL default '0.00',
  total decimal(8,2) NOT NULL default '0.00',
  ip_address varchar(255) default NULL,
  special_instructions text,
  created_at timestamp default NULL,
  updated_at timestamp default NULL,
  state varchar(255) default NULL,
  checkout_complete integer default NULL,
  token varchar(255) default NULL,
  email varchar(255) default NULL,
  bill_address_id integer default NULL,
  ship_address_id integer default NULL
);

--
-- Table structure for table products
--

CREATE TABLE products (
  id integer default NULL,
  name varchar(255) NOT NULL default '',
  description text,
  master_price decimal(8,2) default NULL,
  created_at timestamp default NULL,
  updated_at timestamp default NULL,
  permalink varchar(255) default NULL,
  available_on timestamp default NULL,
  tax_category_id integer default NULL,
  shipping_category integer default NULL,
  deleted_at timestamp default NULL,
  meta_description varchar(255) default NULL,
  meta_keywords varchar(255) default NULL
);

--
-- Table structure for table schema_migrations
--

CREATE TABLE schema_migrations (
  version varchar(255) NOT NULL
);

--
-- Table structure for table shipments
--

CREATE TABLE shipments (
  id integer,
  order_id integer default NULL,
  shipping_method_id integer default NULL,
  tracking varchar(255) default NULL,
  created_at timestamp default NULL,
  updated_at timestamp default NULL,
  number varchar(255) default NULL,
  cost decimal(8,2) default NULL,
  shipped_at timestamp default NULL,
  address_id integer default NULL
);

--
-- Table structure for table shipping_methods
--

CREATE TABLE shipping_methods (
  id integer,
  zone_id integer default NULL,
  shipping_calculator varchar(255) default NULL,
  name varchar(255) default NULL,
  created_at timestamp default NULL,
  updated_at timestamp default NULL
);

--
-- Table structure for table states
--

CREATE TABLE states (
  id integer,
  name varchar(255) default NULL,
  abbr varchar(255) default NULL,
  country_id integer default NULL
);

--
-- Table structure for table tax_categories
--

CREATE TABLE tax_categories (
  id integer,
  name varchar(255) default NULL,
  description varchar(255) default NULL,
  created_at timestamp default NULL,
  updated_at timestamp default NULL
);

--
-- Table structure for table tax_rates
--

CREATE TABLE tax_rates (
  id integer default NULL,
  zone_id integer default NULL,
  amount decimal(8,4) default NULL,
  created_at timestamp default NULL,
  updated_at timestamp default NULL,
  tax_type integer default NULL,
  tax_category_id integer default NULL
);

--
-- Table structure for table variants
--

CREATE TABLE variants (
  id integer default NULL,
  product_id integer default NULL,
  sku varchar(255) default NULL,
  price decimal(8,2) NOT NULL,
  weight decimal(8,2) default NULL,
  height decimal(8,2) default NULL,
  width decimal(8,2) default NULL,
  depth decimal(8,2) default NULL,
  deleted_at timestamp default NULL
);

--
-- Table structure for table zone_members
--

CREATE TABLE zone_members (
  id integer default NULL,
  zone_id integer default NULL,
  country_id integer default NULL,
  zoneable_type varchar(255) default NULL,
  created_at timestamp default NULL,
  updated_at timestamp default NULL
);

--
-- Table structure for table zones
--

CREATE TABLE zones (
  id integer default NULL,
  name varchar(255) default NULL,
  description varchar(255) default NULL,
  created_at timestamp default NULL,
  updated_at timestamp default NULL
);

--
-- Constraints for table zones
--

ALTER TABLE zones
  ADD CONSTRAINT prim_zones_id 
    PRIMARY KEY(id);
ALTER TABLE zones
  ALTER COLUMN id SET NOT NULL;

--
-- Constraints for table orders
--

ALTER TABLE orders
  ADD CONSTRAINT prim_orders_id 
    PRIMARY KEY(id);
ALTER TABLE orders
  ALTER COLUMN id SET NOT NULL;

--
-- Constraints for table addresses
--

ALTER TABLE addresses
  ADD CONSTRAINT prim_addresses_id 
  PRIMARY KEY(id);
ALTER TABLE addresses
  ALTER COLUMN id SET NOT NULL;

--
-- Constraints for table countries
--

ALTER TABLE countries
  ADD CONSTRAINT prim_countries_id 
    PRIMARY KEY(id);
ALTER TABLE countries
  ALTER COLUMN id SET NOT NULL;

--
-- Constraints for table states
--

ALTER TABLE states
  ADD CONSTRAINT prim_states_id 
    PRIMARY KEY(id);
ALTER TABLE states
  ALTER COLUMN id SET NOT NULL;

ALTER TABLE states
  ADD CONSTRAINT for_key_states_country_id
  FOREIGN KEY (country_id) REFERENCES states(id);





--
-- Constraints for table payments
--

ALTER TABLE payments
  ADD CONSTRAINT prim_payments_id 
    PRIMARY KEY(id);
ALTER TABLE payments
  ALTER COLUMN id SET NOT NULL;

ALTER TABLE payments
  ADD CONSTRAINT for_key_payments_order_id
  FOREIGN KEY (order_id) REFERENCES orders(id);

--
-- Constraints for table products
--

ALTER TABLE products
  ADD CONSTRAINT prim_products_id 
    PRIMARY KEY(id);
ALTER TABLE products
  ALTER COLUMN id SET NOT NULL;

--
-- Constraints on table schema_migrations
--

ALTER TABLE schema_migrations
  ADD CONSTRAINT prim_schema_migrations_id 
    PRIMARY KEY(version);
ALTER TABLE schema_migrations
  ALTER COLUMN version SET NOT NULL;

--
-- Constraints for table shipping_methods
--

ALTER TABLE shipping_methods
  ADD CONSTRAINT prim_shipping_methods_id 
    PRIMARY KEY(id);
ALTER TABLE shipping_methods
  ALTER COLUMN id SET NOT NULL;

--
-- Constraints on table shipments
--

ALTER TABLE shipments
  ADD CONSTRAINT prim_shipments_id 
    PRIMARY KEY(id);
ALTER TABLE shipments
  ALTER COLUMN id SET NOT NULL;

ALTER TABLE shipments
  ADD CONSTRAINT for_key_shipments_order_id
  FOREIGN KEY (order_id) REFERENCES orders(id);
ALTER TABLE shipments
  ADD CONSTRAINT for_key_shipments_shipping_method_id
  FOREIGN KEY (shipping_method_id) REFERENCES shipping_methods(id);

--
-- Constraints for table tax_categories
--

ALTER TABLE tax_categories
  ADD CONSTRAINT prim_tax_categories_id 
    PRIMARY KEY(id);
ALTER TABLE tax_categories
  ALTER COLUMN id SET NOT NULL;

--
-- Constraints for table tax_rates
--

ALTER TABLE tax_rates
  ADD CONSTRAINT prim_tax_rates_id 
    PRIMARY KEY(id);
ALTER TABLE tax_rates
  ALTER COLUMN id SET NOT NULL;

ALTER TABLE tax_rates
  ADD CONSTRAINT for_key_tax_rates_zone_id
  FOREIGN KEY (zone_id) REFERENCES zones(id);

--
-- Constraints for table variants
--

ALTER TABLE variants
  ADD CONSTRAINT prim_variants_id 
    PRIMARY KEY(id);
ALTER TABLE variants
  ALTER COLUMN id SET NOT NULL;

ALTER TABLE variants
  ADD CONSTRAINT for_key_variants_on_product_id
  FOREIGN KEY (product_id) REFERENCES variants(id);

--
-- Constraints for table zone_members
--

ALTER TABLE zone_members
  ADD CONSTRAINT prim_zone_members_id 
    PRIMARY KEY(id);
ALTER TABLE zone_members
  ALTER COLUMN id SET NOT NULL;

ALTER TABLE zone_members
  ADD CONSTRAINT for_key_zone_members_country_id
  FOREIGN KEY (country_id) REFERENCES countries(id);
ALTER TABLE zone_members
  ADD CONSTRAINT for_key_zone_members_zone_id
  FOREIGN KEY (zone_id) REFERENCES zones(id);

--
-- Constraints for table line_items
--

ALTER TABLE line_items
  ADD CONSTRAINT prim_line_items_id 
    PRIMARY KEY(id);
ALTER TABLE line_items
  ALTER COLUMN id SET NOT NULL;

ALTER TABLE line_items
  ADD CONSTRAINT for_key_line_items_on_order_id
  FOREIGN KEY (order_id) REFERENCES orders(id);
ALTER TABLE line_items
  ADD CONSTRAINT for_key_line_items_on_variant_id
  FOREIGN KEY (variant_id) REFERENCES variants(id);

WITH

yv(id, product_id, sku, price, weight,
   height, width, depth, deleted_at,
   line_item_id, order_id) AS
  (SELECT v.id, v.product_id, v.sku,
          v.price, v.weight, v.height,
          v.width, v.depth, v.deleted_at, li.id, o.id
     FROM Orders o, Line_Items li, Variants v
    WHERE o.id = li.order_id
      AND li.variant_id = v.id
      AND o.user_id = 20),

Cheapest_Price(product_id, price) AS
  (SELECT product_id, MIN(price)
     FROM Variants v
    WHERE v.product_id IN (SELECT product_id FROM yv)
   GROUP BY product_id),

Cheapest_Variants(id, product_id, sku,
                  price, weight, height,
                  width, depth, deleted_at) AS
  (SELECT v.id, v.product_id, v.sku, v.price,
          v.weight, v.height, v.width, v.depth, v.deleted_at
    FROM Variants v, Cheapest_Price cp
   WHERE v.price = cp.price
     AND v.product_id = cp.product_id),

Suggestions_(rid, id, product_id, sku,
             price, weight, height,
             width, depth, deleted_at) AS
  (SELECT ROW_NUMBER() OVER (PARTITION BY cv.id) AS rid,
          cv.id, cv.product_id, cv.sku, cv.price,
          cv.weight, cv.height, cv.width, cv.depth, cv.deleted_at
     FROM Cheapest_Variants cv),

Suggestions(sugg_id, sugg_product_id, sugg_sku, sugg_price,
            sugg_weight, sugg_height, sugg_width, sugg_depth, sugg_deleted_at,
            id, product_id, sku, price, weight, height, width, depth,
deleted_at,
            line_item_id, order_id) AS
  (SELECT cv.id as sugg_id, cv.product_id as sugg_product_id, cv.sku as
sugg_sku, cv.price as sugg_price,
          cv.weight as sugg_weight, cv.height as sugg_height, cv.width as
sugg_width, cv.depth as sugg_depth,
          cv.deleted_at as sugg_deleted_at,
          yv.id, yv.product_id, yv.sku, yv.price, yv.weight, yv.height,
yv.width, yv.depth, yv.deleted_at,
          yv.line_item_id, yv.order_id
     FROM Suggestions_ cv, yv yv
    WHERE cv.product_id = yv.product_id
      AND rid = 1),

-- return only one variant in suggestions

Savings(order_id, amount) AS
  (SELECT order_id, SUM(s.price - s.sugg_price) AS amount
    FROM Suggestions s
   GROUP BY order_id)

SELECT sugg.*, sav.amount
  FROM suggestions sugg, savings sav
 WHERE sugg.order_id = sav.order_id;


DROP TABLE payments; -- dep: orders
DROP TABLE shipments; -- dep: orders
DROP TABLE line_items; -- dep:  orders
DROP TABLE orders; -- dep: addresses
DROP TABLE addresses; -- dep: states, countries
DROP TABLE zone_members; -- dep: zones, countries
DROP TABLE countries; -- dep:
DROP TABLE variants; -- dep: products
DROP TABLE products; -- dep:
DROP TABLE schema_migrations; -- dep: 
DROP TABLE shipping_methods; -- dep: zones
DROP TABLE states; -- dep: states
DROP TABLE tax_categories; -- dep:
DROP TABLE tax_rates; -- dep: zones
DROP TABLE zones; -- dep:
