
CREATE TABLE SALESMART (
    --ID BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    ITEM VARCHAR(100),
    CATEGORY VARCHAR(100),
    PRICE NUMERIC(10,2),
    CITY VARCHAR(100),
    REGION VARCHAR(100),
    COUNTRY VARCHAR(100)
);

COPY 1000 RECORDS INTO SALESMART FROM STDIN USING DELIMITERS ';', '\n', '"';
L'Oven Fresh white bread;Baked goods;0.59;San Francisco;California;United States
Texas cheese toast;Frozen;1.49;Paris;Ile-De-France;France
Broccoli stir fry mixture;Frozen;0.89;Bluff City;Kansas;United States
Sausage, brown/serve;Frozen;0.99;Plymouth;Tobago;Trinidad and Tobago
Peppers, tricolor pack;Produce;1.99;Glendale;Arizona;United States
Cinnamon Grahams;Snack;0.99;London;Ontario;Canada
Mushrooms;Produce;0.99;Blountsville;Alabama;United States
Tissues;Nonfoods;0.89;Aalestrup;Nordjylland;Denmark
Spoon size shredded wheat;Cereal;1.69;Cairo;Illinois;United States
Cheese, Colby brick;Dairy;1.49;Crooks;South Dakota;United States
Wheat crackers (triscuit);Snack;0.99;Stirling;Scotland;United Kingdom
Eggs, large;Dairy;0.89;Blountsville;Alabama;United States
Broccoli stir fry mixture;Frozen;0.89;Helsinki;Etela-Suomen Laani;Finland
Tissues;Nonfoods;0.89;Bluff City;Kansas;United States
Baking soda;Baking;0.39;West Long Branch;New Jersey;United States
Saltines;Snack;0.69;Amsterdam;Ohio;United States
Lettuce, iceberg;Produce;0.89;North Grafton;Massachusetts;United States
Belmont chocolate chip cookies;Snack;1.49;Victoria Falls;Matabeleland North;Zimbabwe
Orange juice, not from concentrate;Dairy;1.99;Crooks;South Dakota;United States
Milk, 2%;Dairy;2.69;Hechi;Guangxi;China
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Amsterdam;Ohio;United States
Single serve fruit/gel bowls;Canned;1.39;Cushing;Texas;United States
Broccoli stir fry mixture;Frozen;0.89;Hamburg;Illinois;United States
Celery;Produce;0.99;Amsterdam;Ohio;United States
Butter;Dairy;1.99;Prudence Island;Rhode Island;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Jerusalem;Yerushalayim  (Jerusalem);Israel
Orange juice, not from concentrate;Dairy;1.99;Jerusalem;Yerushalayim  (Jerusalem);Israel
Cucumber (large);Produce;0.49;Roma;Texas;United States
Vegetarian vegetable condensed soup;Canned;0.43;Bangkok;Krung Thep Mahanakhon;Thailand
Sausage, brown/serve;Frozen;0.99;Prudence Island;Rhode Island;United States
Snack crackers (ritz);Snack;1.29;Franklin Springs;New York;United States
Sausage, brown/serve;Frozen;0.99;Hawkinsville;Georgia;United States
Saltines;Snack;0.69;Paris;Ile-De-France;France
Beef/bean burrito;Frozen;0.29;Scarborough;Tobago;Trinidad and Tobago
Eggs, large;Dairy;0.89;Holguin;Holguin;Cuba
Larado paper plates;Nonfoods;0.89;Jerusalem;Yerushalayim  (Jerusalem);Israel
Baking soda;Baking;0.39;Tornillo;Texas;United States
Larado paper plates;Nonfoods;0.89;Plymouth;Tobago;Trinidad and Tobago
Spoon size shredded wheat;Cereal;1.69;San Francisco;California;United States
Tissues;Nonfoods;0.89;Finlayson;Minnesota;United States
Celery;Produce;0.99;Montpellier;Languedoc-Roussillon;France
Single serve peach cups;Canned;1.39;Jose Leon Suarez;Buenos Aires;Argentina
Sausage, brown/serve;Frozen;0.99;Bluff City;Kansas;United States
Broccoli stir fry mixture;Frozen;0.89;Tornillo;Texas;United States
Beef/bean burrito;Frozen;0.29;Graysville;Tennessee;United States
Celery;Produce;0.99;Madrid;Distrito Capital;Colombia
Deluxe topping pizza;Frozen;1.99;Hamburg;Illinois;United States
Belmont chocolate chip cookies;Snack;1.49;Santiago del Estero;Santiago del Estero;Argentina
Applesauce cups (reg/cinnamon);Canned;0.89;St Andrews;Scotland;United Kingdom
Cookies, chocolate sandwich (Oreo);Snack;0.99;Thousandsticks;Kentucky;United States
Sausage, brown/serve;Frozen;0.99;Plymouth;Tobago;Trinidad and Tobago
Sausage, brown/serve;Frozen;0.99;Elizabeth;New Jersey;United States
Wheat crackers (triscuit);Snack;0.99;St Andrews;Scotland;United Kingdom
Celery;Produce;0.99;Wellington;Wellington;New Zealand
Vegetarian vegetable condensed soup;Canned;0.43;Seven Rivers;Westmoreland;Jamaica
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Trondheim;Sor-Trondelag;Norway
Beef/bean burrito;Frozen;0.29;Douglas City;California;United States
Single serve fruit/gel bowls;Canned;1.39;Meadowlands;Minnesota;United States
Broccoli stir fry mixture;Frozen;0.89;Hamburg;Hamburg;Germany
Snack crackers (ritz);Snack;1.29;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Cookies, chocolate sandwich (Oreo);Snack;0.99;Plymouth;Tobago;Trinidad and Tobago
Bananas;Produce;0.33;Gafsa;Qafsah;Tunisia
Peppers, tricolor pack;Produce;1.99;North Grafton;Massachusetts;United States
Cucumber (large);Produce;0.49;Roxborough;Tobago;Trinidad and Tobago
Tissues;Nonfoods;0.89;Kyoto;Kyoto;Japan
Butter;Dairy;1.99;Malone;New York;United States
Saltines;Snack;0.69;Thousandsticks;Kentucky;United States
Sausage, brown/serve;Frozen;0.99;Scarborough;Tobago;Trinidad and Tobago
Texas cheese toast;Frozen;1.49;Helsinki;Etela-Suomen Laani;Finland
Snack crackers (ritz);Snack;1.29;West Long Branch;New Jersey;United States
Saltines;Snack;0.69;Amsterdam;Ohio;United States
Spoon size shredded wheat;Cereal;1.69;Beijing;Beijing;China
Sweetener packets;Baking;1.49;Jose Leon Suarez;Buenos Aires;Argentina
Rye bread, seeded;Baked goods;0.99;Chicago;Illinois;United States
Texas cheese toast;Frozen;1.49;Beijing;Beijing;China
Bananas;Produce;0.33;Plymouth;Tobago;Trinidad and Tobago
Waffles;Frozen;0.99;Graysville;Tennessee;United States
Butter;Dairy;1.99;La Rochelle;Poitou-Charentes;France
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
L'Oven Fresh white bread;Baked goods;0.59;Wallops Island;Virginia;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Chicago;Illinois;United States
Beef/bean burrito;Frozen;0.29;New York;New York;United States
Cucumber (large);Produce;0.49;Cogswell;North Dakota;United States
Broccoli stir fry mixture;Frozen;0.89;Saratoga;North Carolina;United States
Applesauce cups (reg/cinnamon);Canned;0.89;West Long Branch;New Jersey;United States
Bananas;Produce;0.33;Beaverton;Michigan;United States
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Beaverton;Michigan;United States
Orange juice, not from concentrate;Dairy;1.99;Ormond Beach;Florida;United States
Vegetarian vegetable condensed soup;Canned;0.43;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Baking soda;Baking;0.39;Wellington;Wellington;New Zealand
Mushrooms;Produce;0.99;Elizabeth;New Jersey;United States
Baking soda;Baking;0.39;Lone Pine;California;United States
Sweetener packets;Baking;1.49;Saratoga;North Carolina;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Cushing;Texas;United States
Baking soda;Baking;0.39;Amsterdam;Ohio;United States
L'Oven Fresh white bread;Baked goods;0.59;Roxborough;Tobago;Trinidad and Tobago
Rye bread, seeded;Baked goods;0.99;Cairo;Illinois;United States
Deluxe topping pizza;Frozen;1.99;Venlo;Limburg;Netherlands
Peppers, tricolor pack;Produce;1.99;Seven Rivers;Westmoreland;Jamaica
Tyson roasted chicken;Meat;3.99;Jose Leon Suarez;Buenos Aires;Argentina
Single serve fruit/gel bowls;Canned;1.39;Douglas City;California;United States
Saltines;Snack;0.69;Saratoga;North Carolina;United States
Cheese, Colby brick;Dairy;1.49;Sliema;Malta;Malta
Single serve peach cups;Canned;1.39;Seven Rivers;Westmoreland;Jamaica
Sausage, brown/serve;Frozen;0.99;Cairo;Illinois;United States
Waffles;Frozen;0.99;Los Angeles;California;United States
Butter;Dairy;1.99;Davos;Graubunden;Switzerland
Deluxe topping pizza;Frozen;1.99;Finlayson;Minnesota;United States
Belmont chocolate chip cookies;Snack;1.49;London;Ontario;Canada
L'Oven Fresh white bread;Baked goods;0.59;Montpellier;Languedoc-Roussillon;France
Sausage, brown/serve;Frozen;0.99;Las Vegas;Nevada;United States
Sausage, brown/serve;Frozen;0.99;Port Elizabeth;New Jersey;United States
Peppers, tricolor pack;Produce;1.99;Glendale;Arizona;United States
Single serve peach cups;Canned;1.39;Crooks;South Dakota;United States
Butter;Dairy;1.99;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Applesauce cups (reg/cinnamon);Canned;0.89;San Francisco;California;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Victoria Falls;Matabeleland North;Zimbabwe
Vegetarian vegetable condensed soup;Canned;0.43;Seven Rivers;Westmoreland;Jamaica
Pudding mix, choc/vanilla;Baking;0.29;Cushing;Texas;United States
Beef/bean burrito;Frozen;0.29;Elizabeth;New Jersey;United States
Vegetarian vegetable condensed soup;Canned;0.43;Graysville;Tennessee;United States
Beef/bean burrito;Frozen;0.29;Cattaraugus;New York;United States
Sausage, brown/serve;Frozen;0.99;Cairo;Illinois;United States
Peppers, green only;Produce;1.39;Kyoto;Kyoto;Japan
Waffles;Frozen;0.99;Stirling;Scotland;United Kingdom
Milk, 2%;Dairy;2.69;Wallops Island;Virginia;United States
Sweetener packets;Baking;1.49;Wallops Island;Virginia;United States
Texas cheese toast;Frozen;1.49;Shanghai;Shanghai;China
Peppers, green only;Produce;1.39;Franklin Springs;New York;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;West Long Branch;New Jersey;United States
Celery;Produce;0.99;Port Elizabeth;New Jersey;United States
Celery;Produce;0.99;Eau Galle;Wisconsin;United States
Rye bread, seeded;Baked goods;0.99;North Grafton;Massachusetts;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Las Vegas;Nevada;United States
Potatoes, Russett;Produce;0.89;Beijing;Beijing;China
Eggs, large;Dairy;0.89;Beijing;Beijing;China
Potatoes, Russett;Produce;0.89;Sliema;Malta;Malta
Single serve peach cups;Canned;1.39;Thelma;Kentucky;United States
Belmont chocolate chip cookies;Snack;1.49;Franklin Springs;New York;United States
Single serve peach cups;Canned;1.39;Montpellier;Languedoc-Roussillon;France
Belmont chocolate chip cookies;Snack;1.49;Beijing;Beijing;China
Butter;Dairy;1.99;Barcelona;CataluÒa;Spain
Lettuce, iceberg;Produce;0.89;Victoria Falls;Matabeleland North;Zimbabwe
Bananas;Produce;0.33;Eau Galle;Wisconsin;United States
Pudding mix, choc/vanilla;Baking;0.29;Victoria Falls;Matabeleland North;Zimbabwe
Butter;Dairy;1.99;Desmet;Idaho;United States
Orange juice, not from concentrate;Dairy;1.99;Bluff City;Kansas;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Fengjie;Sichuan;China
Sweetener packets;Baking;1.49;North Grafton;Massachusetts;United States
Single serve fruit/gel bowls;Canned;1.39;Sofia;Sofiya;Bulgaria
Milk, 2%;Dairy;2.69;London;Ontario;Canada
Cucumber (large);Produce;0.49;Elizabeth;New Jersey;United States
Vegetarian vegetable condensed soup;Canned;0.43;St Andrews;Scotland;United Kingdom
Eggs, large;Dairy;0.89;Malone;New York;United States
Larado paper plates;Nonfoods;0.89;Glendale;Arizona;United States
Cucumber (large);Produce;0.49;Thelma;Kentucky;United States
Baking soda;Baking;0.39;Byron;New York;United States
Vegetarian vegetable condensed soup;Canned;0.43;Beaverton;Michigan;United States
Rye bread, seeded;Baked goods;0.99;Montpellier;Languedoc-Roussillon;France
Sweetener packets;Baking;1.49;Glendale;Arizona;United States
Cinnamon Grahams;Snack;0.99;Sliema;Malta;Malta
Potatoes, Russett;Produce;0.89;Holguin;Holguin;Cuba
Eggs, large;Dairy;0.89;Cogswell;North Dakota;United States
Potatoes, Russett;Produce;0.89;Haslev;Vestsjalland;Denmark
Pudding mix, choc/vanilla;Baking;0.29;Glendale;Arizona;United States
Butter;Dairy;1.99;Santiago;Santiago;Dominican Republic
Applesauce cups (reg/cinnamon);Canned;0.89;Athens;Attiki;Greece
Peppers, green only;Produce;1.39;Blountsville;Alabama;United States
Broccoli stir fry mixture;Frozen;0.89;Campeche;Campeche;Mexico
Cookies, chocolate sandwich (Oreo);Snack;0.99;Chicago;Illinois;United States
Beef/bean burrito;Frozen;0.29;Port Elizabeth;New Jersey;United States
Pudding mix, choc/vanilla;Baking;0.29;London;Ontario;Canada
Baking soda;Baking;0.39;Cushing;Texas;United States
Butter;Dairy;1.99;Milano;Lombardia;Italy
Waffles;Frozen;0.99;Port Elizabeth;New Jersey;United States
Eggs, large;Dairy;0.89;Fengjie;Sichuan;China
Broccoli stir fry mixture;Frozen;0.89;Santa Fe;Tennessee;United States
L'Oven Fresh white bread;Baked goods;0.59;Thousandsticks;Kentucky;United States
Lettuce, iceberg;Produce;0.89;Mount Hope;Ohio;United States
Belmont chocolate chip cookies;Snack;1.49;Douglas City;California;United States
Deluxe topping pizza;Frozen;1.99;San Francisco;California;United States
Peppers, green only;Produce;1.39;Crooks;South Dakota;United States
Eggs, large;Dairy;0.89;Amsterdam;Ohio;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Thelma;Kentucky;United States
Cheese, Colby brick;Dairy;1.49;Prudence Island;Rhode Island;United States
Eggs, large;Dairy;0.89;Chicago;Illinois;United States
Vegetarian vegetable condensed soup;Canned;0.43;Fengjie;Sichuan;China
Beef/bean burrito;Frozen;0.29;Thelma;Kentucky;United States
Lettuce, iceberg;Produce;0.89;North Grafton;Massachusetts;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Helsinki;Etela-Suomen Laani;Finland
Baking soda;Baking;0.39;Prudence Island;Rhode Island;United States
Lettuce, iceberg;Produce;0.89;Roma;Texas;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;Mount Hope;Ohio;United States
Beef/bean burrito;Frozen;0.29;Roma;Texas;United States
L'Oven Fresh white bread;Baked goods;0.59;Franklin Springs;New York;United States
Celery;Produce;0.99;Warsaw;Mazowieckie;Poland
Orange juice, not from concentrate;Dairy;1.99;Fengjie;Sichuan;China
Saltines;Snack;0.69;Athens;Attiki;Greece
Saltines;Snack;0.69;Barcelona;CataluÒa;Spain
Pudding mix, choc/vanilla;Baking;0.29;Gafsa;Qafsah;Tunisia
Snack crackers (ritz);Snack;1.29;Kyoto;Kyoto;Japan
Cucumber (large);Produce;0.49;Kent City;Michigan;United States
Eggs, large;Dairy;0.89;Hechi;Guangxi;China
Potatoes, Russett;Produce;0.89;Sofia;Sofiya;Bulgaria
Single serve fruit/gel bowls;Canned;1.39;Port Harcourt;Rivers;Nigeria
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Kent City;Michigan;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Stirling;Scotland;United Kingdom
Peppers, tricolor pack;Produce;1.99;Eau Galle;Wisconsin;United States
Snack crackers (ritz);Snack;1.29;Thelma;Kentucky;United States
Potatoes, Russett;Produce;0.89;Kyoto;Kyoto;Japan
Single serve peach cups;Canned;1.39;New York;New York;United States
Sausage, brown/serve;Frozen;0.99;Ormond Beach;Florida;United States
Larado paper plates;Nonfoods;0.89;Warsaw;Mazowieckie;Poland
Potatoes, Russett;Produce;0.89;Bangkok;Krung Thep Mahanakhon;Thailand
Butter;Dairy;1.99;Elizabeth;New Jersey;United States
Cheese, Colby brick;Dairy;1.49;Roxborough;Tobago;Trinidad and Tobago
Beef/bean burrito;Frozen;0.29;Milano;Lombardia;Italy
Eggs, large;Dairy;0.89;Rogersville;Alabama;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;Port Elizabeth;New Jersey;United States
Single serve peach cups;Canned;1.39;Venlo;Limburg;Netherlands
Wheat crackers (triscuit);Snack;0.99;Greymouth;West Coast;New Zealand
Eggs, large;Dairy;0.89;Port Elizabeth;New Jersey;United States
Larado paper plates;Nonfoods;0.89;Helsinki;Etela-Suomen Laani;Finland
Sweetener packets;Baking;1.49;Mount Hope;Ohio;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Helsinki;Etela-Suomen Laani;Finland
Cat litter, scoopable;Nonfoods;2.99;Sofia;Sofiya;Bulgaria
L'Oven Fresh white bread;Baked goods;0.59;West Long Branch;New Jersey;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Malone;New York;United States
Saltines;Snack;0.69;Helsinki;Etela-Suomen Laani;Finland
Spoon size shredded wheat;Cereal;1.69;Kent City;Michigan;United States
Potatoes, Russett;Produce;0.89;Kent City;Michigan;United States
Saltines;Snack;0.69;Molodesjnaja;Enderby Land;Antarctica
Vegetarian vegetable condensed soup;Canned;0.43;Gafsa;Qafsah;Tunisia
Tissues;Nonfoods;0.89;Hamburg;Illinois;United States
Beef/bean burrito;Frozen;0.29;Victoria Falls;Matabeleland North;Zimbabwe
Applesauce cups (reg/cinnamon);Canned;0.89;Sofia;Sofiya;Bulgaria
Snack crackers (ritz);Snack;1.29;Jose Leon Suarez;Buenos Aires;Argentina
Cheese, Colby brick;Dairy;1.49;Byron;New York;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Scarborough;Tobago;Trinidad and Tobago
Applesauce cups (reg/cinnamon);Canned;0.89;Paris;Ile-De-France;France
Eggs, large;Dairy;0.89;Marthaville;Louisiana;United States
Eggs, large;Dairy;0.89;Venlo;Limburg;Netherlands
L'Oven Fresh white bread;Baked goods;0.59;Cairo;Illinois;United States
Sausage, brown/serve;Frozen;0.99;Jacksonville;Alabama;United States
Bananas;Produce;0.33;Prudence Island;Rhode Island;United States
Beef/bean burrito;Frozen;0.29;Barcelona;CataluÒa;Spain
Peppers, green only;Produce;1.39;Cairo;Illinois;United States
Cucumber (large);Produce;0.49;Trondheim;Sor-Trondelag;Norway
Applesauce cups (reg/cinnamon);Canned;0.89;Milano;Lombardia;Italy
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Rogersville;Alabama;United States
Potatoes, Russett;Produce;0.89;Campeche;Campeche;Mexico
Single serve peach cups;Canned;1.39;Moscow;Moskva;Russia
Orange juice, not from concentrate;Dairy;1.99;Wellington;Wellington;New Zealand
Baking soda;Baking;0.39;London;Ontario;Canada
Vegetarian vegetable condensed soup;Canned;0.43;Cogswell;North Dakota;United States
Peppers, tricolor pack;Produce;1.99;Hamburg;Illinois;United States
Baking soda;Baking;0.39;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Cucumber (large);Produce;0.49;San Francisco;California;United States
Beef/bean burrito;Frozen;0.29;Thousandsticks;Kentucky;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Cushing;Texas;United States
Saltines;Snack;0.69;Stirling;Scotland;United Kingdom
Rye bread, seeded;Baked goods;0.99;Jacksonville;Alabama;United States
Single serve fruit/gel bowls;Canned;1.39;Graysville;Tennessee;United States
Snack crackers (ritz);Snack;1.29;Vienna;Georgia;United States
Potatoes, Russett;Produce;0.89;Chicago;Illinois;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;Thelma;Kentucky;United States
Bananas;Produce;0.33;La Rochelle;Poitou-Charentes;France
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Saratoga;North Carolina;United States
Peppers, tricolor pack;Produce;1.99;Moscow;Moskva;Russia
Saltines;Snack;0.69;Borden;Indiana;United States
Rye bread, seeded;Baked goods;0.99;Vienna;Georgia;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Port Harcourt;Rivers;Nigeria
Deluxe topping pizza;Frozen;1.99;Saratoga;North Carolina;United States
Sausage, brown/serve;Frozen;0.99;Montpellier;Languedoc-Roussillon;France
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Bangkok;Krung Thep Mahanakhon;Thailand
Orange juice, not from concentrate;Dairy;1.99;Hamburg;Hamburg;Germany
Rye bread, seeded;Baked goods;0.99;Tornillo;Texas;United States
Tyson roasted chicken;Meat;3.99;West Long Branch;New Jersey;United States
Cinnamon Grahams;Snack;0.99;Lone Pine;California;United States
Beef/bean burrito;Frozen;0.29;Hechi;Guangxi;China
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Las Vegas;Nevada;United States
Baking soda;Baking;0.39;Chicago;Illinois;United States
Peppers, green only;Produce;1.39;Cattaraugus;New York;United States
Broccoli stir fry mixture;Frozen;0.89;Aalestrup;Nordjylland;Denmark
Cookies, chocolate sandwich (Oreo);Snack;0.99;Glendale;Arizona;United States
Wheat crackers (triscuit);Snack;0.99;New York;New York;United States
Belmont chocolate chip cookies;Snack;1.49;Fengjie;Sichuan;China
Waffles;Frozen;0.99;La Rochelle;Poitou-Charentes;France
Beef/bean burrito;Frozen;0.29;Cushing;Texas;United States
Baking soda;Baking;0.39;New York;New York;United States
Mushrooms;Produce;0.99;Jacksonville;Alabama;United States
Wheat crackers (triscuit);Snack;0.99;Elizabeth;New Jersey;United States
Applesauce cups (reg/cinnamon);Canned;0.89;North Grafton;Massachusetts;United States
Milk, 2%;Dairy;2.69;Meadowlands;Minnesota;United States
Belmont chocolate chip cookies;Snack;1.49;Beijing;Beijing;China
Single serve fruit/gel bowls;Canned;1.39;St Andrews;Scotland;United Kingdom
Saltines;Snack;0.69;Hawkinsville;Georgia;United States
Saltines;Snack;0.69;Madrid;Distrito Capital;Colombia
Lite drink mix (Crystal Lite);Beverages;1.89;La Rochelle;Poitou-Charentes;France
Spoon size shredded wheat;Cereal;1.69;Amsterdam;Ohio;United States
Larado paper plates;Nonfoods;0.89;Santiago del Estero;Santiago del Estero;Argentina
Texas cheese toast;Frozen;1.49;Seven Rivers;Westmoreland;Jamaica
Sausage, brown/serve;Frozen;0.99;Rogersville;Alabama;United States
Sweetener packets;Baking;1.49;Crooks;South Dakota;United States
Belmont chocolate chip cookies;Snack;1.49;Desmet;Idaho;United States
Belmont chocolate chip cookies;Snack;1.49;Wellington;Wellington;New Zealand
Broccoli stir fry mixture;Frozen;0.89;Venlo;Limburg;Netherlands
Bananas;Produce;0.33;Cologne;Nordrhein-Westfalen;Germany
Deluxe topping pizza;Frozen;1.99;Prudence Island;Rhode Island;United States
Mushrooms;Produce;0.99;Tokyo;Tokyo;Japan
Baking soda;Baking;0.39;Seven Rivers;Westmoreland;Jamaica
Celery;Produce;0.99;Tokyo;Tokyo;Japan
Lite drink mix (Crystal Lite);Beverages;1.89;Port Harcourt;Rivers;Nigeria
Lite drink mix (Crystal Lite);Beverages;1.89;Bluff City;Kansas;United States
Deluxe topping pizza;Frozen;1.99;Trondheim;Sor-Trondelag;Norway
Peppers, green only;Produce;1.39;Fengjie;Sichuan;China
Sausage, brown/serve;Frozen;0.99;Wellington;Wellington;New Zealand
Texas cheese toast;Frozen;1.49;Hechi;Guangxi;China
Snack crackers (ritz);Snack;1.29;Kent City;Michigan;United States
Vegetarian vegetable condensed soup;Canned;0.43;Borden;Indiana;United States
Potatoes, Russett;Produce;0.89;Franklin Springs;New York;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Meadowlands;Minnesota;United States
Potatoes, Russett;Produce;0.89;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Single serve peach cups;Canned;1.39;Bangkok;Krung Thep Mahanakhon;Thailand
Lettuce, iceberg;Produce;0.89;Molodesjnaja;Enderby Land;Antarctica
Broccoli stir fry mixture;Frozen;0.89;Bluff City;Kansas;United States
Mushrooms;Produce;0.99;Roxborough;Tobago;Trinidad and Tobago
Cat litter, scoopable;Nonfoods;2.99;Jerusalem;Yerushalayim  (Jerusalem);Israel
Milk, 2%;Dairy;2.69;Amsterdam;Ohio;United States
Mushrooms;Produce;0.99;Scarborough;Tobago;Trinidad and Tobago
Sweetener packets;Baking;1.49;Jerusalem;Yerushalayim  (Jerusalem);Israel
Lite drink mix (Crystal Lite);Beverages;1.89;Saratoga;North Carolina;United States
Vegetarian vegetable condensed soup;Canned;0.43;Tokyo;Tokyo;Japan
Wheat crackers (triscuit);Snack;0.99;Scarborough;Tobago;Trinidad and Tobago
Cinnamon Grahams;Snack;0.99;St Andrews;Scotland;United Kingdom
Vegetarian vegetable condensed soup;Canned;0.43;St Andrews;Scotland;United Kingdom
Snack crackers (ritz);Snack;1.29;Cattaraugus;New York;United States
Tissues;Nonfoods;0.89;Marthaville;Louisiana;United States
Pudding mix, choc/vanilla;Baking;0.29;Meadowlands;Minnesota;United States
Deluxe topping pizza;Frozen;1.99;Eau Galle;Wisconsin;United States
Peppers, tricolor pack;Produce;1.99;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Bananas;Produce;0.33;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Broccoli stir fry mixture;Frozen;0.89;Sliema;Malta;Malta
Belmont chocolate chip cookies;Snack;1.49;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Tissues;Nonfoods;0.89;Santa Fe;Tennessee;United States
Pudding mix, choc/vanilla;Baking;0.29;Tokyo;Tokyo;Japan
Deluxe topping pizza;Frozen;1.99;Sofia;Sofiya;Bulgaria
Tyson roasted chicken;Meat;3.99;Athens;Attiki;Greece
Larado paper plates;Nonfoods;0.89;Roxborough;Tobago;Trinidad and Tobago
Celery;Produce;0.99;Plymouth;Tobago;Trinidad and Tobago
Orange juice, not from concentrate;Dairy;1.99;Cairo;Illinois;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Victoria Falls;Matabeleland North;Zimbabwe
Waffles;Frozen;0.99;Crooks;South Dakota;United States
Cat litter, scoopable;Nonfoods;2.99;Davos;Graubunden;Switzerland
Wheat crackers (triscuit);Snack;0.99;Cattaraugus;New York;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;Campeche;Campeche;Mexico
Cheese, Colby brick;Dairy;1.49;Hawkinsville;Georgia;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Tokyo;Tokyo;Japan
Tissues;Nonfoods;0.89;Saratoga;North Carolina;United States
Pudding mix, choc/vanilla;Baking;0.29;Cushing;Texas;United States
Orange juice, not from concentrate;Dairy;1.99;Byron;New York;United States
Rye bread, seeded;Baked goods;0.99;West Long Branch;New Jersey;United States
Bananas;Produce;0.33;Venlo;Limburg;Netherlands
Wheat crackers (triscuit);Snack;0.99;Holguin;Holguin;Cuba
Peppers, tricolor pack;Produce;1.99;Cushing;Texas;United States
Wheat crackers (triscuit);Snack;0.99;Las Vegas;Nevada;United States
Peppers, tricolor pack;Produce;1.99;Roxborough;Tobago;Trinidad and Tobago
Snack crackers (ritz);Snack;1.29;Greymouth;West Coast;New Zealand
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Mount Hope;Ohio;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Blountsville;Alabama;United States
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Greymouth;West Coast;New Zealand
Sausage, brown/serve;Frozen;0.99;Desmet;Idaho;United States
Waffles;Frozen;0.99;Byron;New York;United States
Tyson roasted chicken;Meat;3.99;Franklin Springs;New York;United States
Eggs, large;Dairy;0.89;Scarborough;Tobago;Trinidad and Tobago
Beef/bean burrito;Frozen;0.29;Crooks;South Dakota;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;Tokyo;Tokyo;Japan
Lite drink mix (Crystal Lite);Beverages;1.89;Milano;Lombardia;Italy
Vegetarian vegetable condensed soup;Canned;0.43;Vienna;Georgia;United States
Peppers, green only;Produce;1.39;Thousandsticks;Kentucky;United States
Deluxe topping pizza;Frozen;1.99;La Rochelle;Poitou-Charentes;France
Spoon size shredded wheat;Cereal;1.69;Cushing;Texas;United States
L'Oven Fresh white bread;Baked goods;0.59;Greymouth;West Coast;New Zealand
Celery;Produce;0.99;Rogersville;Alabama;United States
Wheat crackers (triscuit);Snack;0.99;Vienna;Georgia;United States
Deluxe topping pizza;Frozen;1.99;Lone Pine;California;United States
Single serve fruit/gel bowls;Canned;1.39;Port Harcourt;Rivers;Nigeria
Single serve peach cups;Canned;1.39;Hamburg;Illinois;United States
Deluxe topping pizza;Frozen;1.99;Hechi;Guangxi;China
Baking soda;Baking;0.39;New York;New York;United States
Broccoli stir fry mixture;Frozen;0.89;Finlayson;Minnesota;United States
Peppers, tricolor pack;Produce;1.99;Milano;Lombardia;Italy
Milk, 2%;Dairy;2.69;Mill Creek;Oklahoma;United States
Mushrooms;Produce;0.99;Los Angeles;California;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Vienna;Georgia;United States
Cucumber (large);Produce;0.49;Herbster;Wisconsin;United States
Broccoli stir fry mixture;Frozen;0.89;Ormond Beach;Florida;United States
Peppers, tricolor pack;Produce;1.99;Sofia;Sofiya;Bulgaria
Cheese, Colby brick;Dairy;1.49;Borden;Indiana;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;Jerusalem;Yerushalayim  (Jerusalem);Israel
Texas cheese toast;Frozen;1.49;Santiago;Santiago;Dominican Republic
Milk, 2%;Dairy;2.69;Hamburg;Hamburg;Germany
Tyson roasted chicken;Meat;3.99;Jerusalem;Yerushalayim  (Jerusalem);Israel
Cat litter, scoopable;Nonfoods;2.99;Herbster;Wisconsin;United States
Tissues;Nonfoods;0.89;Tokyo;Tokyo;Japan
Wheat crackers (triscuit);Snack;0.99;Douglas City;California;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Jose Leon Suarez;Buenos Aires;Argentina
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;North Grafton;Massachusetts;United States
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Hamburg;Arkansas;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Fengjie;Sichuan;China
Tissues;Nonfoods;0.89;Tokyo;Tokyo;Japan
Pudding mix, choc/vanilla;Baking;0.29;Cattaraugus;New York;United States
Milk, 2%;Dairy;2.69;Malone;New York;United States
Wheat crackers (triscuit);Snack;0.99;Houston;Ohio;United States
Waffles;Frozen;0.99;Thousandsticks;Kentucky;United States
Sausage, brown/serve;Frozen;0.99;Eau Galle;Wisconsin;United States
Peppers, tricolor pack;Produce;1.99;Greymouth;West Coast;New Zealand
Applesauce cups (reg/cinnamon);Canned;0.89;Thousandsticks;Kentucky;United States
Milk, 2%;Dairy;2.69;Hamburg;Illinois;United States
Pudding mix, choc/vanilla;Baking;0.29;Finlayson;Minnesota;United States
Vegetarian vegetable condensed soup;Canned;0.43;Elizabeth;New Jersey;United States
Orange juice, not from concentrate;Dairy;1.99;Seven Rivers;Westmoreland;Jamaica
Deluxe topping pizza;Frozen;1.99;Roma;Texas;United States
Rye bread, seeded;Baked goods;0.99;Paris;Arkansas;United States
Sausage, brown/serve;Frozen;0.99;Hamburg;Arkansas;United States
Cucumber (large);Produce;0.49;Wallops Island;Virginia;United States
Cinnamon Grahams;Snack;0.99;Plymouth;Tobago;Trinidad and Tobago
Potatoes, Russett;Produce;0.89;Hamburg;Hamburg;Germany
Snack crackers (ritz);Snack;1.29;Bangkok;Krung Thep Mahanakhon;Thailand
Sausage, brown/serve;Frozen;0.99;Elizabeth;New Jersey;United States
Deluxe topping pizza;Frozen;1.99;Prudence Island;Rhode Island;United States
Tissues;Nonfoods;0.89;Scarborough;Tobago;Trinidad and Tobago
Cucumber (large);Produce;0.49;Molodesjnaja;Enderby Land;Antarctica
Tyson roasted chicken;Meat;3.99;Cairo;Illinois;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Chicago;Illinois;United States
Peppers, green only;Produce;1.39;Cushing;Texas;United States
Wheat crackers (triscuit);Snack;0.99;London;Ontario;Canada
Cookies, chocolate sandwich (Oreo);Snack;0.99;Marthaville;Louisiana;United States
Larado paper plates;Nonfoods;0.89;Thousandsticks;Kentucky;United States
Snack crackers (ritz);Snack;1.29;Plymouth;Tobago;Trinidad and Tobago
Wheat crackers (triscuit);Snack;0.99;Paris;Arkansas;United States
Saltines;Snack;0.69;Wallops Island;Virginia;United States
Butter;Dairy;1.99;Mount Hope;Ohio;United States
Cat litter, scoopable;Nonfoods;2.99;Finlayson;Minnesota;United States
Broccoli stir fry mixture;Frozen;0.89;Beijing;Beijing;China
Celery;Produce;0.99;Sofia;Sofiya;Bulgaria
Saltines;Snack;0.69;Malone;New York;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;Davos;Graubunden;Switzerland
Eggs, large;Dairy;0.89;Scarborough;Tobago;Trinidad and Tobago
Baking soda;Baking;0.39;Port Elizabeth;New Jersey;United States
Wheat crackers (triscuit);Snack;0.99;Warsaw;Mazowieckie;Poland
Lite drink mix (Crystal Lite);Beverages;1.89;Desmet;Idaho;United States
Tyson roasted chicken;Meat;3.99;Plymouth;Tobago;Trinidad and Tobago
Belmont chocolate chip cookies;Snack;1.49;Athens;Attiki;Greece
Waffles;Frozen;0.99;San Francisco;California;United States
Peppers, tricolor pack;Produce;1.99;Cushing;Texas;United States
Cinnamon Grahams;Snack;0.99;Tokyo;Tokyo;Japan
Beef/bean burrito;Frozen;0.29;Vest;Kentucky;United States
Belmont chocolate chip cookies;Snack;1.49;Helsinki;Etela-Suomen Laani;Finland
Texas cheese toast;Frozen;1.49;Scarborough;Tobago;Trinidad and Tobago
Lite drink mix (Crystal Lite);Beverages;1.89;Finlayson;Minnesota;United States
Deluxe topping pizza;Frozen;1.99;Sofia;Sofiya;Bulgaria
Single serve peach cups;Canned;1.39;Campeche;Campeche;Mexico
Eggs, large;Dairy;0.89;Tokyo;Tokyo;Japan
Potatoes, Russett;Produce;0.89;Herbster;Wisconsin;United States
Milk, 2%;Dairy;2.69;Herbster;Wisconsin;United States
Spoon size shredded wheat;Cereal;1.69;Elizabeth;New Jersey;United States
Peppers, tricolor pack;Produce;1.99;Greymouth;West Coast;New Zealand
Cinnamon Grahams;Snack;0.99;Paris;Arkansas;United States
Single serve peach cups;Canned;1.39;Crooks;South Dakota;United States
Sweetener packets;Baking;1.49;Herbster;Wisconsin;United States
Peppers, green only;Produce;1.39;Paris;Arkansas;United States
Deluxe topping pizza;Frozen;1.99;Thousandsticks;Kentucky;United States
Deluxe topping pizza;Frozen;1.99;Thelma;Kentucky;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;Aalestrup;Nordjylland;Denmark
Baking soda;Baking;0.39;Shanghai;Shanghai;China
Sausage, brown/serve;Frozen;0.99;Chicago;Illinois;United States
Butter;Dairy;1.99;St Andrews;Scotland;United Kingdom
Spoon size shredded wheat;Cereal;1.69;Gafsa;Qafsah;Tunisia
Cucumber (large);Produce;0.49;Port Harcourt;Rivers;Nigeria
Pudding mix, choc/vanilla;Baking;0.29;Wellington;Wellington;New Zealand
Broccoli stir fry mixture;Frozen;0.89;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Mushrooms;Produce;0.99;Borden;Indiana;United States
Larado paper plates;Nonfoods;0.89;Helsinki;Etela-Suomen Laani;Finland
Pudding mix, choc/vanilla;Baking;0.29;Jose Leon Suarez;Buenos Aires;Argentina
Pudding mix, choc/vanilla;Baking;0.29;Blountsville;Alabama;United States
Milk, 2%;Dairy;2.69;Roxborough;Tobago;Trinidad and Tobago
Applesauce cups (reg/cinnamon);Canned;0.89;Blountsville;Alabama;United States
Eggs, large;Dairy;0.89;North Grafton;Massachusetts;United States
Wheat crackers (triscuit);Snack;0.99;Trondheim;Sor-Trondelag;Norway
Sweetener packets;Baking;1.49;Elizabeth;New Jersey;United States
Eggs, large;Dairy;0.89;Mount Hope;Ohio;United States
Wheat crackers (triscuit);Snack;0.99;Saratoga;North Carolina;United States
Pudding mix, choc/vanilla;Baking;0.29;Campeche;Campeche;Mexico
Wheat crackers (triscuit);Snack;0.99;Jose Leon Suarez;Buenos Aires;Argentina
Orange juice, not from concentrate;Dairy;1.99;Hamburg;Hamburg;Germany
Spoon size shredded wheat;Cereal;1.69;Tornillo;Texas;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Helsinki;Etela-Suomen Laani;Finland
Orange juice, not from concentrate;Dairy;1.99;Gafsa;Qafsah;Tunisia
Potatoes, Russett;Produce;0.89;Santa Fe;Tennessee;United States
Eggs, large;Dairy;0.89;San Francisco;California;United States
Sausage, brown/serve;Frozen;0.99;New York;New York;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Kent City;Michigan;United States
Mushrooms;Produce;0.99;Eau Galle;Wisconsin;United States
Rye bread, seeded;Baked goods;0.99;Ormond Beach;Florida;United States
Celery;Produce;0.99;Meadowlands;Minnesota;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Finlayson;Minnesota;United States
Spoon size shredded wheat;Cereal;1.69;Byron;New York;United States
L'Oven Fresh white bread;Baked goods;0.59;Port Elizabeth;New Jersey;United States
Cucumber (large);Produce;0.49;Gafsa;Qafsah;Tunisia
Peppers, green only;Produce;1.39;Stirling;Scotland;United Kingdom
Peppers, green only;Produce;1.39;Roxborough;Tobago;Trinidad and Tobago
Pudding mix, choc/vanilla;Baking;0.29;Djerba;Madanin;Tunisia
Butter;Dairy;1.99;Houston;Ohio;United States
Eggs, large;Dairy;0.89;Wellington;Wellington;New Zealand
Belmont chocolate chip cookies;Snack;1.49;Marthaville;Louisiana;United States
Peppers, tricolor pack;Produce;1.99;Bangkok;Krung Thep Mahanakhon;Thailand
Peppers, green only;Produce;1.39;Kent City;Michigan;United States
Cinnamon Grahams;Snack;0.99;Hamburg;Illinois;United States
Potatoes, Russett;Produce;0.89;Haslev;Vestsjalland;Denmark
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Paris;Arkansas;United States
Celery;Produce;0.99;Greymouth;West Coast;New Zealand
Lite drink mix (Crystal Lite);Beverages;1.89;Madrid;Distrito Capital;Colombia
Snack crackers (ritz);Snack;1.29;Vienna;Georgia;United States
Vegetarian vegetable condensed soup;Canned;0.43;Kent City;Michigan;United States
Cinnamon Grahams;Snack;0.99;Cairo;Illinois;United States
Celery;Produce;0.99;Kyoto;Kyoto;Japan
Saltines;Snack;0.69;Meadowlands;Minnesota;United States
Belmont chocolate chip cookies;Snack;1.49;Glendale;Arizona;United States
Beef/bean burrito;Frozen;0.29;Las Vegas;Nevada;United States
Deluxe topping pizza;Frozen;1.99;Tornillo;Texas;United States
Cat litter, scoopable;Nonfoods;2.99;Bangkok;Krung Thep Mahanakhon;Thailand
Applesauce cups (reg/cinnamon);Canned;0.89;Holguin;Holguin;Cuba
Rye bread, seeded;Baked goods;0.99;Djerba;Madanin;Tunisia
Bananas;Produce;0.33;Kent City;Michigan;United States
Eggs, large;Dairy;0.89;Bangkok;Krung Thep Mahanakhon;Thailand
Lettuce, iceberg;Produce;0.89;Beijing;Beijing;China
Rye bread, seeded;Baked goods;0.99;Scarborough;Tobago;Trinidad and Tobago
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Chicago;Illinois;United States
Belmont chocolate chip cookies;Snack;1.49;Lone Pine;California;United States
Orange juice, not from concentrate;Dairy;1.99;Beaverton;Michigan;United States
Single serve fruit/gel bowls;Canned;1.39;Prudence Island;Rhode Island;United States
Potatoes, Russett;Produce;0.89;Holguin;Holguin;Cuba
Larado paper plates;Nonfoods;0.89;Douglas City;California;United States
Orange juice, not from concentrate;Dairy;1.99;Eau Galle;Wisconsin;United States
Peppers, green only;Produce;1.39;Milano;Lombardia;Italy
Mushrooms;Produce;0.99;Helsinki;Etela-Suomen Laani;Finland
Waffles;Frozen;0.99;Venlo;Limburg;Netherlands
Single serve fruit/gel bowls;Canned;1.39;Shanghai;Shanghai;China
Pudding mix, choc/vanilla;Baking;0.29;Jerusalem;Yerushalayim  (Jerusalem);Israel
Sausage, brown/serve;Frozen;0.99;Kyoto;Kyoto;Japan
Cookies, chocolate sandwich (Oreo);Snack;0.99;Venlo;Limburg;Netherlands
Cinnamon Grahams;Snack;0.99;Jacksonville;Alabama;United States
Peppers, tricolor pack;Produce;1.99;Beaverton;Michigan;United States
Bananas;Produce;0.33;Shanghai;Shanghai;China
Cinnamon Grahams;Snack;0.99;Cogswell;North Dakota;United States
Milk, 2%;Dairy;2.69;Stirling;Scotland;United Kingdom
Lettuce, iceberg;Produce;0.89;Aalestrup;Nordjylland;Denmark
Vegetarian vegetable condensed soup;Canned;0.43;Ormond Beach;Florida;United States
Spoon size shredded wheat;Cereal;1.69;North Grafton;Massachusetts;United States
Broccoli stir fry mixture;Frozen;0.89;Herbster;Wisconsin;United States
Vegetarian vegetable condensed soup;Canned;0.43;Crooks;South Dakota;United States
Celery;Produce;0.99;Jacksonville;Alabama;United States
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Malone;New York;United States
Bananas;Produce;0.33;Jacksonville;Alabama;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;St Andrews;Scotland;United Kingdom
Deluxe topping pizza;Frozen;1.99;Port Harcourt;Rivers;Nigeria
Waffles;Frozen;0.99;Santiago;Santiago;Dominican Republic
Lite drink mix (Crystal Lite);Beverages;1.89;Campeche;Campeche;Mexico
Rye bread, seeded;Baked goods;0.99;Thousandsticks;Kentucky;United States
Broccoli stir fry mixture;Frozen;0.89;Trondheim;Sor-Trondelag;Norway
Deluxe topping pizza;Frozen;1.99;Santiago;Santiago;Dominican Republic
Vegetarian vegetable condensed soup;Canned;0.43;Desmet;Idaho;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;Jose Leon Suarez;Buenos Aires;Argentina
Cheese, Colby brick;Dairy;1.49;Hamburg;Illinois;United States
Beef/bean burrito;Frozen;0.29;Kyoto;Kyoto;Japan
Cucumber (large);Produce;0.49;Greymouth;West Coast;New Zealand
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Graysville;Tennessee;United States
Rye bread, seeded;Baked goods;0.99;Byron;New York;United States
Butter;Dairy;1.99;Jose Leon Suarez;Buenos Aires;Argentina
Tyson roasted chicken;Meat;3.99;Molodesjnaja;Enderby Land;Antarctica
Single serve fruit/gel bowls;Canned;1.39;Vest;Kentucky;United States
Lettuce, iceberg;Produce;0.89;Mount Hope;Ohio;United States
Beef/bean burrito;Frozen;0.29;Milano;Lombardia;Italy
Cookies, chocolate sandwich (Oreo);Snack;0.99;Warsaw;Mazowieckie;Poland
Orange juice, not from concentrate;Dairy;1.99;Haslev;Vestsjalland;Denmark
Tyson roasted chicken;Meat;3.99;Scarborough;Tobago;Trinidad and Tobago
Belmont chocolate chip cookies;Snack;1.49;Haslev;Vestsjalland;Denmark
Texas cheese toast;Frozen;1.49;Kent City;Michigan;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Barcelona;CataluÒa;Spain
Belmont chocolate chip cookies;Snack;1.49;Wallops Island;Virginia;United States
Waffles;Frozen;0.99;Trondheim;Sor-Trondelag;Norway
Wheat crackers (triscuit);Snack;0.99;Meadowlands;Minnesota;United States
Vegetarian vegetable condensed soup;Canned;0.43;Elizabeth;New Jersey;United States
Cucumber (large);Produce;0.49;Greymouth;West Coast;New Zealand
Peppers, tricolor pack;Produce;1.99;Amsterdam;Ohio;United States
Sausage, brown/serve;Frozen;0.99;Madrid;Distrito Capital;Colombia
Mushrooms;Produce;0.99;Kyoto;Kyoto;Japan
Cucumber (large);Produce;0.49;Finlayson;Minnesota;United States
Waffles;Frozen;0.99;Madrid;Distrito Capital;Colombia
Wheat crackers (triscuit);Snack;0.99;Helsinki;Etela-Suomen Laani;Finland
Mushrooms;Produce;0.99;Port Harcourt;Rivers;Nigeria
Rye bread, seeded;Baked goods;0.99;Thelma;Kentucky;United States
L'Oven Fresh white bread;Baked goods;0.59;Douglas City;California;United States
Larado paper plates;Nonfoods;0.89;Franklin Springs;New York;United States
Sweetener packets;Baking;1.49;Cairo;Illinois;United States
Broccoli stir fry mixture;Frozen;0.89;Shanghai;Shanghai;China
Single serve peach cups;Canned;1.39;New York;New York;United States
Broccoli stir fry mixture;Frozen;0.89;Saratoga;North Carolina;United States
Larado paper plates;Nonfoods;0.89;Los Angeles;California;United States
Rye bread, seeded;Baked goods;0.99;Venlo;Limburg;Netherlands
Cheese, Colby brick;Dairy;1.49;Vest;Kentucky;United States
Single serve fruit/gel bowls;Canned;1.39;Finlayson;Minnesota;United States
Peppers, tricolor pack;Produce;1.99;Wellington;Wellington;New Zealand
Beef/bean burrito;Frozen;0.29;La Rochelle;Poitou-Charentes;France
Spoon size shredded wheat;Cereal;1.69;Hamburg;Illinois;United States
Cucumber (large);Produce;0.49;Aalestrup;Nordjylland;Denmark
Lettuce, iceberg;Produce;0.89;Lone Pine;California;United States
Wheat crackers (triscuit);Snack;0.99;Aalestrup;Nordjylland;Denmark
Spoon size shredded wheat;Cereal;1.69;Barcelona;CataluÒa;Spain
Baking soda;Baking;0.39;Bangkok;Krung Thep Mahanakhon;Thailand
Sweetener packets;Baking;1.49;Meadowlands;Minnesota;United States
Baking soda;Baking;0.39;Roxborough;Tobago;Trinidad and Tobago
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Jacksonville;Alabama;United States
Wheat crackers (triscuit);Snack;0.99;Bluff City;Kansas;United States
Baking soda;Baking;0.39;Djerba;Madanin;Tunisia
Larado paper plates;Nonfoods;0.89;Rogersville;Alabama;United States
Vegetarian vegetable condensed soup;Canned;0.43;Warsaw;Mazowieckie;Poland
L'Oven Fresh white bread;Baked goods;0.59;North Grafton;Massachusetts;United States
Texas cheese toast;Frozen;1.49;Fengjie;Sichuan;China
Cookies, chocolate sandwich (Oreo);Snack;0.99;Helsinki;Etela-Suomen Laani;Finland
Belmont chocolate chip cookies;Snack;1.49;Cattaraugus;New York;United States
Cinnamon Grahams;Snack;0.99;Rogersville;Alabama;United States
Sweetener packets;Baking;1.49;Eau Galle;Wisconsin;United States
Vegetarian vegetable condensed soup;Canned;0.43;Eau Galle;Wisconsin;United States
Lettuce, iceberg;Produce;0.89;Tornillo;Texas;United States
Vegetarian vegetable condensed soup;Canned;0.43;Elizabeth;New Jersey;United States
Cinnamon Grahams;Snack;0.99;Los Angeles;California;United States
Peppers, tricolor pack;Produce;1.99;Mount Hope;Ohio;United States
Broccoli stir fry mixture;Frozen;0.89;Madrid;Distrito Capital;Colombia
Belmont chocolate chip cookies;Snack;1.49;Mount Hope;Ohio;United States
Peppers, tricolor pack;Produce;1.99;Kyoto;Kyoto;Japan
Snack crackers (ritz);Snack;1.29;Santiago del Estero;Santiago del Estero;Argentina
Mushrooms;Produce;0.99;La Rochelle;Poitou-Charentes;France
Lite drink mix (Crystal Lite);Beverages;1.89;Franklin Springs;New York;United States
Pudding mix, choc/vanilla;Baking;0.29;London;Ontario;Canada
Eggs, large;Dairy;0.89;Madrid;Distrito Capital;Colombia
Celery;Produce;0.99;Santiago;Santiago;Dominican Republic
Spoon size shredded wheat;Cereal;1.69;Aalestrup;Nordjylland;Denmark
Lettuce, iceberg;Produce;0.89;Amsterdam;Ohio;United States
Cinnamon Grahams;Snack;0.99;Houston;Ohio;United States
Texas cheese toast;Frozen;1.49;Wallops Island;Virginia;United States
Mushrooms;Produce;0.99;Jose Leon Suarez;Buenos Aires;Argentina
Cookies, chocolate sandwich (Oreo);Snack;0.99;Beaverton;Michigan;United States
Lite drink mix (Crystal Lite);Beverages;1.89;San Francisco;California;United States
Broccoli stir fry mixture;Frozen;0.89;Franklin Springs;New York;United States
Peppers, green only;Produce;1.39;Cogswell;North Dakota;United States
Snack crackers (ritz);Snack;1.29;Scarborough;Tobago;Trinidad and Tobago
Snack crackers (ritz);Snack;1.29;Mill Creek;Oklahoma;United States
Saltines;Snack;0.69;Franklin Springs;New York;United States
L'Oven Fresh white bread;Baked goods;0.59;Herbster;Wisconsin;United States
Vegetarian vegetable condensed soup;Canned;0.43;Finlayson;Minnesota;United States
Belmont chocolate chip cookies;Snack;1.49;Beijing;Beijing;China
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Bluff City;Kansas;United States
Peppers, green only;Produce;1.39;Houston;Ohio;United States
Sausage, brown/serve;Frozen;0.99;Elizabeth;New Jersey;United States
Milk, 2%;Dairy;2.69;Helsinki;Etela-Suomen Laani;Finland
Single serve peach cups;Canned;1.39;La Rochelle;Poitou-Charentes;France
Eggs, large;Dairy;0.89;Cologne;Nordrhein-Westfalen;Germany
Sausage, brown/serve;Frozen;0.99;Athens;Attiki;Greece
Single serve peach cups;Canned;1.39;Bangkok;Krung Thep Mahanakhon;Thailand
Cat litter, scoopable;Nonfoods;2.99;Prudence Island;Rhode Island;United States
Cheese, Colby brick;Dairy;1.49;Barcelona;CataluÒa;Spain
Waffles;Frozen;0.99;London;Ontario;Canada
Texas cheese toast;Frozen;1.49;Vienna;Georgia;United States
Cookies, chocolate sandwich (Oreo);Snack;0.99;Meadowlands;Minnesota;United States
Eggs, large;Dairy;0.89;Sofia;Sofiya;Bulgaria
L'Oven Fresh white bread;Baked goods;0.59;Tokyo;Tokyo;Japan
Tyson roasted chicken;Meat;3.99;Crooks;South Dakota;United States
Single serve fruit/gel bowls;Canned;1.39;Jose Leon Suarez;Buenos Aires;Argentina
Beef/bean burrito;Frozen;0.29;Saratoga;North Carolina;United States
Pudding mix, choc/vanilla;Baking;0.29;Kyoto;Kyoto;Japan
Waffles;Frozen;0.99;Los Angeles;California;United States
Beef/bean burrito;Frozen;0.29;Elizabeth;New Jersey;United States
Vegetarian vegetable condensed soup;Canned;0.43;Trondheim;Sor-Trondelag;Norway
Deluxe topping pizza;Frozen;1.99;Herbster;Wisconsin;United States
Potatoes, Russett;Produce;0.89;Chicago;Illinois;United States
Sausage, brown/serve;Frozen;0.99;Victoria Falls;Matabeleland North;Zimbabwe
Baking soda;Baking;0.39;Barcelona;CataluÒa;Spain
Bananas;Produce;0.33;Sliema;Malta;Malta
Baking soda;Baking;0.39;Aalestrup;Nordjylland;Denmark
Butter;Dairy;1.99;Las Vegas;Nevada;United States
Butter;Dairy;1.99;Warsaw;Mazowieckie;Poland
Snack crackers (ritz);Snack;1.29;Port Harcourt;Rivers;Nigeria
Milk, 2%;Dairy;2.69;Madrid;Distrito Capital;Colombia
Peppers, tricolor pack;Produce;1.99;Cattaraugus;New York;United States
Orange juice, not from concentrate;Dairy;1.99;Byron;New York;United States
Sweetener packets;Baking;1.49;Roma;Texas;United States
Cheese, Colby brick;Dairy;1.49;Roxborough;Tobago;Trinidad and Tobago
Orange juice, not from concentrate;Dairy;1.99;Eau Galle;Wisconsin;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Graysville;Tennessee;United States
Vegetarian vegetable condensed soup;Canned;0.43;Meadowlands;Minnesota;United States
Lettuce, iceberg;Produce;0.89;Molodesjnaja;Enderby Land;Antarctica
Mushrooms;Produce;0.99;Sliema;Malta;Malta
Lite drink mix (Crystal Lite);Beverages;1.89;Marthaville;Louisiana;United States
Lettuce, iceberg;Produce;0.89;Douglas City;California;United States
Butter;Dairy;1.99;Paris;Arkansas;United States
Saltines;Snack;0.69;Trondheim;Sor-Trondelag;Norway
Butter;Dairy;1.99;Rogersville;Alabama;United States
Eggs, large;Dairy;0.89;Campeche;Campeche;Mexico
Single serve fruit/gel bowls;Canned;1.39;Milano;Lombardia;Italy
Belmont chocolate chip cookies;Snack;1.49;Wellington;Wellington;New Zealand
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Molodesjnaja;Enderby Land;Antarctica
Single serve peach cups;Canned;1.39;Jerusalem;Yerushalayim  (Jerusalem);Israel
Belmont chocolate chip cookies;Snack;1.49;North Grafton;Massachusetts;United States
Vegetarian vegetable condensed soup;Canned;0.43;Franklin Springs;New York;United States
Waffles;Frozen;0.99;Helsinki;Etela-Suomen Laani;Finland
Lite drink mix (Crystal Lite);Beverages;1.89;Franklin Springs;New York;United States
Cheese, Colby brick;Dairy;1.49;Roma;Texas;United States
Single serve peach cups;Canned;1.39;Beijing;Beijing;China
Snack crackers (ritz);Snack;1.29;Sliema;Malta;Malta
Deluxe topping pizza;Frozen;1.99;Djerba;Madanin;Tunisia
Butter;Dairy;1.99;Helsinki;Etela-Suomen Laani;Finland
Peppers, tricolor pack;Produce;1.99;Tokyo;Tokyo;Japan
Beef/bean burrito;Frozen;0.29;Rogersville;Alabama;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Kent City;Michigan;United States
Lite drink mix (Crystal Lite);Beverages;1.89;Beaverton;Michigan;United States
Larado paper plates;Nonfoods;0.89;Elizabeth;New Jersey;United States
Bananas;Produce;0.33;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Vegetarian vegetable condensed soup;Canned;0.43;Graysville;Tennessee;United States
Mushrooms;Produce;0.99;Paris;Ile-De-France;France
Milk, 2%;Dairy;2.69;Los Angeles;California;United States
Sweetener packets;Baking;1.49;Desmet;Idaho;United States
Single serve fruit/gel bowls;Canned;1.39;Paris;Ile-De-France;France
Mushrooms;Produce;0.99;Malone;New York;United States
Tyson roasted chicken;Meat;3.99;Mill Creek;Oklahoma;United States
Saltines;Snack;0.69;Finlayson;Minnesota;United States
Pudding mix, choc/vanilla;Baking;0.29;Sofia;Sofiya;Bulgaria
Cheese, Colby brick;Dairy;1.49;Haslev;Vestsjalland;Denmark
Peppers, green only;Produce;1.39;Paris;Arkansas;United States
Larado paper plates;Nonfoods;0.89;Graysville;Tennessee;United States
Broccoli stir fry mixture;Frozen;0.89;Eau Galle;Wisconsin;United States
Wheat crackers (triscuit);Snack;0.99;Jerusalem;Yerushalayim  (Jerusalem);Israel
Tissues;Nonfoods;0.89;Paris;Arkansas;United States
Cat litter, scoopable;Nonfoods;2.99;Marthaville;Louisiana;United States
Lettuce, iceberg;Produce;0.89;Hamburg;Arkansas;United States
Single serve fruit/gel bowls;Canned;1.39;Santiago del Estero;Santiago del Estero;Argentina
Butter;Dairy;1.99;Las Vegas;Nevada;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Cattaraugus;New York;United States
Baking soda;Baking;0.39;Chicago;Illinois;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Venlo;Limburg;Netherlands
Milk, 2%;Dairy;2.69;London;Ontario;Canada
Mushrooms;Produce;0.99;Ormond Beach;Florida;United States
Butter;Dairy;1.99;Graysville;Tennessee;United States
Celery;Produce;0.99;Haslev;Vestsjalland;Denmark
Waffles;Frozen;0.99;Port Elizabeth;New Jersey;United States
Pudding mix, choc/vanilla;Baking;0.29;Kyoto;Kyoto;Japan
Deluxe topping pizza;Frozen;1.99;Cushing;Texas;United States
Waffles;Frozen;0.99;Santiago;Santiago;Dominican Republic
Applesauce cups (reg/cinnamon);Canned;0.89;Mill Creek;Oklahoma;United States
Rye bread, seeded;Baked goods;0.99;Roma;Texas;United States
Single serve fruit/gel bowls;Canned;1.39;Mount Hope;Ohio;United States
Single serve peach cups;Canned;1.39;Cushing;Texas;United States
Single serve fruit/gel bowls;Canned;1.39;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Bananas;Produce;0.33;Chicago;Illinois;United States
Sausage, brown/serve;Frozen;0.99;Beijing;Beijing;China
Butter;Dairy;1.99;Meadowlands;Minnesota;United States
Milk, 2%;Dairy;2.69;Scarborough;Tobago;Trinidad and Tobago
Potatoes, Russett;Produce;0.89;Franklin Springs;New York;United States
Single serve peach cups;Canned;1.39;Thousandsticks;Kentucky;United States
Lettuce, iceberg;Produce;0.89;Saratoga;North Carolina;United States
Cucumber (large);Produce;0.49;Saratoga;North Carolina;United States
Single serve peach cups;Canned;1.39;Trondheim;Sor-Trondelag;Norway
Single serve peach cups;Canned;1.39;Bluff City;Kansas;United States
Spoon size shredded wheat;Cereal;1.69;Santiago del Estero;Santiago del Estero;Argentina
Peppers, tricolor pack;Produce;1.99;Hechi;Guangxi;China
Tyson roasted chicken;Meat;3.99;Davos;Graubunden;Switzerland
Baking soda;Baking;0.39;Bluff City;Kansas;United States
Sweetener packets;Baking;1.49;Santiago;Santiago;Dominican Republic
Belmont chocolate chip cookies;Snack;1.49;Djerba;Madanin;Tunisia
Tyson roasted chicken;Meat;3.99;Cairo;Illinois;United States
Pudding mix, choc/vanilla;Baking;0.29;Saratoga;North Carolina;United States
L'Oven Fresh white bread;Baked goods;0.59;Bluff City;Kansas;United States
Cinnamon Grahams;Snack;0.99;St Andrews;Scotland;United Kingdom
Cat litter, scoopable;Nonfoods;2.99;West Long Branch;New Jersey;United States
Butter;Dairy;1.99;Elizabeth;New Jersey;United States
Lettuce, iceberg;Produce;0.89;Blountsville;Alabama;United States
Cheese, Colby brick;Dairy;1.49;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Snack crackers (ritz);Snack;1.29;Amsterdam;Ohio;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Borden;Indiana;United States
Spoon size shredded wheat;Cereal;1.69;Borden;Indiana;United States
Single serve peach cups;Canned;1.39;Prudence Island;Rhode Island;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Byron;New York;United States
Butter;Dairy;1.99;Bangkok;Krung Thep Mahanakhon;Thailand
Applesauce cups (reg/cinnamon);Canned;0.89;Madrid;Distrito Capital;Colombia
Deluxe topping pizza;Frozen;1.99;Blountsville;Alabama;United States
Saltines;Snack;0.69;Roxborough;Tobago;Trinidad and Tobago
Milk, 2%;Dairy;2.69;Plymouth;Tobago;Trinidad and Tobago
Waffles;Frozen;0.99;Malone;New York;United States
Peppers, green only;Produce;1.39;La Rochelle;Poitou-Charentes;France
L'Oven Fresh white bread;Baked goods;0.59;Cologne;Nordrhein-Westfalen;Germany
Cinnamon Grahams;Snack;0.99;Cattaraugus;New York;United States
Tissues;Nonfoods;0.89;Amsterdam;Ohio;United States
Cinnamon Grahams;Snack;0.99;Vest;Kentucky;United States
Broccoli stir fry mixture;Frozen;0.89;Jacksonville;Alabama;United States
Mushrooms;Produce;0.99;Marthaville;Louisiana;United States
Potatoes, Russett;Produce;0.89;Holguin;Holguin;Cuba
Cheese, Colby brick;Dairy;1.49;Houston;Ohio;United States
Single serve fruit/gel bowls;Canned;1.39;Fengjie;Sichuan;China
Rye bread, seeded;Baked goods;0.99;Santiago;Santiago;Dominican Republic
Broccoli stir fry mixture;Frozen;0.89;Douglas City;California;United States
Potatoes, Russett;Produce;0.89;Douglas City;California;United States
Tyson roasted chicken;Meat;3.99;Roma;Texas;United States
Rye bread, seeded;Baked goods;0.99;Fengjie;Sichuan;China
Peppers, tricolor pack;Produce;1.99;Cattaraugus;New York;United States
Celery;Produce;0.99;Bluff City;Kansas;United States
Butter;Dairy;1.99;Shanghai;Shanghai;China
Applesauce cups (reg/cinnamon);Canned;0.89;Montpellier;Languedoc-Roussillon;France
Larado paper plates;Nonfoods;0.89;Athens;Attiki;Greece
Belmont chocolate chip cookies;Snack;1.49;Beaverton;Michigan;United States
L'Oven Fresh white bread;Baked goods;0.59;Elizabeth;New Jersey;United States
Saltines;Snack;0.69;Santiago del Estero;Santiago del Estero;Argentina
Spoon size shredded wheat;Cereal;1.69;Kent City;Michigan;United States
Eggs, large;Dairy;0.89;Scarborough;Tobago;Trinidad and Tobago
Broccoli stir fry mixture;Frozen;0.89;Moscow;Moskva;Russia
Cheese, Colby brick;Dairy;1.49;Wallops Island;Virginia;United States
Milk, 2%;Dairy;2.69;Venlo;Limburg;Netherlands
Potatoes, Russett;Produce;0.89;Stirling;Scotland;United Kingdom
Bananas;Produce;0.33;Fengjie;Sichuan;China
Beef/bean burrito;Frozen;0.29;Desmet;Idaho;United States
Cucumber (large);Produce;0.49;Crooks;South Dakota;United States
Orange juice, not from concentrate;Dairy;1.99;Paris;Ile-De-France;France
Lettuce, iceberg;Produce;0.89;Cologne;Nordrhein-Westfalen;Germany
Waffles;Frozen;0.99;Las Vegas;Nevada;United States
Broccoli stir fry mixture;Frozen;0.89;Jacksonville;Alabama;United States
Celery;Produce;0.99;Los Angeles;California;United States
Rye bread, seeded;Baked goods;0.99;West Long Branch;New Jersey;United States
Tissues;Nonfoods;0.89;Santiago;Santiago;Dominican Republic
Rye bread, seeded;Baked goods;0.99;Victoria Falls;Matabeleland North;Zimbabwe
Applesauce cups (reg/cinnamon);Canned;0.89;Mount Hope;Ohio;United States
Peppers, tricolor pack;Produce;1.99;Hechi;Guangxi;China
Eggs, large;Dairy;0.89;Amsterdam;Ohio;United States
Rye bread, seeded;Baked goods;0.99;Amsterdam;Ohio;United States
Cheese, Colby brick;Dairy;1.49;Malone;New York;United States
Wheat crackers (triscuit);Snack;0.99;Malone;New York;United States
Belmont chocolate chip cookies;Snack;1.49;Holguin;Holguin;Cuba
Baking soda;Baking;0.39;Cairo;Illinois;United States
Cinnamon Grahams;Snack;0.99;Cologne;Nordrhein-Westfalen;Germany
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Hamburg;Illinois;United States
Bananas;Produce;0.33;Santiago del Estero;Santiago del Estero;Argentina
Single serve peach cups;Canned;1.39;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Beef/bean burrito;Frozen;0.29;Santiago del Estero;Santiago del Estero;Argentina
Cat litter, scoopable;Nonfoods;2.99;Athens;Attiki;Greece
Baking soda;Baking;0.39;Cologne;Nordrhein-Westfalen;Germany
Tissues;Nonfoods;0.89;London;Ontario;Canada
Cinnamon Grahams;Snack;0.99;Las Vegas;Nevada;United States
Belmont chocolate chip cookies;Snack;1.49;Saratoga;North Carolina;United States
Lettuce, iceberg;Produce;0.89;Hawkinsville;Georgia;United States
Sausage, brown/serve;Frozen;0.99;Byron;New York;United States
Single serve peach cups;Canned;1.39;Amsterdam;Ohio;United States
Orange juice, not from concentrate;Dairy;1.99;North Grafton;Massachusetts;United States
Broccoli stir fry mixture;Frozen;0.89;Kent City;Michigan;United States
Cucumber (large);Produce;0.49;Vest;Kentucky;United States
Broccoli stir fry mixture;Frozen;0.89;Beaverton;Michigan;United States
Snack crackers (ritz);Snack;1.29;Cogswell;North Dakota;United States
Baking soda;Baking;0.39;Glendale;Arizona;United States
Rye bread, seeded;Baked goods;0.99;Sliema;Malta;Malta
Saltines;Snack;0.69;Vest;Kentucky;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Meadowlands;Minnesota;United States
Waffles;Frozen;0.99;Bluff City;Kansas;United States
Snack crackers (ritz);Snack;1.29;Paris;Arkansas;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Fengjie;Sichuan;China
Sweetener packets;Baking;1.49;Paris;Arkansas;United States
Milk, 2%;Dairy;2.69;Greymouth;West Coast;New Zealand
Broccoli stir fry mixture;Frozen;0.89;Rogersville;Alabama;United States
Beef/bean burrito;Frozen;0.29;Byron;New York;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Desmet;Idaho;United States
Bananas;Produce;0.33;Sarajevo;Federation of Bosnia and Herzegovina;Bosnia and Herzegovina
Baking soda;Baking;0.39;Santiago del Estero;Santiago del Estero;Argentina
Wheat crackers (triscuit);Snack;0.99;Byron;New York;United States
Rye bread, seeded;Baked goods;0.99;Tokyo;Tokyo;Japan
Texas cheese toast;Frozen;1.49;Santiago del Estero;Santiago del Estero;Argentina
Cat litter, scoopable;Nonfoods;2.99;Milano;Lombardia;Italy
Cat litter, scoopable;Nonfoods;2.99;Thelma;Kentucky;United States
Tissues;Nonfoods;0.89;Elizabeth;New Jersey;United States
Mushrooms;Produce;0.99;Elizabeth;New Jersey;United States
Mushrooms;Produce;0.99;Lone Pine;California;United States
Lettuce, iceberg;Produce;0.89;Roma;Texas;United States
Butter;Dairy;1.99;Prudence Island;Rhode Island;United States
Saltines;Snack;0.69;Mount Hope;Ohio;United States
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;Madrid;Distrito Capital;Colombia
Sausage, brown/serve;Frozen;0.99;Aalestrup;Nordjylland;Denmark
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Seven Rivers;Westmoreland;Jamaica
Lettuce, iceberg;Produce;0.89;Hechi;Guangxi;China
Deluxe topping pizza;Frozen;1.99;Prudence Island;Rhode Island;United States
Potatoes, Russett;Produce;0.89;Gafsa;Qafsah;Tunisia
Cat litter, scoopable;Nonfoods;2.99;Desmet;Idaho;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Milano;Lombardia;Italy
Tyson roasted chicken;Meat;3.99;Fengjie;Sichuan;China
Deluxe topping pizza;Frozen;1.99;St Andrews;Scotland;United Kingdom
Pudding mix, choc/vanilla;Baking;0.29;Finlayson;Minnesota;United States
Single serve peach cups;Canned;1.39;Bangkok;Krung Thep Mahanakhon;Thailand
Milk, 2%;Dairy;2.69;Herbster;Wisconsin;United States
Pudding mix, choc/vanilla;Baking;0.29;Malone;New York;United States
Texas cheese toast;Frozen;1.49;Mill Creek;Oklahoma;United States
Celery;Produce;0.99;Milano;Lombardia;Italy
Butter;Dairy;1.99;Haslev;Vestsjalland;Denmark
Cheese, Feta, blue or gorgonzola tub;Dairy;1.69;London;Ontario;Canada
Eggs, large;Dairy;0.89;Blountsville;Alabama;United States
Tissues;Nonfoods;0.89;Kyoto;Kyoto;Japan
Rye bread, seeded;Baked goods;0.99;Cattaraugus;New York;United States
Bananas;Produce;0.33;Amsterdam;Ohio;United States
Peppers, green only;Produce;1.39;Stirling;Scotland;United Kingdom
Tissues;Nonfoods;0.89;Moscow;Moskva;Russia
Orange juice, not from concentrate;Dairy;1.99;Fengjie;Sichuan;China
Deluxe topping pizza;Frozen;1.99;Sliema;Malta;Malta
Waffles;Frozen;0.99;Helsinki;Etela-Suomen Laani;Finland
Potatoes, Russett;Produce;0.89;Franklin Springs;New York;United States
Milk, 2%;Dairy;2.69;Aalestrup;Nordjylland;Denmark
Celery;Produce;0.99;Cairo;Illinois;United States
Rye bread, seeded;Baked goods;0.99;Vest;Kentucky;United States
Pudding mix, choc/vanilla;Baking;0.29;Campeche;Campeche;Mexico
Cookies, chocolate sandwich (Oreo);Snack;0.99;Tornillo;Texas;United States
Wheat crackers (triscuit);Snack;0.99;Roxborough;Tobago;Trinidad and Tobago
Tissues;Nonfoods;0.89;Chicago;Illinois;United States
Waffles;Frozen;0.99;Campeche;Campeche;Mexico
Waffles;Frozen;0.99;Madrid;Distrito Capital;Colombia
L'Oven Fresh white bread;Baked goods;0.59;Santiago del Estero;Santiago del Estero;Argentina
Sausage, brown/serve;Frozen;0.99;Jose Leon Suarez;Buenos Aires;Argentina
Rye bread, seeded;Baked goods;0.99;Vest;Kentucky;United States
Wheat crackers (triscuit);Snack;0.99;Warsaw;Mazowieckie;Poland
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Madrid;Distrito Capital;Colombia
Waffles;Frozen;0.99;Douglas City;California;United States
Single serve fruit/gel bowls;Canned;1.39;Warsaw;Mazowieckie;Poland
Spoon size shredded wheat;Cereal;1.69;Mount Hope;Ohio;United States
Potatoes, Russett;Produce;0.89;Paris;Arkansas;United States
Celery;Produce;0.99;St Andrews;Scotland;United Kingdom
Applesauce cups (reg/cinnamon);Canned;0.89;Seven Rivers;Westmoreland;Jamaica
Broccoli stir fry mixture;Frozen;0.89;Hamburg;Illinois;United States
Sausage, brown/serve;Frozen;0.99;Jacksonville;Alabama;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Kent City;Michigan;United States
Bananas;Produce;0.33;Ormond Beach;Florida;United States
Cat litter, scoopable;Nonfoods;2.99;Wellington;Wellington;New Zealand
Orange juice, not from concentrate;Dairy;1.99;Douglas City;California;United States
Peppers, tricolor pack;Produce;1.99;Byron;New York;United States
Saltines;Snack;0.69;Fengjie;Sichuan;China
Rye bread, seeded;Baked goods;0.99;Hamburg;Arkansas;United States
Rye bread, seeded;Baked goods;0.99;Trondheim;Sor-Trondelag;Norway
Sweetener packets;Baking;1.49;Stirling;Scotland;United Kingdom
Deluxe topping pizza;Frozen;1.99;Roma;Texas;United States
Texas cheese toast;Frozen;1.49;Borden;Indiana;United States
Bananas;Produce;0.33;Las Vegas;Nevada;United States
Snack crackers (ritz);Snack;1.29;Sliema;Malta;Malta
Sausage, brown/serve;Frozen;0.99;Eau Galle;Wisconsin;United States
Baking soda;Baking;0.39;Wellington;Wellington;New Zealand
Tissues;Nonfoods;0.89;Barcelona;CataluÒa;Spain
Saltines;Snack;0.69;La Rochelle;Poitou-Charentes;France
Cookies, chocolate sandwich (Oreo);Snack;0.99;Sliema;Malta;Malta
Beef/bean burrito;Frozen;0.29;Campeche;Campeche;Mexico
Spoon size shredded wheat;Cereal;1.69;Jerusalem;Yerushalayim  (Jerusalem);Israel
Spoon size shredded wheat;Cereal;1.69;Mount Hope;Ohio;United States
Single serve peach cups;Canned;1.39;Kyoto;Kyoto;Japan
Lettuce, iceberg;Produce;0.89;North Grafton;Massachusetts;United States
Sweetener packets;Baking;1.49;Seven Rivers;Westmoreland;Jamaica
Mushrooms;Produce;0.99;Douglas City;California;United States
Pudding mix, choc/vanilla;Baking;0.29;Hawkinsville;Georgia;United States
Butter;Dairy;1.99;Victoria Falls;Matabeleland North;Zimbabwe
Butter;Dairy;1.99;Jerusalem;Yerushalayim  (Jerusalem);Israel
Peppers, tricolor pack;Produce;1.99;Wallops Island;Virginia;United States
Texas cheese toast;Frozen;1.49;Desmet;Idaho;United States
Cheese, Deli style slices, Swiss, Provolone or Muenster;Dairy;1.69;Davos;Graubunden;Switzerland
Spoon size shredded wheat;Cereal;1.69;Stirling;Scotland;United Kingdom
Mushrooms;Produce;0.99;Wallops Island;Virginia;United States
Mushrooms;Produce;0.99;Athens;Attiki;Greece
Mushrooms;Produce;0.99;Roxborough;Tobago;Trinidad and Tobago
Texas cheese toast;Frozen;1.49;Vest;Kentucky;United States
Wheat crackers (triscuit);Snack;0.99;London;Ontario;Canada
Lite drink mix (Crystal Lite);Beverages;1.89;Saratoga;North Carolina;United States
Deluxe topping pizza;Frozen;1.99;Milano;Lombardia;Italy
Vegetarian vegetable condensed soup;Canned;0.43;Davos;Graubunden;Switzerland
Texas cheese toast;Frozen;1.49;Herbster;Wisconsin;United States
Wheat crackers (triscuit);Snack;0.99;San Francisco;California;United States
Pudding mix, choc/vanilla;Baking;0.29;West Long Branch;New Jersey;United States
Baking soda;Baking;0.39;Hamburg;Arkansas;United States
Peppers, green only;Produce;1.39;Montpellier;Languedoc-Roussillon;France
Wheat crackers (triscuit);Snack;0.99;Malone;New York;United States
Cucumber (large);Produce;0.49;Byron;New York;United States
Larado paper plates;Nonfoods;0.89;Venlo;Limburg;Netherlands
Lite drink mix (Crystal Lite);Beverages;1.89;Thousandsticks;Kentucky;United States
Orange juice, not from concentrate;Dairy;1.99;Desmet;Idaho;United States
Tyson roasted chicken;Meat;3.99;Cattaraugus;New York;United States
Applesauce cups (reg/cinnamon);Canned;0.89;Glendale;Arizona;United States
Single serve fruit/gel bowls;Canned;1.39;Port Harcourt;Rivers;Nigeria
Saltines;Snack;0.69;Montpellier;Languedoc-Roussillon;France
Applesauce cups (reg/cinnamon);Canned;0.89;Vienna;Georgia;United States
Orange juice, not from concentrate;Dairy;1.99;Santiago;Santiago;Dominican Republic
Texas cheese toast;Frozen;1.49;Roxborough;Tobago;Trinidad and Tobago
Cucumber (large);Produce;0.49;North Grafton;Massachusetts;United States
Tyson roasted chicken;Meat;3.99;Eau Galle;Wisconsin;United States
Deluxe topping pizza;Frozen;1.99;Thousandsticks;Kentucky;United States

SELECT SUM(PRICE) as PRICE, ITEM, CATEGORY, CITY, REGION, COUNTRY
FROM  (SELECT MAX(price) as MAXPRICE FROM SALESMART) T, SALESMART S
WHERE T.MAXPRICE = S.PRICE
GROUP BY ITEM, CATEGORY, CITY, REGION, COUNTRY;

SELECT SUM(PRICE) as PRICE, ITEM, CATEGORY, CITY, REGION, COUNTRY
FROM  SALESMART S
WHERE S.PRICE IN (SELECT MAX(price) as MAXPRICE FROM SALESMART)
GROUP BY ITEM, CATEGORY, CITY, REGION, COUNTRY;

DROP TABLE SALESMART;
