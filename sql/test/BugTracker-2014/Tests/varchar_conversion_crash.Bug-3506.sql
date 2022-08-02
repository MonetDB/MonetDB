CREATE TABLE config (id SERIAL ,dataset INT NOT NULL ,section VARCHAR(100) ,key VARCHAR(100) ,value VARCHAR(500) ,type VARCHAR(5) ,UNIQUE (dataset, section, key)) ;

INSERT INTO config (dataset, section, key, value, type) VALUES (6, 'quality_lofar', 'low_bound', 1.9, 'float');
INSERT INTO config (dataset, section, key, value, type) VALUES (6, 'quality_lofar', 'high_bound', 80, 'int');

select * from config;

drop table config;
