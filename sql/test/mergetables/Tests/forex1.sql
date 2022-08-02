CREATE MERGE TABLE forex ( clk timestamp, currency string, ts timestamp, bid decimal(12,6), offer decimal(12,6), spread decimal(12,6) );
CREATE TABLE forex1 ( clk timestamp, currency string, ts timestamp, bid decimal(12,6), offer decimal(12,6), spread decimal(12,6) );
ALTER TABLE forex1 SET READ ONLY;
ALTER TABLE forex ADD TABLE forex1;

SELECT X.clk FROM forex AS X;

-- drop the single partition
ALTER TABLE forex DROP TABLE forex1;
DROP TABLE forex1;
DROP TABLE forex;
