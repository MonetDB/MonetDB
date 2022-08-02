SELECT * from tables OFFSET 2147483647; -- never finishes
SELECT * from tables OFFSET 2147483646; -- returns instantly
