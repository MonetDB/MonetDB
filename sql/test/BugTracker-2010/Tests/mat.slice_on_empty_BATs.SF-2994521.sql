CREATE TABLE category_entry (
    category VARCHAR(32)
);
CREATE TABLE available_segment (
    category VARCHAR(32),
    segment CHAR(1)
);
INSERT
    INTO available_segment (segment,category)
    SELECT 'a','alpha';
INSERT
    INTO available_segment (segment,category)
    SELECT 'a','alpha';
DELETE
    FROM available_segment
    WHERE NOT EXISTS (
        SELECT true
            FROM category_entry
            WHERE available_segment.category=category_entry.category
    );
SELECT segment,category
    FROM available_segment
    LIMIT 1;
DROP TABLE available_segment;
DROP TABLE category_entry;
