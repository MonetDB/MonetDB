CREATE FUNCTION factory14(aa INT, bb CLOB) RETURNS TABLE (aa INT, bb CLOB) BEGIN
    YIELD TABLE (SELECT aa, bb);
    SET aa = aa + 1;
    SET bb = 'just';
    YIELD TABLE (SELECT aa, bb);
    SET aa = aa + 1;
    SET bb = 'other';
    YIELD TABLE (SELECT aa, bb);
    SET aa = aa + 1;
    SET bb = 'string';
    YIELD TABLE (SELECT aa, bb);
END;

SELECT aa, bb FROM factory14(0, '');
SELECT aa, bb FROM factory14(0, '');
SELECT aa, bb FROM factory14(0, '');
SELECT aa, bb FROM factory14(0, '');
SELECT aa, bb FROM factory14(0, ''); --error

DROP FUNCTION factory14;
