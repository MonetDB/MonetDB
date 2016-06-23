

START TRANSACTION;


CREATE FUNCTION varres_test() RETURNS TABLE(*) LANGUAGE PYTHON {
    return {'i': 5, 'j': 10};  
};

explain SELECT * FROM varres_test();
SELECT * FROM varres_test();


ROLLBACK;
