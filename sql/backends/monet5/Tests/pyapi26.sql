# Test loading functions from files
# This test is a bit... unique
# We first create a file in the users home directory by using a Python UDF
# Then we create a UDF that points to that file (with a relative path)
# Then we delete the file again with another Python UDF


START TRANSACTION;

CREATE FUNCTION average(i INTEGER) RETURNS INTEGER LANGUAGE PYTHON 'pyapi26_test.py';

CREATE FUNCTION create_file() RETURNS TABLE(i INTEGER) LANGUAGE PYTHON {
    import os
    homedir = os.getenv('HOME');
    f = open("%s/pyapi26_test.py" % (homedir), 'w+')
    f.write("return numpy.mean(i)")
    f.close()  
    return 1
};

CREATE FUNCTION delete_file() RETURNS TABLE(i INTEGER) LANGUAGE PYTHON {
    import os
    homedir = os.getenv('HOME');
    os.remove("%s/pyapi26_test.py" % (homedir))
    return 1
};

SELECT * FROM create_file();

CREATE FUNCTION integers() RETURNS TABLE(i INTEGER) LANGUAGE PYTHON { return numpy.arange(10000) + 1 };

SELECT average(i) FROM integers();

SELECT * FROM delete_file();



ROLLBACK;
