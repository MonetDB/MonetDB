
START TRANSACTION;

# decimal
# decimals are integer types (bit, short, int or lng)
# each decimal type has a "scale" (supplied as double)
# this scale is the scaling factor of the value
# for example, the value "10.3" may be stored as "103" with scaling factor "10"
# to convert to double: 103 / 10.0 = 10.3
# to convert a constant back into the decimal: 10 * 10 = 100 (which is equal to )

CREATE FUNCTION capi11(inp DECIMAL) RETURNS DECIMAL(11,1) LANGUAGE C {
    size_t i;
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        if (inp.data[i] == inp.null_value) {
            result->data[i] = result->null_value;
        } else {
            result->data[i] = (inp.data[i] / inp.scale) * result->scale;
        }
    }
};

CREATE TABLE decimals(d DECIMAL(18,3));
INSERT INTO decimals VALUES (10.3), (10.5), (NULL), (10.7);

SELECT capi11(d) FROM decimals;

DROP FUNCTION capi11;

# to convert from a decimal to a double simply divide the value by the scale
CREATE FUNCTION _dec2dbl(inp DECIMAL) RETURNS DOUBLE LANGUAGE C {
    size_t i;
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        if (inp.data[i] == inp.null_value) {
            result->data[i] = result->null_value;
        } else {
            result->data[i] = inp.data[i] / inp.scale;
        }
    }
};

SELECT _dec2dbl(d) FROM decimals;

DROP TABLE decimals;
DROP FUNCTION _dec2dbl;

# to convert a double value to a decimal multiply by the scale
CREATE FUNCTION _dbl2dec(inp DOUBLE) RETURNS DECIMAL LANGUAGE C {
    size_t i;
    result->initialize(result, inp.count);
    for(i = 0; i < inp.count; i++) {
        result->data[i] = inp.data[i] * result->scale;
    }
};

CREATE TABLE doubles(d DOUBLE);
INSERT INTO doubles VALUES (10.3), (10.5), (10.7);


SELECT _dbl2dec(d) FROM doubles;


ROLLBACK;
