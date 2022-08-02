start transaction;

CREATE TABLE exchange (
    pk_symbol VARCHAR(16) NOT NULL,
    name VARCHAR(128) NOT NULL,
    CONSTRAINT exchange_pk PRIMARY KEY (pk_symbol)
);

CREATE TABLE entity (
    pk_uuid UUID NOT NULL,
    name VARCHAR(128) NOT NULL,
    industry VARCHAR(64),
    category VARCHAR(64),
    subcategory VARCHAR(64),
    CONSTRAINT entity_pk PRIMARY KEY (pk_uuid)
);

CREATE TABLE instrument (
    pk_uuid UUID NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    fk_exchange VARCHAR(16) NOT NULL,
    fk_entity_uuid UUID NOT NULL,
    CONSTRAINT instrument_pk PRIMARY KEY (pk_uuid),
-- INCORRECT DOUBLE CONSTRAINT
    CONSTRAINT instrument_exchange_fk FOREIGN KEY (fk_exchange) REFERENCES exchange (pk_symbol),
    CONSTRAINT instrument_exchange_fk FOREIGN KEY (fk_exchange) REFERENCES exchange (pk_symbol),
    CONSTRAINT instrument_entity_fk FOREIGN KEY (fk_entity_uuid) REFERENCES entity (pk_uuid)
);

rollback;
