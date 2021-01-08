-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.

CREATE SCHEMA logging;

-- Flush the buffer
CREATE PROCEDURE logging.flush()
        EXTERNAL NAME logging.flush;

-- Sets the log level for a specific component
CREATE PROCEDURE logging.setcomplevel(comp_id STRING, lvl_id STRING)
        EXTERNAL NAME logging.setcomplevel;

-- Resets the log level for a specific component back to the default
CREATE PROCEDURE logging.resetcomplevel(comp_id STRING)
        EXTERNAL NAME logging.resetcomplevel;

-- Sets the log level for a specific layer
CREATE PROCEDURE logging.setlayerlevel(layer_id STRING, lvl_id STRING)
        EXTERNAL NAME logging.setlayerlevel;

-- Resets the log level for a specific layer back to the default
CREATE PROCEDURE logging.resetlayerlevel(layer_id STRING)
        EXTERNAL NAME logging.resetlayerlevel;

-- Sets the flush level
CREATE PROCEDURE logging.setflushlevel(lvl_id STRING)
       EXTERNAL NAME logging.setflushlevel;

-- Resets the flush level back to the default
CREATE PROCEDURE logging.resetflushlevel()
       EXTERNAL NAME logging.resetflushlevel;

-- Sets the adapter
CREATE PROCEDURE logging.setadapter(adapter_id STRING)
       EXTERNAL NAME logging.setadapter;

-- Resets the adapter back to the default
CREATE PROCEDURE logging.resetadapter()
       EXTERNAL NAME logging.resetadapter;

-- Returns in the form of a SQL result-set all the 
-- components along with their ID and their current 
-- logging level being set
CREATE FUNCTION logging.compinfo()
RETURNS TABLE(
	"id" int,
	"component" string,
	"log_level" string
)
EXTERNAL NAME logging.compinfo;
GRANT EXECUTE ON FUNCTION logging.compinfo TO public;

CREATE view logging.compinfo AS SELECT * FROM logging.compinfo();
GRANT SELECT ON logging.compinfo TO public;
