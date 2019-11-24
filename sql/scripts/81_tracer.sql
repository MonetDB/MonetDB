-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.

CREATE SCHEMA logging;

-- Flush the buffer
CREATE PROCEDURE logging.flush()
        EXTERNAL NAME logging.flush;

-- Set the log level for a specific component
CREATE PROCEDURE logging.setcomplevel(comp INT, lvl INT)
        EXTERNAL NAME logging.setcomplevel;

-- Reset the log level for a specific component
CREATE PROCEDURE logging.resetcomplevel(comp INT)
        EXTERNAL NAME logging.resetcomplevel;

-- Set the log level for a specific layer
CREATE PROCEDURE logging.setlayerlevel(layer INT, lvl INT)
        EXTERNAL NAME logging.setlayerlevel;

-- Reset the log level for a specific layer
CREATE PROCEDURE logging.resetlayerlevel(layer INT)
        EXTERNAL NAME logging.resetlayerlevel;

-- Set the flush level
CREATE PROCEDURE logging.setflushlevel(lvl INT)
       EXTERNAL NAME logging.setflushlevel;

-- Reset the flush level
CREATE PROCEDURE logging.resetflushlevel()
       EXTERNAL NAME logging.resetflushlevel;

-- Set the adapter
CREATE PROCEDURE logging.setadapter(adapter INT)
       EXTERNAL NAME logging.setadapter;

-- Reset the adapter
CREATE PROCEDURE logging.resetadapter()
       EXTERNAL NAME logging.resetadapter;
