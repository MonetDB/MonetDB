-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

-- (co) Arjen de Rijke, Bart Scheers
-- Use statistical functions from gsl library

-- Calculate Chi squared probability
create function sys.chi2prob(chi2 double, datapoints double)
returns double external name gsl."chi2prob";
