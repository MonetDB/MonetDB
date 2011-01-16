-- The Unicode BOM (Byte Order Marker) can exist not only at the start
-- of the file (where mclient strips it for xquery), but anywhere.
-- When that happens the BOM should be ignored.
﻿
-- the line above has nothing but the BOM, the line below starts with one
﻿SELECT 1;
-- next line has the BOM in the middle of the SELECT
SEL﻿ECT 1;
-- finally, more than one BOM scattered over the entire statement
SE﻿LE﻿CT﻿ 1﻿;﻿
