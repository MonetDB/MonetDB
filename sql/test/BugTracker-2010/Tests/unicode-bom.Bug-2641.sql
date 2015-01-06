-- The Unicode BOM (Byte Order Marker) is only a BOM when at the start
-- of a file.  Anywhere else it's a ZERO WIDTH NO-BREAK SPACE which we
-- shouldn't ignore.
﻿
-- the line above has nothing but the BOM, the line below starts with one
﻿SELECT 1;
-- next line has the BOM in the middle of the SELECT
SEL﻿ECT 1;
-- finally, more than one BOM scattered over the entire statement
SE﻿LE﻿CT﻿ 1﻿;﻿
