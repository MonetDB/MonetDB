-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

CREATE TABLE sys.comments (
        id INTEGER NOT NULL PRIMARY KEY,
        remark VARCHAR(65000) NOT NULL
);

CREATE PROCEDURE sys.comment_on(obj_id INTEGER, obj_remark VARCHAR(65000))
BEGIN
        IF obj_remark IS NULL OR obj_remark = '' THEN
                DELETE FROM sys.comments WHERE id = obj_id;
        ELSEIF EXISTS (SELECT id FROM sys.comments WHERE id = obj_id) THEN
                UPDATE sys.comments SET remark = obj_remark WHERE id = obj_id;
        ELSE
                INSERT INTO sys.comments VALUES (obj_id, obj_remark);
        END IF;
END;
