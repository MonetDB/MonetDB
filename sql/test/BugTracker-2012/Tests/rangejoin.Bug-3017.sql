
CREATE TABLE "sys"."y" (
        "x" BOOLEAN
);

select * from y, y as y1, y as y2 where y.x between y1.x and y2.x;

drop table y;
