start transaction;

create table im
  (imageid int
  ,ds_id int
  ,bmaj double
  ,primary key (imageid)
  )
;

create table xtr
  (xtrsrcid int
  ,image_id int
  ,zone int
  ,ra double
  ,decl double
  ,ra_err double
  ,decl_err double
  ,x double
  ,y double
  ,z double
  ,primary key (xtrsrcid)
  ,foreign key (image_id) references im (imageid)
  )
;

create table rc
  (xtrsrc_id int
  ,ds_id int
  ,wm_ra double
  ,wm_decl double
  ,wm_ra_err double
  ,wm_decl_err double
  ,x double
  ,y double
  ,z double
  )
;
insert into rc values (1, 1, 331.79744325500002, 43.448623302500003,
0.11882242138100001, 0.116043821024, 0.63980334843300002, -0.34309630504999999,
0.68770386126299998);

insert into im values (2,1,0.0063888886943499999);

insert into xtr values (1, 2, 43, 331.79750356540063, 43.448645530624432,
0.45809725743310992, 0.48429103640614812, 0.6398034744584673,
-0.34309550549403994, 0.68770414291369697);

select t.xtrsrc_id
  from (SELECT r.xtrsrc_id
              ,r.x * x.x + r.y * x.y + r.z * x.z AS dist
              ,COS(RADIANS(im0.bmaj)) AS dist_lim
          FROM rc r
              ,xtr x
              ,im im0
         WHERE x.image_id = 2
           AND x.image_id = im0.imageid
           AND im0.ds_id = r.ds_id
           AND x.zone BETWEEN CAST(FLOOR(r.wm_decl - im0.bmaj) as integer)
                          AND CAST(FLOOR(r.wm_decl - im0.bmaj) as integer)
           AND x.decl BETWEEN r.wm_decl - im0.bmaj
                          AND r.wm_decl + im0.bmaj
           AND x.ra BETWEEN r.wm_ra - alpha(r.wm_decl, im0.bmaj)
                        AND r.wm_ra + alpha(r.wm_decl, im0.bmaj)
       ) t
 where t.dist > t.dist_lim
;

SELECT r.xtrsrc_id
  FROM rc r
      ,xtr x
      ,im im0
 WHERE x.image_id = 2
   AND x.image_id = im0.imageid
   AND im0.ds_id = r.ds_id
   AND x.zone BETWEEN CAST(FLOOR(r.wm_decl - im0.bmaj) as integer)
                  AND CAST(FLOOR(r.wm_decl - im0.bmaj) as integer)
   AND x.decl BETWEEN r.wm_decl - im0.bmaj
                  AND r.wm_decl + im0.bmaj
   AND x.ra BETWEEN r.wm_ra - alpha(r.wm_decl, im0.bmaj)
                AND r.wm_ra + alpha(r.wm_decl, im0.bmaj)
   AND r.x * x.x + r.y * x.y + r.z * x.z > COS(RADIANS(im0.bmaj))
;

rollback;
