select s1.framenumber, avg(c.zanger_c)
 from concepts c, video_segment s1, video_segment s2, video v
 where c.video_segment_id = s2.video_segment_id
  and s1.video_id = v.video_id
  and s2.video_id = v.video_id
  and v.media_uri = '20050407_mocky.mpg'
  and s2.framenumber between s1.framenumber and (s1.framenumber + 124)
 group by s1.framenumber
 order by s1.framenumber;
