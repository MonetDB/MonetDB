select * from example_view;
update example_view set val3=-1.0 where val1 >= 5;
select * from example_view;

commit;
