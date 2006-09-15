START TRANSACTION;

select boatname from "voyages";
select distinct boatname from "voyages";

commit;
