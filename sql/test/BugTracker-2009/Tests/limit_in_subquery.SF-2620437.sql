select id from tables where id not in (select id from columns limit 10);

