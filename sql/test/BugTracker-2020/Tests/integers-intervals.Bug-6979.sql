select extract(second from interval '3600' second), extract(hour from interval '3600' second), extract(epoch from interval '3600' second), extract(epoch from now() - now());
