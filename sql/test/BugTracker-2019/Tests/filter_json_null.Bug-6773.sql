select json.filter('{"foo": null}', '$.foo');
select json.filter('[null]', '$[0]');
