
select write_feature(
   'public."test"'::regclass,
   '{"id":"test2"}',
   'REPLACE',
   null,
   false,
   null,
   1
);

select delete_feature('test','test4',123, 'dw');

select * from loadFeature('public."test"'::regclass, 'test')

select write_row('public."test"'::regclass, 'test4'::TEXT, 999::BIGINT, 'I'::CHAR, 'author'::TEXT, '{}'::JSONB, ST_GeomFromGeoJSON('{"type":"Point","coordinates":[-48.23456,20.12345]}'));

select write_row('public."test"'::regclass, 'test3'::TEXT, 999::BIGINT, 'I'::CHAR, 'author'::TEXT, '{}'::JSONB, NULL::GEOMETRY);

select update_row('public."test"'::regclass, 'test4'::TEXT, 999::BIGINT, 'U'::CHAR, 'author'::TEXT, '{}'::JSONB, ST_GeomFromGeoJSON('{"type":"Point","coordinates":[-48.23456,20.12345]}'));

select update_row('public."test"'::regclass, 'test4'::TEXT, 999::BIGINT, 'U'::CHAR, 'author'::TEXT, '{}'::JSONB, ST_GeomFromGeoJSON('{"type":"Point","coordinates":[-48.23456,20.12345]}'));

SELECT enrich_feature('{}', null , 0, null)

select resolveOperation(null::JSONB, '{}'::JSONB);

select has_version_conflict(
       '{
           "properties": {
               "@ns:com:here:xyz" : {
                   "version" : 12
               }
           }
       }'::JSONB,
       '{
           "properties": {
               "@ns:com:here:xyz" : {
                   "version" : 13
               }
           }
       }'::JSONB
);

select find_conflicts(
       '{
           "id": "1",
           "type": "Feature",
           "properties": {
               "name": "head",
               "val": 100,
               "nested" : { "foo " : "bar"},
               "keyToDelete": true
           }
       }'::JSONB,
       '{
           "properties": {
               "val": 300,
               "newKey": "newValue",
               "nested" : { "foo2" : "bar"},
               "keyToDelete" : null
           }
       }'::JSONB,
       ''
);

select diff(
       '{
           "id": "1",
           "type": "Feature",
           "properties": {
               "name": "head",
               "val": 100,
               "nested" : { "foo " : "bar"},
               "keyToDelete": true
           }
       }'::JSONB,
       '{
           "properties": {
               "val": 300,
               "newKey": "newValue",
               "nested" : { "foo2" : "bar"},
               "keyToDelete" : null
           }
       }'::JSONB
);

select patch(
       '{
           "id": "1",
           "type": "Feature",
           "properties": {
               "name": "head",
               "val": 100,
               "nested" : { "foo " : "bar"},
               "keyToDelete": true
           }
       }'::JSONB,
       '{
           "properties": {
               "val": 300,
               "newKey": "newValue",
               "nested" : { "foo2" : "bar"},
               "keyToDelete" : null
           }
       }'::JSONB
);


select resolveOperation(
       '{
              "properties": {
                  "@ns:com:here:xyz" : {
                      "deleted" : true
                  }
              }
        }'::JSONB,
       null::JSONB
);

select clean_feature('{
   "properties": {
       "@ns:com:here:xyz" : {
           "version" : 12,
            "author" : "someone"
       }
   },
    "geometry" : {
        "crs" : { "test" : true}
        }
    }'::JSONB
);