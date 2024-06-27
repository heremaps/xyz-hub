DROP FUNCTION write_feature_without_history(tbl regclass, context TEXT, historyEnabled BOOLEAN,
    input_feature JSONB, author TEXT, version BIGINT, is_partial BOOLEAN,
    onExists TEXT, onNotExists TEXT, on_version_conflict TEXT, on_merge_conflict TEXT);

-- onExistsResolution           DELETE, REPLACE(default), RETAIN, ERROR
-- onNotExistsResolution:       CREATE(default), ERROR, RETAIN
-- MergeConflictResolution:     ERROR (default), RETAIN, REPLACE
-- VersionConflictResolution:   ERROR, RETAIN, REPLACE (default for DELETE), MERGE (default for WRITE)

select write_feature_without_history(
       'public."test"'::regclass,
        null,   --context
        false,  --context

       '{"id":"id1","properties":{"foo": "bar","foo2":true}}',
       'author', --author
       1,		 --version
       false,	 --partial

       'REPLACE',  --on_exists
       'CREATE', 	 --on_not_exists,
       'ERROR',  --on_version_conflict
       'ERROR' 	 --on_merge_conflict
);

select write_feature_without_history(
       'public."test"'::regclass,
       null,    --context
       false,   --context

       '{"id":"id1","properties":{"foo": null,"foo2":true},"geometry":{"type":"Point","coordinates":[-48.23256,20.12345]}}',
       'author', --author
       1,		 --version
       false,	 --partial

       'REPLACE',   --onExists
       'CREATE',    --on_not_exists,
       'REPLACE',     --on_version_conflict
       'ERROR'      --on_merge_conflict
);

select write_feature_without_history(
       'public."test"'::regclass,
       null,   --context
       false,  --historyEnabled

       '{"id":"id21322","properties":{"foo": "bar","foo2":true}}',
       'author', --author
       1,		 --version
       false,	 --partial

       null,  --onExists
       null, 	 --on_merge_conflict,
       null,  --on_version_conflict
       null 	 --on_merge_conflict
);
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION delete_feature(tbl regclass, id TEXT, version BIGINT, on_version_conflict TEXT )

select delete_feature('test','test4',123, 'dw');
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION handle_version_conflict(input_feature JSONB, on_version_conflict TEXT, on_merge_conflict TEXT, is_partial BOOLEAN) RETURNS JSONB AS $BODY$
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION merge_changes(base_feature JSONB, input_diff JSONB, head_diff JSONB, on_merge_conflict TEXT) RETURNS JSONB AS $BODY$
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION handle_merge_conflict(input_diff JSONB, head_diff JSONB, on_merge_conflict TEXT) RETURNS JSONB AS $BODY$
------------------------------------------------------------------------------------------------------------------------
--DROP FUNCTION write_row(input_feature JSONB, input_head JSONB) RETURNS VOID AS $BODY$
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION write_row(tbl regclass, input_feature JSONB)

select write_row(
   'public."test"'::regclass,
   enrich_feature('{}', 'author' , 1)
);

select write_row(
   'public."test"'::regclass,
   enrich_feature('{"id":"test"}', 'author' , 1)
);
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION write_row(tbl regclass, id TEXT, version BIGINT, operation CHAR, author TEXT, jsondata JSONB, geo JSONB)

select write_row('public."test"'::regclass, 'test4'::TEXT, 999::BIGINT, 'I'::CHAR, 'author'::TEXT, '{}'::JSONB, '{"type":"Point","coordinates":[-48.23456,20.12345]}');
select write_row('public."test"'::regclass, 'test3'::TEXT, 999::BIGINT, 'I'::CHAR, 'author'::TEXT, '{}'::JSONB, NULL::JSONB);
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION update_row(tbl regclass, input_feature JSONB)

select update_row(
   'public."test"'::regclass,
   enrich_feature('{"id":"test2","geometry":{"type":"Point","coordinates":[-48.23456,20.12345]},"properties":{"foo":"bar"}}', 'author' , 1)
);
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION update_row(tbl regclass, id TEXT, version BIGINT, operation CHAR, author TEXT, jsondata JSONB, geo JSONB)

select update_row('public."test"'::regclass, '123'::TEXT, 999::BIGINT, 'I'::CHAR, 'author'::TEXT, '{"test":2}'::JSONB, '{"type":"Point","coordinates":[-48.23456,20.12345]}');
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION loadFeature(tbl regclass, id TEXT, version BIGINT = -1)

select * from loadFeature('public."test"'::regclass, 'test')
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION diff(minuend JSONB, subtrahend JSONB)

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
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION patch(target JSONB, input_diff JSONB)

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
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION find_conflicts(obj1 JSONB, obj2 JSONB, path TEXT)

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
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION has_version_conflict(input_feature JSONB, head_feature JSONB)

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
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION enrich_feature(input_feature JSONB, author TEXT, version BIGINT)

SELECT enrich_feature('{}', null , 0)

SELECT enrich_feature('{
  "id": "eoznlmxa",
  "type": "Feature",
  "properties": {
    "@ns:com:here:xyz": {
      "author": "ANOYMOUS",
      "version": "0",
      "createdAt": 1719229508037,
      "updatedAt": 1719229508037
    }
  }
}', null , 0)
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION resolveOperation(input_feature JSONB, head_feature JSONB)

select resolveOperation(null::JSONB, '{}'::JSONB);

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
------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION clean_feature(input_feature JSONB)

select clean_feature('{
           "properties": {
               "@ns:com:here:xyz" : {
                   "version" : 12,
					"author" : "someone"
               }
           },
			"geometry" : {
				"crs2" : { "test" : true}
			}
       }'::JSONB);

------------------------------------------------------------------------------------------------------------

select context('{
		"schema" : "public",
		"table" : "iml-import-test-max:test-layer-max-2",
		"context" : "default",
		"historyEnabled" : false
	}'::JSONB);

select write_feature(
               '{"id":"id3","properties":{"foo232": "bar","foo2":null},"geometry":null}',  --feature
               1,       --version
               'test',	 --author
               null,  	--on_exists
               null, 	--on_not_exists,
               null,		--on_version_conflict
               null,		--on_merge_conflict
               false 		--isPartial
       );