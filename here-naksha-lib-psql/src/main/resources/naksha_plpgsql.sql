-- noinspection SqlResolveForFile @ object-type/"hstore"

-- This script is executed whenever IStorage::initStorage() is invoked.
-- Note: schema and storage_id must not persist out of special characters, only: [0-9a-z_]
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS topology;
CREATE SCHEMA IF NOT EXISTS hint_plan;
CREATE SCHEMA IF NOT EXISTS "${schema}";
SET SESSION search_path TO "${schema}", public, topology;

-- Install extensions
CREATE EXTENSION IF NOT EXISTS btree_gist SCHEMA public;
CREATE EXTENSION IF NOT EXISTS btree_gin SCHEMA public;
CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;
CREATE EXTENSION IF NOT EXISTS postgis_topology SCHEMA topology;
-- Restore search_path, because postgis_topology modifies it.
SET SESSION search_path TO "${schema}", public, topology;
CREATE EXTENSION IF NOT EXISTS pg_hint_plan SCHEMA hint_plan;
-- Restore search_path, because hint_plan modifies it.
SET SESSION search_path TO "${schema}", public, topology;
--CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public;
--CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA public;
CREATE EXTENSION IF NOT EXISTS hstore SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public;
COMMIT;
SET SESSION search_path TO "${schema}", public, topology;

CREATE OR REPLACE FUNCTION nk__________CONSTANTS_________() RETURNS void LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN END $$;

-- Returns the packed Naksha extension version: 16 bit reserved, 16 bit major, 16 bit minor, 16 bit revision.
CREATE OR REPLACE FUNCTION naksha_version() RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN ${version};
END $$;

-- Returns the storage-id of this storage, this is created when the Naksha extension is installed and never changes.
CREATE OR REPLACE FUNCTION naksha_storage_id() RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN '${storage_id}';
END $$;

-- Returns the schema of this storage, this is created when the Naksha extension is installed and never changes.
CREATE OR REPLACE FUNCTION naksha_schema() RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN '${schema}';
END $$;

-- Returns the OID of the schema of the storage.
CREATE OR REPLACE FUNCTION nk_const_schema_oid() RETURNS oid LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN (SELECT oid FROM pg_namespace WHERE nspname = '${schema}');
END $$;

-- The default type for collections.
CREATE OR REPLACE FUNCTION nk_const_collection_type() RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN 'NakshaCollection';
END $$;

-- The constant to keep track of the last error number
CREATE OR REPLACE FUNCTION nk_const_err_no() RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN 'naksha.err_no';
END $$;

-- The constant to keep track of the last error message
CREATE OR REPLACE FUNCTION nk_const_err_msg() RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN 'naksha.err_msg';
END $$;

-- The constant to keep track of the uid used by the last after-trigger when acting upon a DELETE.
CREATE OR REPLACE FUNCTION nk_const_del_uid() RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN 'naksha.del_uid';
END $$;

CREATE OR REPLACE FUNCTION nk__________ERRORS_________() RETURNS void LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN END $$;

-- https://www.postgresql.org/docs/current/errcodes-appendix.html

CREATE OR REPLACE PROCEDURE nk_raise_uninitialized() LANGUAGE 'plpgsql' AS $$ BEGIN
  PERFORM nk_set_error('N0000', 'Session not initialized, please invoke naksha_start_session first and set application_name');
  RAISE EXCEPTION 'Session not initialized, please invoke naksha_start_session first and set application_name' USING ERRCODE='N0000';
END $$;

CREATE OR REPLACE PROCEDURE nk_raise_collection_exists(in _collection text) LANGUAGE 'plpgsql' AS $$ BEGIN
  PERFORM nk_set_error('N0001', format('Collection %L exists', _collection));
  RAISE EXCEPTION 'Collection % exists', _collection USING ERRCODE='N0001';
END $$;

CREATE OR REPLACE PROCEDURE nk_raise_collection_not_exists(in _collection text) LANGUAGE 'plpgsql' AS $$ BEGIN
  PERFORM nk_set_error('N0002', format('Collection %L not exists', _collection));
  RAISE EXCEPTION 'Collection % not exists', _collection USING ERRCODE='N0002';
END $$;

CREATE OR REPLACE PROCEDURE nk_raise_check_violation(in _msg text) LANGUAGE 'plpgsql' AS $$ BEGIN
  PERFORM nk_set_error('23514', _msg);
  RAISE EXCEPTION '%', _msg USING ERRCODE='23514';
END $$;

CREATE OR REPLACE PROCEDURE nk_raise_invalid_parameter_value(in _msg text) LANGUAGE 'plpgsql' AS $$ BEGIN
  PERFORM nk_set_error('22023', _msg);
  RAISE EXCEPTION '%', _msg USING ERRCODE='22023';
END $$;

CREATE OR REPLACE PROCEDURE nk_raise_unique_violation(in _msg text) LANGUAGE 'plpgsql' AS $$ BEGIN
  PERFORM nk_set_error('23505', _msg);
  RAISE EXCEPTION '%', _msg USING ERRCODE='23505';
END $$;

CREATE OR REPLACE PROCEDURE nk_raise_no_data(in _msg text) LANGUAGE 'plpgsql' AS $$ BEGIN
  PERFORM nk_set_error('02000', _msg);
  RAISE EXCEPTION '%', _msg USING ERRCODE='02000';
END $$;

-- 02000 no_data
CREATE OR REPLACE FUNCTION nk_err_no_data(_msg text) RETURNS jsonb LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN nk_set_error('02000', _msg);
END $$;

-- 42P01 undefined_table
CREATE OR REPLACE FUNCTION nk_err_undefined_table(_name text) RETURNS jsonb LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN nk_set_error('42P01', format('Undefined table %L',_name));
END $$;

-- 22023 invalid_parameter_value
CREATE OR REPLACE FUNCTION nk_err_invalid_parameter_value(_msg text) RETURNS jsonb LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN nk_set_error('22023', _msg);
END $$;

-- 23514 check_violation
CREATE OR REPLACE FUNCTION nk_err_check_violation(_msg text) RETURNS jsonb LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN nk_set_error('23514', _msg);
END $$;

-- 23505 unique_violation
CREATE OR REPLACE FUNCTION nk_err_unique_violation(_msg text) RETURNS jsonb LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN nk_set_error('23505', _msg);
END $$;

CREATE OR REPLACE FUNCTION nk__________SEQUENCES_________() RETURNS void LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN END $$;

-- This is quite special, because we roll it over every day.
CREATE SEQUENCE IF NOT EXISTS naksha_tx_id_seq AS int8;
-- Just used to generate UUIDs for the collection pseudo-feature.
CREATE SEQUENCE IF NOT EXISTS naksha_collection_id_seq AS int8;

CREATE OR REPLACE FUNCTION nk__________TXN_STRUCT_________() RETURNS void LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN END $$;

--#DO $$ BEGIN
  CREATE TYPE nk_txn_struct AS (year int, month int, day int, uid int8, ts timestamptz);
--#EXCEPTION WHEN duplicate_object THEN NULL; END; $$;

-- Create a transaction number struct from the raw values.
-- Example: SELECT nk_txn(2023,12,31,98765432101::int8);
CREATE OR REPLACE FUNCTION nk_txn_struct(_year int, _month int, _day int, _uid int8) RETURNS nk_txn_struct LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$
DECLARE
  r nk_txn_struct;
BEGIN
  r.year = _year;
  r.month = _month;
  r.day = _day;
  r.uid = _uid;
  r.ts = make_timestamptz(r.year, r.month, r.day, 0, 0, 0.0, 'UTC');
  RETURN r;
END $$;

-- Unpack the transaction number structure from a packed 64-bit transaction number.
-- Example: SELECT nk_unpack_txn(2023123198765432101);
CREATE OR REPLACE FUNCTION nk_txn_unpack(_txn int8) RETURNS nk_txn_struct LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$
DECLARE
  SEQ_DIVIDER constant int8 := 100000000000;
  r nk_txn_struct;
  v int8;
BEGIN
  r.uid = _txn % SEQ_DIVIDER;
  v = _txn / SEQ_DIVIDER;
  r.day = v % 100;
  v = v / 100;
  r.month = v % 100;
  r.year = v / 100;
  r.ts = make_timestamptz(r.year, r.month, r.day, 0, 0, 0.0, 'UTC');
  RETURN r;
END $$;

-- Pack the transaction number structure into an 64-bit integer.
CREATE OR REPLACE FUNCTION nk_txn_pack(_txn nk_txn_struct) RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$
BEGIN
  RETURN nk_txn(_txn.year, _txn.month, _txn.day, _txn.uid);
END $$;

-- Create a packed transaction number from the given day and uid.
CREATE OR REPLACE FUNCTION nk_txn(_year int, _month int, _day int, _uid int8) RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$
DECLARE
  SEQ_DIVIDER constant int8 := 100000000000;
BEGIN
  RETURN (_year * 10000 + _month * 100 + _day)::int8 * SEQ_DIVIDER + _uid;
END $$;

-- Create a packed transaction number from the given timestamp and uid.
CREATE OR REPLACE FUNCTION nk_txn(_ts timestamptz, _uid int8) RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$
DECLARE
  year int;
  month int;
  day int;
BEGIN
  year = extract('year' FROM _ts);
  month = extract('month' FROM _ts);
  day = extract('day' FROM _ts);
--RAISE NOTICE 'nk_txn(%,%,%,%)',year,month,day,_uid;
  RETURN nk_txn(year, month, day, _uid);
END $$;

CREATE OR REPLACE FUNCTION nk__________GUID_STRUCT_________() RETURNS void LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN END $$;

--#DO $$ BEGIN
  CREATE TYPE nk_guid_struct AS (storage_id text, collection_id text, year int, month int, day int, uid int8, ts timestamptz);
--#EXCEPTION WHEN duplicate_object THEN NULL; END; $$;

-- Create a nk_txn_struct row.
-- Example: SELECT nk_txn(2023,12,31,98765432101::int8);
CREATE OR REPLACE FUNCTION nk_guid_struct(collection_id text, _year int, _month int, _day int, _uid int8)
  RETURNS nk_guid_struct
  LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$
DECLARE
  r nk_guid_struct;
BEGIN
  r.storage_id = naksha_storage_id();
  r.collection_id = collection_id;
  r.year = _year;
  r.month = _month;
  r.day = _day;
  r.uid = _uid;
  r.ts = make_timestamptz(r.year, r.month, r.day, 0, 0, 0.0, 'UTC');
  RETURN r;
END $$;

CREATE OR REPLACE FUNCTION nk_guid(_collection_id text, _ts timestamptz, _uid int8) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$ BEGIN
  RETURN naksha_storage_id()||':'||_collection_id||':'||to_char(_ts, 'YYYY:MM:DD')||':'||_uid;
END $$;

CREATE OR REPLACE FUNCTION nk_guid_pack(_guid nk_guid_struct) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$ BEGIN
  RETURN _guid.storage_id||':'||_guid.collection_id||':'||to_char(_guid.ts, 'YYYY:MM:DD')||':'||_guid.uid;
END; $$;

CREATE OR REPLACE FUNCTION nk_guid_unpack(_guid text) RETURNS nk_guid_struct LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$
DECLARE
  parts text[];
  r nk_guid_struct;
BEGIN
  parts = string_to_array(_guid, ':');
  IF array_length(parts, 1) <> 6 THEN
    CALL nk_raise_invalid_parameter_value('Invalid GUID given: '||_guid);
  END IF;
  r.storage_id = parts[1];
  r.collection_id = parts[2];
  r.year = (parts[3])::int;
  r.month = (parts[4])::int;
  r.day = (parts[5])::int;
  r.uid = (parts[6])::int8;
  r.ts = make_timestamptz(r.year, r.month, r.day, 0, 0, 0.0, 'UTC');
  RETURN r;
END $$;

CREATE OR REPLACE FUNCTION nk__________HELPER_________() RETURNS void LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN END $$;

CREATE OR REPLACE FUNCTION nk_head_partition_id(_id text) RETURNS text LANGUAGE 'plpgsql' STRICT IMMUTABLE AS $$ BEGIN
  RETURN lpad(get_byte(decode(md5(_id), 'hex'), 0)::int::text,3,'0');
END $$;

CREATE OR REPLACE FUNCTION nk_head_partition_name(_collection_id text, _partition_id int)
  RETURNS text
  LANGUAGE 'plpgsql' STRICT IMMUTABLE AS $$
BEGIN
  IF _partition_id < 0 OR _partition_id > 255 THEN
    RETURN _collection_id;
  END IF;
  RETURN _collection_id||'_p'||lpad(_partition_id::text,3,'0');
END $$;

CREATE OR REPLACE FUNCTION nk_lock_id(_name text) RETURNS int8 LANGUAGE 'plpgsql' STRICT IMMUTABLE AS $$ BEGIN
  RETURN ('x'||substr(md5(_name),1,16))::bit(64)::int8;
END $$;

CREATE OR REPLACE FUNCTION nk_key(_key text) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN format('naksha.%I', _key);
END $$;

CREATE OR REPLACE FUNCTION nk_key(_collection text, _key text) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN
  RETURN format('naksha.%I', '_'||md5(_collection)||'_'||_key||'_');
END $$;

CREATE OR REPLACE FUNCTION nk_get_config(_key text) RETURNS text LANGUAGE 'plpgsql' VOLATILE AS $$
DECLARE
  value text = NULL;
BEGIN
  IF _key IS NOT NULL THEN
    value = current_setting(_key, true);
    IF value = '' THEN
      value = NULL;
    END IF;
  END IF;
  RETURN value;
END $$;

CREATE OR REPLACE FUNCTION nk_set_config(_key text, _value text, _is_local bool) RETURNS void LANGUAGE 'plpgsql' VOLATILE AS $$
BEGIN
  IF _key IS NOT NULL THEN
    PERFORM set_config(_key, _value, coalesce(_is_local,false));
  END IF;
END $$;

CREATE OR REPLACE FUNCTION nk_set_error(_err text, _msg text) RETURNS jsonb LANGUAGE 'plpgsql' VOLATILE AS $$
BEGIN
  PERFORM set_config(nk_const_err_no(), _err, true);
  PERFORM set_config(nk_const_err_msg(), _msg, true);
  RETURN jsonb_build_object('err',_err,'msg', _msg);
END $$;

CREATE OR REPLACE FUNCTION nk_random_id() RETURNS text LANGUAGE 'plpgsql' VOLATILE AS $$ BEGIN
  RETURN md5(random()||'_'||clock_timestamp());
END $$;

-- Create a pseudo GRID (Geo-Hash Reference ID) from the feature-id.
-- The GRID is used for distributed processing of features.
CREATE OR REPLACE FUNCTION nk_grid_for_feature_id(_feature_id text) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$
DECLARE
  -- https://en.wikipedia.org/wiki/Geohash#Textual_representation
  base32 constant text[] = ARRAY['0','1','2','3','4','5','6','7','8','9','b','c','d','e','f','g',
                                 'h','j','k','m','n','p','q','r','s','t','u','v','w','x','y','z'];
  hash text;
  m text;
  i int;
  index int8;
BEGIN
  m = md5(_feature_id);
  index = ('x'||substr(m,1,16))::bit(64)::int8;
  hash = '';
  i = 0;
  WHILE i < 7 loop
    hash = hash || base32[((index & x'1f'::int8)::int)+1];
    index = index >> 5;
    i = i + 1;
  END LOOP;
  RETURN hash;
END $$;

-- Create a GRID (Geo-Hash Reference ID) from the given geometry.
-- The GRID is used for distributed processing of features.
-- We use GeoHash at level 7, which uses 17 bits for latitude and 18 bits for longitude (35-bit total).
-- The precision is therefore around 76 x 76 meter (5776m²).
-- https://en.wikipedia.org/wiki/Geohash
-- https://www.movable-type.co.uk/scripts/geohash.html
CREATE OR REPLACE FUNCTION nk_grid_for_geometry(_geo geometry) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$ BEGIN
  -- noinspection SqlResolve
  RETURN ST_GeoHash(ST_Centroid(_geo),7);
END $$;

CREATE OR REPLACE FUNCTION nk_partition_name_for_ts(_ts timestamptz) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$ BEGIN
  RETURN to_char(_ts, 'YYYY_MM_DD');
END $$;

CREATE OR REPLACE FUNCTION nk_partition_name_for_date(_year int, _month int, _day int) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$ BEGIN
  RETURN nk_partition_name_for_ts(make_timestamptz(_year, _month, _day, 0, 0, 0.0, 'UTC'));
END $$;

CREATE OR REPLACE FUNCTION jsonb_is(_value jsonb, _type text) RETURNS bool LANGUAGE 'plpgsql' IMMUTABLE AS $$
DECLARE
  type text;
BEGIN
  type = jsonb_typeof(_value);
  RETURN type IS NOT NULL AND type = _type;
END $$;

CREATE OR REPLACE FUNCTION nk_partition_name_for_txn(_txn int8) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE STRICT AS $$
DECLARE
  t nk_txn_struct;
BEGIN
  t = nk_txn_unpack(_txn);
  RETURN nk_partition_name_for_ts(t.ts);
END $$;

CREATE OR REPLACE FUNCTION nk_get_collection(_collection_id text) RETURNS jsonb LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
  feature jsonb;
BEGIN
  SELECT obj_description(oid)::jsonb FROM pg_class
  WHERE relnamespace = nk_const_schema_oid() AND relname::text = _collection_id INTO feature;
  IF naksha_feature_type(feature) = nk_const_collection_type() AND naksha_feature_id(feature) = _collection_id THEN
    -- TODO: We need to add the estimated feature count
    --       For partitioned tables, (relkind='p'), we need to get the count for all partitions
    --       Lets as well gather the amount of deleted features.
    RETURN feature;
  END IF;
  RETURN NULL;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END $$;

-- This is a function that caches the state for this session.
CREATE OR REPLACE FUNCTION nk_get_collection_disable_history(_collection_id text) RETURNS bool LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
  cache_key text = nk_key(_collection_id, 'dishst');
  cache_value text;
  disable_history bool = false;
  feature jsonb;
BEGIN
  cache_value = nk_get_config(cache_key);
  IF cache_value = 'true' THEN
    RETURN true;
  END IF;
  IF cache_value = 'false' THEN
    RETURN false;
  END IF;
  SELECT obj_description(oid)::jsonb FROM pg_class
  WHERE relnamespace = nk_const_schema_oid() AND relname::text = _collection_id INTO feature;
  IF jsonb_is(feature->'disableHistory', 'boolean') THEN
    disable_history = (feature->'disableHistory')::bool;
  END IF;
  PERFORM nk_set_config(cache_key, disable_history::text, true);
  RETURN disable_history;
EXCEPTION WHEN OTHERS THEN RETURN false;
END $$;

-- This is a function that caches the state for this session.
CREATE OR REPLACE FUNCTION nk_get_collection_points_only(_collection_id text) RETURNS bool LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
  cache_key text = nk_key(_collection_id, 'ptonly');
  cache_value text;
  points_only bool = false;
  feature jsonb;
BEGIN
  cache_value = nk_get_config(cache_key);
  IF cache_value = 'true' THEN
    RETURN true;
  END IF;
  IF cache_value = 'false' THEN
    RETURN false;
  END IF;
  SELECT obj_description(oid)::jsonb FROM pg_class
  WHERE relnamespace = nk_const_schema_oid() AND relname::text = _collection_id INTO feature;
  IF jsonb_is(feature->'pointsOnly', 'boolean') THEN
    points_only = (feature->'pointsOnly')::bool;
  END IF;
  PERFORM nk_set_config(cache_key, points_only::text, true);
  RETURN points_only;
EXCEPTION WHEN OTHERS THEN RETURN false;
END $$;

CREATE OR REPLACE FUNCTION nk_get_collection_id_from_table_name(_tg_table_name text) RETURNS text LANGUAGE 'plpgsql'IMMUTABLE AS $$ BEGIN
  IF _tg_table_name ~ '.*_p[0-9][0-9]$' THEN
    RETURN substr(_tg_table_name,1,length(_tg_table_name)-4);
  END IF;
  RETURN _tg_table_name;
END; $$;

-- Returns the updated new feature (txn_next will be 0).
CREATE OR REPLACE FUNCTION nk_set_xyz_namespace_insert(_collection_id text, _new_uid int8, _new_feature jsonb, _new_geo geometry)
  RETURNS jsonb
  LANGUAGE 'plpgsql' VOLATILE AS $$
DECLARE
  txn int8 = naksha_txn();
  txn_struct nk_txn_struct = nk_txn_unpack(txn);
  ts_millis int8 = naksha_current_millis(current_timestamp);
  rts_millis int8 = naksha_current_millis(clock_timestamp());
  new_xyz jsonb;
  new_tags jsonb;
  id text;
  grid text;
  crid jsonb;
  xyz jsonb;
BEGIN
  IF NOT jsonb_is(_new_feature, 'object') THEN
    _new_feature = jsonb_build_object();
  END IF;
  IF NOT jsonb_is(_new_feature->'type', 'string') THEN
    _new_feature = jsonb_set(_new_feature, array['type'], to_jsonb('Feature'::text), true);
  END IF;
  IF NOT jsonb_is(_new_feature->'properties', 'object') THEN
    _new_feature = jsonb_set(_new_feature, array['properties'], jsonb_build_object(), true);
  END IF;
  new_xyz = _new_feature->'properties'->'@ns:com:here:xyz';
  IF jsonb_is(new_xyz->'tags', 'array') THEN
    new_tags = new_xyz->'tags';
  ELSE
    new_tags = jsonb_build_array();
  END IF;
  IF NOT jsonb_is(_new_feature->'id', 'string') THEN
    id = nk_random_id();
    _new_feature = jsonb_set(_new_feature, array['id'], to_jsonb(id), true);
  ELSE
    id = _new_feature->>'id';
  END IF;
  IF _new_geo IS NULL THEN
    grid = nk_grid_for_feature_id(id);
  ELSE
    grid = nk_grid_for_geometry(_new_geo);
  END IF;
  IF jsonb_is(new_xyz->'crid', 'string') THEN
    crid = new_xyz->'crid';
  ELSE
    crid = null::jsonb;
  END IF;
  xyz = jsonb_build_object(
      'createdAt', ts_millis,
      'updatedAt', ts_millis,
      'rt_ts', rts_millis,
      'txn', txn,
      'txn_next', 0,
      'txn_uuid', nk_guid(_collection_id, current_timestamp, txn_struct.uid),
      'uuid', nk_guid(_collection_id, current_timestamp, _new_uid),
      'puuid', null::jsonb,
      -- muuid
      'action', 'CREATE',
      'version', 1,
      'author', naksha_author(naksha_app_id()),
      'author_ts', ts_millis,
      'app_id', naksha_app_id(),
      'tags', new_tags,
      'crid', crid,
      'grid', grid,
      'stream_id', naksha_stream_id()
  );
  _new_feature = jsonb_set(_new_feature, ARRAY['properties','@ns:com:here:xyz'], xyz, true);
  RETURN _new_feature;
END; $$;

-- Returns the updated new features (txn_next will be 0).
CREATE OR REPLACE FUNCTION nk_set_xyz_namespace_update(_collection_id text, _new_uid int8, _new_feature jsonb, _new_geo geometry, _old_feature jsonb)
  RETURNS jsonb
  LANGUAGE 'plpgsql' VOLATILE AS $$
DECLARE
  txn int8 = naksha_txn();
  txn_struct nk_txn_struct = nk_txn_unpack(txn);
  ts_millis int8 = naksha_current_millis(current_timestamp);
  rts_millis int8 = naksha_current_millis(clock_timestamp());
  created_at jsonb;
  new_author text;
  new_author_ts int8;
  old_author text;
  old_author_ts int8;
  old_uuid jsonb;
  version int8;
  new_xyz jsonb;
  tags jsonb;
  id text;
  grid text;
  crid jsonb;
  xyz jsonb;
BEGIN
  IF NOT jsonb_is(_old_feature, 'object') THEN
    RAISE WARNING 'The old feature is not valid feature';
    _old_feature = jsonb_build_object();
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'uuid', 'string') THEN
    old_uuid = _old_feature->'properties'->'@ns:com:here:xyz'->'uuid';
  ELSE
    RAISE WARNING 'Old feature in update process does not have a valid uuid, set puuid to null';
    old_uuid = null::jsonb;
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'author', 'string') THEN
    old_author = _old_feature->'properties'->'@ns:com:here:xyz'->>'author';
  ELSE
    old_author = 'anonymous';
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'author_ts', 'number') THEN
    old_author_ts = (_old_feature->'properties'->'@ns:com:here:xyz'->'author_ts')::int8;
  ELSE
    old_author_ts = ts_millis;
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'version', 'number') THEN
    version = (_old_feature->'properties'->'@ns:com:here:xyz'->'version')::int8 + 1::int8;
  ELSE
    RAISE WARNING 'Old feature in update process does not have a valid version, setting version to 0';
    version = 0::int8;
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'createdAt', 'number') THEN
    created_at = _old_feature->'properties'->'@ns:com:here:xyz'->'createdAt';
  ELSE
    RAISE WARNING 'Old feature in update process does not have a valid createdAt, setting to current time';
    created_at = to_jsonb(ts_millis);
  END IF;
  IF NOT jsonb_is(_new_feature, 'object') THEN
    _new_feature = jsonb_build_object();
  END IF;
  IF NOT jsonb_is(_new_feature->'type', 'string') THEN
    _new_feature = jsonb_set(_new_feature, array['type'], to_jsonb('Feature'::text), true);
  END IF;
  IF NOT jsonb_is(_new_feature->'properties', 'object') THEN
    _new_feature = jsonb_set(_new_feature, array['properties'], jsonb_build_object(), true);
  END IF;
  new_xyz = _new_feature->'properties'->'@ns:com:here:xyz';
  IF jsonb_is(new_xyz->'tags', 'array') THEN
    tags = new_xyz->'tags';
  ELSE
    tags = jsonb_build_array();
  END IF;
  IF NOT jsonb_is(_new_feature->'id', 'string') THEN
    IF jsonb_is(_old_feature->'id', 'string') THEN
      id = _old_feature->>'id';
      RAISE WARNING 'New feature does not have an id, copy the old one: %', id;
    ELSE
      id = nk_random_id();
      RAISE WARNING 'New feature AND old feature do not have an id, create a new random one: %', id;
    END IF;
    _new_feature = jsonb_set(_new_feature, array['id'], to_jsonb(id), true);
  ELSE
    id = _new_feature->>'id';
  END IF;
  IF _new_geo IS NULL THEN
    grid = nk_grid_for_feature_id(id);
  ELSE
    grid = nk_grid_for_geometry(_new_geo);
  END IF;
  IF jsonb_is(new_xyz->'crid', 'string') THEN
    crid = new_xyz->'crid';
  ELSE
    crid = null::jsonb;
  END IF;
  new_author = naksha_author(NULL::text);
  IF new_author IS NULL THEN
    new_author = old_author;
    new_author_ts = old_author_ts;
  ELSE
    new_author_ts = ts_millis;
  END IF;
  xyz = jsonb_build_object(
      'createdAt', created_at,
      'updatedAt', ts_millis,
      'rt_ts', rts_millis,
      'txn', txn,
      'txn_next', 0,
      'txn_uuid', nk_guid(_collection_id, current_timestamp, txn_struct.uid),
      'uuid', nk_guid(_collection_id, current_timestamp, _new_uid),
      'puuid', old_uuid,
      -- muuid
      'action', 'UPDATE',
      'version', version,
      'author', new_author,
      'author_ts', new_author_ts,
      'app_id', naksha_app_id(),
      'tags', tags,
      'crid', crid,
      'grid', grid,
      'stream_id', naksha_stream_id()
  );
  _new_feature = jsonb_set(_new_feature, ARRAY['properties','@ns:com:here:xyz'], xyz, true);
  RETURN _new_feature;
END; $$;

-- Returns the updated new features (txn_next will be 0).
CREATE OR REPLACE FUNCTION nk_set_xyz_namespace_delete(_collection_id text, _new_uid int8, _old_feature jsonb)
  RETURNS jsonb
  LANGUAGE 'plpgsql' VOLATILE AS $$
DECLARE
  txn int8 = naksha_txn();
  txn_struct nk_txn_struct = nk_txn_unpack(txn);
  ts_millis int8 = naksha_current_millis(current_timestamp);
  rts_millis int8 = naksha_current_millis(clock_timestamp());
  created_at jsonb;
  new_author text;
  new_author_ts int8;
  old_author text;
  old_author_ts int8;
  old_uuid jsonb;
  version int8;
  tags jsonb;
  id text;
  grid text;
  crid jsonb;
  xyz jsonb;
BEGIN
  IF NOT jsonb_is(_old_feature, 'object') THEN
    RAISE WARNING 'The old feature is not valid feature';
    _old_feature = jsonb_build_object();
  END IF;
  IF NOT jsonb_is(_old_feature->'properties', 'object') THEN
    RAISE WARNING 'The old feature does not have properties, creating empty';
    _old_feature = jsonb_set(_old_feature, array['properties'], jsonb_build_object(), true);
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'uuid', 'string') THEN
    old_uuid = _old_feature->'properties'->'@ns:com:here:xyz'->'uuid';
  ELSE
    RAISE WARNING 'Old feature in delete process does not have a valid uuid, set puuid to null';
    old_uuid = null::jsonb;
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'author', 'string') THEN
    old_author = _old_feature->'properties'->'@ns:com:here:xyz'->>'author';
  ELSE
    old_author = 'anonymous';
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'author_ts', 'number') THEN
    old_author_ts = (_old_feature->'properties'->'@ns:com:here:xyz'->'author_ts')::int8;
  ELSE
    old_author_ts = ts_millis;
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'version', 'number') THEN
    version = (_old_feature->'properties'->'@ns:com:here:xyz'->'version')::int8 + 1::int8;
  ELSE
    RAISE WARNING 'Old feature in delete process does not have a valid version, setting version to 0';
    version = 0::int8;
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'createdAt', 'number') THEN
    created_at = _old_feature->'properties'->'@ns:com:here:xyz'->'createdAt';
  ELSE
    RAISE WARNING 'Old feature in delete process does not have a valid createdAt, setting to current time';
    created_at = to_jsonb(ts_millis);
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'tags', 'array') THEN
    tags = _old_feature->'properties'->'@ns:com:here:xyz'->'tags';
  ELSE
    tags = jsonb_build_array();
  END IF;
  IF jsonb_is(_old_feature->'id', 'string') THEN
    id = _old_feature->>'id';
  ELSE
    id = nk_random_id();
    RAISE WARNING 'Old feature do not have an id, create a new random one: %', id;
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'grid', 'string') THEN
    grid = _old_feature->'properties'->'@ns:com:here:xyz'->>'grid';
  ELSE
    grid = nk_grid_for_feature_id(id);
    RAISE WARNING 'Old feature does not have a grid, creating one for id';
  END IF;
  IF jsonb_is(_old_feature->'properties'->'@ns:com:here:xyz'->'crid', 'string') THEN
    crid = _old_feature->'properties'->'@ns:com:here:xyz'->'crid';
  ELSE
    crid = null::jsonb;
  END IF;
  new_author = naksha_author(NULL::text);
  IF new_author IS NULL THEN
    new_author = old_author;
    new_author_ts = old_author_ts;
  ELSE
    new_author_ts = ts_millis;
  END IF;
  xyz = jsonb_build_object(
      'createdAt', created_at,
      'updatedAt', ts_millis,
      'rt_ts', rts_millis,
      'txn', txn,
      'txn_next', 0,
      'txn_uuid', nk_guid(_collection_id, current_timestamp, txn_struct.uid),
      'uuid', nk_guid(_collection_id, current_timestamp, _new_uid),
      'puuid', old_uuid,
      -- muuid
      'action', 'DELETE',
      'version', version,
      'author', new_author,
      'author_ts', new_author_ts,
      'app_id', naksha_app_id(),
      'tags', tags,
      'crid', crid,
      'grid', grid,
      'stream_id', naksha_stream_id()
  );
  _old_feature = jsonb_set(_old_feature, ARRAY['properties','@ns:com:here:xyz'], xyz, true);
  RETURN _old_feature;
END; $$;

CREATE OR REPLACE FUNCTION nk__________TRIGGER_________() RETURNS void LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN END $$;

-- The trigger added to the HEAD partitions or HEAD main table
-- Added before INSERT and UPDATE
CREATE OR REPLACE FUNCTION nk_trigger_before() RETURNS trigger LANGUAGE 'plpgsql' AS $$
DECLARE
  app_id text;
  collection_id text;
BEGIN
  app_id = current_setting('naksha.app_id');
  IF app_id IS NULL OR app_id = '' THEN
    RAISE EXCEPTION 'Session not initialized, please invoke naksha_start_session first and set application_name' USING ERRCODE='N0000';
  END IF;
  --collection_id = nk_get_collection_id_from_table_name(TG_TABLE_NAME);
  IF tg_table_name ~ '.*_p[0-9][0-9][0-9]$' THEN
    collection_id = substr(tg_table_name,1,length(tg_table_name)-5);
  ELSE
    collection_id = tg_table_name;
  END IF;
  IF TG_OP = 'INSERT' THEN
    NEW.i = nextval('"'||TG_TABLE_SCHEMA||'"."'||collection_id||'_i_seq"');
    NEW.jsondata = nk_set_xyz_namespace_insert(collection_id, NEW.i, NEW.jsondata, NEW.geo);
    RETURN NEW;
  END IF;
  IF TG_OP = 'UPDATE' THEN
    NEW.i = nextval('"'||TG_TABLE_SCHEMA||'"."'||collection_id||'_i_seq"');
    NEW.jsondata = nk_set_xyz_namespace_update(collection_id, NEW.i, NEW.jsondata, NEW.geo, OLD.jsondata);
    RETURN NEW;
  END IF;
  -- TG_OP = 'DELETE'
  RETURN OLD;
END $$;

-- The trigger added to the HEAD partitions or HEAD main table
-- Added after INSERT, UPDATE and DELETE
CREATE OR REPLACE FUNCTION nk_trigger_after() RETURNS trigger LANGUAGE 'plpgsql' VOLATILE AS $$
DECLARE
  txn int8;
  txn_struct nk_txn_struct;
  sql text;
  collection_id text;
  disable_history bool;
BEGIN
  --collection_id = nk_get_collection_id_from_table_name(TG_TABLE_NAME);
  IF tg_table_name ~ '.*_p[0-9][0-9][0-9]$' THEN
    collection_id = substr(tg_table_name,1,length(tg_table_name)-5);
  ELSE
    collection_id = tg_table_name;
  END IF;
  disable_history = nk_get_collection_disable_history(collection_id);
  txn = naksha_txn();
  -- TODO: Update the transaction-log
  -- We do not write INSERT into history (why should we duplicate the HEAD state?).
  IF NOT disable_history AND (TG_OP = 'UPDATE' OR TG_OP = 'DELETE') THEN
    -- Create a backup of the old state (should be CREATE or UPDATE), copy it into the history.
    OLD.jsondata = jsonb_set(OLD.jsondata, array['properties','@ns:com:here:xyz','txn_next'], to_jsonb(txn), true);
    sql = format('INSERT INTO %I (jsondata,geo,i) VALUES($1,$2,$3);', format('%s_hst', collection_id));
    --RAISE NOTICE '%', sql;
    BEGIN
      EXECUTE sql USING OLD.jsondata, OLD.geo, OLD.i;
    EXCEPTION WHEN check_violation THEN
      -- Ensure the history partition exists
      txn_struct = nk_txn_unpack(txn);
      PERFORM nk_create_hst_partition_by_ts(collection_id, txn_struct.ts);
      -- Try insert again
      EXECUTE sql USING OLD.jsondata, OLD.geo, OLD.i;
    END;
  END IF;
  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    -- Feature inserted or updated, ensure that there is no pending version in the deletion table.
    sql = format('DELETE FROM %I WHERE jsondata->>''id'' = $1;', format('%s_del', collection_id));
    --RAISE NOTICE '%', sql;
    EXECUTE sql USING NEW.jsondata->>'id';
    RETURN NEW;
  END IF;
  -- TG_OP = 'DELETE'
  -- We need anyway to create a copy for the deletion table (it will have txn_next=0).
  OLD.i = nextval('"'||TG_TABLE_SCHEMA||'"."'||collection_id||'_i_seq"');
  OLD.jsondata = nk_set_xyz_namespace_delete(collection_id, OLD.i, OLD.jsondata);
  sql = format('INSERT INTO %I (jsondata,geo,i) VALUES($1,$2,$3);', format('%s_del', collection_id));
  --RAISE NOTICE '%', sql;
  EXECUTE sql USING OLD.jsondata, OLD.geo, OLD.i;
  -- We keep the uid for the nk_write_features to fix the namespace.
  PERFORM nk_set_config(nk_const_del_uid(), OLD.i::text, true);

  -- If the history is enabled, we copy the deletion table entry as well into history, but with txn_next=txn.
  -- Notice: Therefore, we have two writes into history table for a DELETE, which compensates for the one we safe
  --         in the INSERT case. However, we assume we always have far less deleted than inserts, so this is a good
  --         deal.
  IF NOT disable_history THEN
    -- Create a backup of the DELETE state into history.
    OLD.jsondata = jsonb_set(OLD.jsondata, array['properties','@ns:com:here:xyz','txn_next'], to_jsonb(txn), true);
    sql = format('INSERT INTO %I (jsondata,geo,i) VALUES($1,$2,$3);', format('%s_hst', collection_id));
    --RAISE NOTICE '%', sql;
    BEGIN
      EXECUTE sql USING OLD.jsondata, OLD.geo, OLD.i;
    EXCEPTION WHEN check_violation THEN
      -- Ensure the history partition exists
      txn_struct = nk_txn_unpack(txn);
      PERFORM nk_create_hst_partition_by_ts(collection_id, txn_struct.ts);
      -- Try insert again
      EXECUTE sql USING OLD.jsondata, OLD.geo, OLD.i;
    END;
  END IF;
  RETURN OLD;
END
$$;

CREATE OR REPLACE FUNCTION nk__________IMPL_________() RETURNS void LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN END $$;

CREATE OR REPLACE FUNCTION nk_optimize_table(_table text, _history bool) RETURNS void LANGUAGE 'plpgsql' VOLATILE AS $BODY$
DECLARE
  sql text;
BEGIN
  sql := format('ALTER TABLE %I ALTER COLUMN "jsondata" SET STORAGE MAIN;'
             || 'ALTER TABLE %I ALTER COLUMN "jsondata" SET COMPRESSION lz4;'
             || 'ALTER TABLE %I ALTER COLUMN "geo" SET STORAGE MAIN;'
             || 'ALTER TABLE %I ALTER COLUMN "geo" SET COMPRESSION lz4;',
             _table, _table, _table, _table);
--RAISE NOTICE '%', sql;
  EXECUTE sql;

  IF _history THEN
    sql := format('ALTER TABLE %I SET ('
               || 'toast_tuple_target=8160'
               || ',fillfactor=100'
               || ',autovacuum_enabled=OFF, toast.autovacuum_enabled=OFF'
               || ');', _table);
  ELSE
    sql := format('ALTER TABLE %I SET ('
             || 'toast_tuple_target=8160'
             || ',fillfactor=50'
             -- Specifies the minimum number of updated or deleted tuples needed to trigger a VACUUM in any one table.
             || ',autovacuum_vacuum_threshold=10000, toast.autovacuum_vacuum_threshold=10000'
             -- Specifies the number of inserted tuples needed to trigger a VACUUM in any one table.
             || ',autovacuum_vacuum_insert_threshold=10000, toast.autovacuum_vacuum_insert_threshold=10000'
             -- Specifies a fraction of the table size to add to autovacuum_vacuum_threshold when deciding whether to trigger a VACUUM.
             || ',autovacuum_vacuum_scale_factor=0.1, toast.autovacuum_vacuum_scale_factor=0.1'
             -- Specifies a fraction of the table size to add to autovacuum_analyze_threshold when deciding whether to trigger an ANALYZE.
             || ',autovacuum_analyze_threshold=10000, autovacuum_analyze_scale_factor=0.1'
             || ');', _table);
  END IF;
--RAISE NOTICE '%', sql;
  EXECUTE sql;
END $BODY$;

CREATE OR REPLACE FUNCTION nk_create_indices(_collection text, _table text, _use_sp_gist bool, _is_head bool)
  RETURNS void
  LANGUAGE 'plpgsql' VOLATILE AS $BODY$
DECLARE
  fill_factor text;
  geo_index_type text;
  sql text;
BEGIN
  IF _is_head THEN
    fill_factor = '50';

    -- "id"
    sql = format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I USING btree ('
              || ' (jsondata->>''id'') COLLATE "C" text_pattern_ops DESC'
              || ') WITH (fillfactor=%s)',
                 format('%s_id_idx', _table), _table, fill_factor);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;
  ELSE
    fill_factor = '100';

    -- "id"
    sql = format('CREATE INDEX IF NOT EXISTS %I ON %I USING btree ('
              || ' (jsondata->>''id'') COLLATE "C" text_pattern_ops DESC'
              || ') WITH (fillfactor=%s)',
                 format('%s_id_idx', _table), _table, fill_factor);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;
  END IF;

  -- "i"
  sql = format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I USING btree (i) WITH (fillfactor=%s)',
               format('%s_i_idx', _table), _table, fill_factor);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  -- "txn"
  sql = format('CREATE INDEX IF NOT EXISTS %I ON %I USING btree ('
            || '((jsondata->''properties''->''@ns:com:here:xyz''->''txn'')::int8) DESC'
            || ') WITH (fillfactor=%s)',
               format('%s_txn_idx', _table), _table, fill_factor);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  -- "geo"
  IF _use_sp_gist THEN
    geo_index_type = 'sp-gist';
  ELSE
    geo_index_type = 'gist';
  END IF;
  sql = format('CREATE INDEX IF NOT EXISTS %I ON %I USING %s ('
            || ' geo '
            || ',((jsondata->''properties''->''@ns:com:here:xyz''->''txn'')::int8)'
            || ') WITH (buffering=ON,fillfactor=%s)',
               format('%s_geo_idx', _table), _table, geo_index_type, fill_factor);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  -- gin: https://www.postgresql.org/docs/current/gin-tips.html
  -- "tags"
  sql = format('CREATE INDEX IF NOT EXISTS %I ON %I USING gin ('
            || ' (jsondata->''properties''->''@ns:com:here:xyz''->''tags'')'
            || ',((jsondata->''properties''->''@ns:com:here:xyz''->''txn'')::int8)'
            || ') WITH (fastupdate=ON,gin_pending_list_limit=32768)', --KiB, default is 4096
               format('%s_tags_idx', _table), _table);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  -- "grid"
  sql = format('CREATE INDEX IF NOT EXISTS %I ON %I USING btree ('
            || ' (jsondata->''properties''->''@ns:com:here:xyz''->>''grid'') COLLATE "C" DESC'
            || ',((jsondata->''properties''->''@ns:com:here:xyz''->''txn'')::int8) DESC'
            || ') WITH (fillfactor=%s)',
               format('%s_grid_idx', _table), _table, fill_factor);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  -- "mrid"
  sql = format('CREATE INDEX IF NOT EXISTS %I ON %I USING btree ('
            || ' (naksha_feature_mrid(jsondata)) COLLATE "C" DESC '
            || ',((jsondata->''properties''->''@ns:com:here:xyz''->''txn'')::int8) DESC'
            || ') WITH (fillfactor=%s)',
               format('%s_mrid_idx', _table), _table, fill_factor);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  -- "app_id"
  sql = format('CREATE INDEX IF NOT EXISTS %I ON %I USING btree ('
            || ' (jsondata->''properties''->''@ns:com:here:xyz''->>''app_id'') COLLATE "C" DESC'
            || ',((jsondata->''properties''->''@ns:com:here:xyz''->''updatedAt'')::int8) DESC'
            || ',((jsondata->''properties''->''@ns:com:here:xyz''->''txn'')::int8) DESC'
            || ') WITH (fillfactor=%s)',
               format('%s_appid_idx', _table), _table, fill_factor);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  -- "author" and "author_ts"
  sql = format('CREATE INDEX IF NOT EXISTS %I ON %I USING btree ('
            || ' (jsondata->''properties''->''@ns:com:here:xyz''->>''author'') COLLATE "C" DESC'
            || ',((jsondata->''properties''->''@ns:com:here:xyz''->''author_ts'')::int8) DESC'
            || ',((jsondata->''properties''->''@ns:com:here:xyz''->''txn'')::int8) DESC'
            || ') WITH (fillfactor=%s)',
               format('%s_author_idx', _table), _table, fill_factor);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;
END; $BODY$;

CREATE OR REPLACE FUNCTION nk_create_hst_partition_by_ts(_collection text, _from_ts timestamptz)
  RETURNS void
  LANGUAGE 'plpgsql' STRICT VOLATILE AS $$
DECLARE
  sql text;
  from_day text;
  from_part_id int8;
  to_day text;
  to_ts timestamptz;
  to_part_id int8;
  hst_table_name text;
  hst_partition_table_name text;
BEGIN
--RAISE NOTICE 'Prepare partition creation';
  from_day := nk_partition_name_for_ts(_from_ts);
  from_part_id := nk_txn(_from_ts, 0::int8);
  to_ts := _from_ts + '1 day'::interval;
  to_day := nk_partition_name_for_ts(to_ts);
  to_part_id := nk_txn(to_ts, 0::int8);
  hst_table_name := format('%s_hst', _collection);
  hst_partition_table_name := format('%s_hst_%s', _collection, from_day); -- example: foo_hst_2023_03_01

  sql := format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s);',
                hst_partition_table_name, hst_table_name, from_part_id, to_part_id);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;
  PERFORM nk_optimize_table(hst_partition_table_name, true);
  PERFORM nk_create_indices(_collection, hst_partition_table_name, nk_get_collection_points_only(_collection), false);
END $$;

-- Create all tables and indices, but not triggers.
-- CREATE UNLOGGED ... TABLE
-- TODO: SET { LOGGED | UNLOGGED }
CREATE OR REPLACE FUNCTION nk_upsert_collection( _collection_id text, _partition bool, _sp_gist bool, unlogged bool )
  RETURNS void
  LANGUAGE 'plpgsql' VOLATILE AS $BODY$
DECLARE
  CREATE_TABLE text;
  part_id text;
  part_name text;
  del_name text;
  meta_name text;
  hst_name text;
  sql text;
  trigger_name text;
  full_name text;
BEGIN
  IF unlogged THEN
    CREATE_TABLE = 'CREATE TABLE';
  ELSE
    CREATE_TABLE = 'CREATE UNLOGGED TABLE';
  END IF;
  IF _partition THEN
    sql = format('%s IF NOT EXISTS %I ('
              || ' jsondata jsonb CHECK (((jsondata->''properties''->''@ns:com:here:xyz''->''txn_next'')::int8) = 0)'
              || ',geo geometry(GeometryZ, 4326)'
              || ',i int8'
              || ') PARTITION BY LIST (nk_head_partition_id(jsondata->>''id''))',
              CREATE_TABLE, _collection_id);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    FOR i IN 0..255 LOOP
      part_id = lpad(i::text,3,'0');
      part_name = format('%s_p%s', _collection_id, part_id);
      sql = format('%s IF NOT EXISTS %I PARTITION OF %I FOR VALUES IN (%L);',
                    CREATE_TABLE, part_name, _collection_id, part_id);
      --RAISE NOTICE '  %', sql;
      EXECUTE sql;
      PERFORM nk_optimize_table(part_name, false);
      PERFORM nk_create_indices(_collection_id, part_name, _sp_gist, true);
    END LOOP;
  ELSE
    sql = format('%s IF NOT EXISTS %I ('
              || ' jsondata jsonb CHECK (((jsondata->''properties''->''@ns:com:here:xyz''->''txn_next'')::int8) = 0)'
              || ',geo geometry(GeometryZ, 4326)'
              || ',i int8'
              || ')', CREATE_TABLE, _collection_id);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;
    PERFORM nk_optimize_table(_collection_id, false);
    PERFORM nk_create_indices(_collection_id, _collection_id, _sp_gist, true);
  END IF;

  -- Create the sequence for uid (aka "i").
  sql := format('CREATE SEQUENCE IF NOT EXISTS %I AS int8 CACHE 1000 OWNED BY %I.i', format('%s_i_seq', _collection_id), _collection_id);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  del_name = format('%s_del', _collection_id);
  sql = format('%s IF NOT EXISTS %I ('
            || ' jsondata jsonb CHECK (((jsondata->''properties''->''@ns:com:here:xyz''->''txn_next'')::int8) = 0)'
            || ',geo geometry(GeometryZ, 4326)'
            || ',i int8'
            || ')', CREATE_TABLE, del_name);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;
  PERFORM nk_optimize_table(del_name, false);
  PERFORM nk_create_indices(_collection_id, del_name, _sp_gist, true);

  meta_name = format('%s_meta', _collection_id);
  sql = format('%s IF NOT EXISTS %I ('
            || ' jsondata jsonb'
            || ',geo geometry(GeometryZ, 4326)'
            || ',i int8'
            || ')', CREATE_TABLE, meta_name);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;
  PERFORM nk_optimize_table(meta_name, false);
  PERFORM nk_create_indices(_collection_id, meta_name, _sp_gist, true);

  hst_name = format('%s_hst', _collection_id);
  sql = format('%s IF NOT EXISTS %I ('
            || ' jsondata jsonb CHECK (((jsondata->''properties''->''@ns:com:here:xyz''->''txn_next'')::int8) > 0)'
            || ',geo geometry(GeometryZ, 4326)'
            || ',i int8'
            || ') PARTITION BY RANGE (((jsondata->''properties''->''@ns:com:here:xyz''->''txn_next'')::int8))',
            CREATE_TABLE, hst_name);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;
  PERFORM nk_optimize_table(meta_name, true);

  trigger_name := format('%s_before', _collection_id);
  full_name := format('%I', _collection_id);
  IF NOT EXISTS(SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = full_name::regclass and tgname = trigger_name) THEN
      sql := format('CREATE TRIGGER %I '
                 || 'BEFORE INSERT OR UPDATE ON %I '
                 || 'FOR EACH ROW EXECUTE FUNCTION nk_trigger_before();',
                    trigger_name, _collection_id);
      --RAISE NOTICE '%', sql;
      EXECUTE sql;
  END IF;

  trigger_name := format('%s_after', _collection_id);
  full_name := format('%I', _collection_id);
  IF NOT EXISTS(SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = full_name::regclass and tgname = trigger_name) THEN
      sql := format('CREATE TRIGGER %I '
                 || 'BEFORE INSERT OR UPDATE OR DELETE ON %I '
                 || 'FOR EACH ROW EXECUTE FUNCTION nk_trigger_after();',
                    trigger_name, _collection_id);
      --RAISE NOTICE '%', sql;
      EXECUTE sql;
  END IF;
END
$BODY$;

-- Create all tables and indices, but not triggers.
CREATE OR REPLACE FUNCTION nk_drop_collection(_collection_id text) RETURNS void LANGUAGE 'plpgsql' VOLATILE AS $BODY$
DECLARE
  sql text;
BEGIN
  sql = format('DROP TABLE IF EXISTS %I CASCADE', _collection_id);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  sql = format('DROP TABLE IF EXISTS %I CASCADE', format('%s_del', _collection_id));
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  sql = format('DROP TABLE IF EXISTS %I CASCADE', format('%s_del', _collection_id));
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  sql = format('DROP TABLE IF EXISTS %I', format('%s_meta', _collection_id));
  --RAISE NOTICE '%', sql;
  EXECUTE sql;

  sql = format('DROP TABLE IF EXISTS %I', format('%s_hst', _collection_id));
  --RAISE NOTICE '%', sql;
  EXECUTE sql;
END
$BODY$;

-- min_result will set feature and geometry to null.
CREATE OR REPLACE FUNCTION nk_write_features(
  collection_id text,
  partition_id int,
  ops text array,
  ids text array,
  uuids text array,
  features jsonb array,
  geometries geometry array,
  min_result bool,
  errors_only bool
)
  RETURNS TABLE (r_op text, r_id text, r_uuid text, r_type text, r_ptype text, r_feature jsonb, r_geometry geometry, r_err jsonb)
  LANGUAGE 'plpgsql' VOLATILE STRICT AS $$
DECLARE
  collection          jsonb;
  table_name          text;
  arr_size            int;
  txn                 int8;
  uid                 int8;
  rows_affected       int;
  i                   int;
  id                  text;
  uuid                text;
  e_uuid              text;
  feature             jsonb;
  geo                 geometry;
  op                  text;
  select_head_stmt    text;
  select_del_stmt     text;
  insert_stmt         text;
  update_stmt         text;
  update_atomic_stmt  text;
  delete_stmt         text;
  delete_atomic_stmt  text;
  purge_stmt          text;
  purge_atomic_stmt   text;
BEGIN
  PERFORM nk_set_error(null, null);
  IF collection_id IS NULL THEN
    CALL nk_raise_invalid_parameter_value('Collection parameter must not be null');
  END IF;
  collection = naksha_collection_get(collection_id);
  IF collection IS NULL THEN
    CALL nk_raise_collection_not_exists(collection_id);
  END IF;
  table_name = nk_head_partition_name(collection_id, partition_id);
  IF ops IS NULL THEN
    CALL nk_raise_invalid_parameter_value('"ops" must not be null');
  END IF;
  arr_size = array_length(ops, 1);
  IF ids IS NULL OR array_length(ids, 1) != arr_size THEN
    CALL nk_raise_invalid_parameter_value('"ids" must not be null and have same size as the ops array');
  END IF;
  IF features IS NULL OR array_length(features, 1) != arr_size THEN
    CALL nk_raise_invalid_parameter_value('"features" must not be null and have same size as the ops array');
  END IF;
  IF uuids IS NULL OR array_length(uuids, 1) != arr_size THEN
    CALL nk_raise_invalid_parameter_value('"uuids" must not be null and have same size as the ops array');
  END IF;
  IF geometries IS NULL OR array_length(geometries, 1) != arr_size THEN
    CALL nk_raise_invalid_parameter_value('"geometries" must not be null and have same size as the ops array');
  END IF;

  --RAISE NOTICE 'Start write_features';
  txn = naksha_txn();
  -- id
  select_head_stmt = format('SELECT jsondata, geo FROM %I WHERE jsondata->>''id''=$1;', table_name);
  -- feature, geo
  insert_stmt = format('INSERT INTO %I (jsondata, geo) VALUES ($1, ST_Force3D($2)) RETURNING jsondata;', table_name);
  -- feature, geo, id
  update_stmt = format('UPDATE %I SET jsondata=$1, geo=ST_Force3D($2) WHERE jsondata->>''id''=$3 RETURNING jsondata;', table_name);
  -- feature, geo, id, uuid
  update_atomic_stmt = format('UPDATE %I SET jsondata=$1, geo=ST_Force3D($2) WHERE jsondata->>''id''=$3 AND jsondata->''properties''->''@ns:com:here:xyz''->>''uuid''=$4 RETURNING jsondata;', table_name);
  -- id
  delete_stmt = format('DELETE FROM %I WHERE jsondata->>''id''=$1 RETURNING jsondata, geo;', table_name);
  -- id, uuid
  delete_atomic_stmt = format('DELETE FROM %I WHERE jsondata->>''id''=$1 AND jsondata->''properties''->''@ns:com:here:xyz''->>''uuid''=$2 RETURNING jsondata, geo;', table_name);
  -- id
  purge_stmt = format('DELETE FROM %I WHERE jsondata->>''id''=$1 RETURNING jsondata, geo;', collection_id||'_del');
  -- id, uuid
  purge_atomic_stmt = format('DELETE FROM %I WHERE jsondata->>''id''=$1 AND jsondata->''properties''->''@ns:com:here:xyz''->>''uuid''=$2 RETURNING jsondata, geo;', collection_id||'_del');
  -- id
  select_del_stmt = format('SELECT jsondata, geo FROM %I WHERE jsondata->>''id''=$1;', collection_id||'_del');
  i = 1;
  WHILE i <= arr_size
  LOOP
    --RAISE NOTICE 'Loop %', i;
    r_op = NULL;
    r_id = NULL;
    r_uuid = NULL;
    r_type = NULL;
    r_ptype = NULL;
    r_feature = NULL;
    r_geometry = NULL;
    r_err = NULL;

    feature = features[i];
    id = naksha_feature_id(feature);
    IF id IS NULL THEN
      id = ids[i];
      IF id IS NULL THEN
        id = md5(random()::text);
        --RAISE NOTICE 'Generate id for write_ops[%]: %', i, id;
        IF NOT jsonb_is(feature, 'object') THEN
          r_op = 'ERROR';
          r_err = nk_err_invalid_parameter_value(format('[%s] No feature and no id given for operation %L', i, op));
          RETURN NEXT;
          i = i +1;
          CONTINUE;
        END IF;
        feature = jsonb_set_lax(feature, array['id'], to_jsonb(id), true, 'use_json_null');
      END IF;
    END IF;
    uuid = naksha_feature_uuid(feature, uuids[i]);
    geo = geometries[i];

    -- Prefill result values, but they should be updated ones we have real values.
    r_id = id;
    r_uuid = uuid;
    r_type = naksha_feature_type(feature);
    r_ptype = naksha_feature_ptype(feature);
    IF NOT min_result THEN
      r_feature = feature;
      r_geometry = geo;
    END IF;

    op = ops[i];
    IF op = 'INSERT' THEN
      op = 'CREATE';
    ELSIF op = 'UPSERT' THEN
      op = 'PUT';
    END IF;
    IF op IS NULL OR (op <> 'CREATE' AND op <> 'UPDATE' AND op <> 'PUT' AND op <> 'DELETE' AND op <> 'PURGE') THEN
      r_op = 'ERROR';
      r_err = nk_err_invalid_parameter_value(format('[%s] Invalid or missing operation: %L', i, op));
      RETURN NEXT;
      i = i + 1;
      CONTINUE;
    END IF;

    BEGIN
      --RAISE NOTICE 'op=''%'', id=''%'', feature=''%'', uuid=''%''', op, id, feature, uuid;
      IF op = 'CREATE' OR op = 'PUT' THEN
        BEGIN
          EXECUTE insert_stmt USING feature, geo INTO r_feature;
          GET DIAGNOSTICS rows_affected = ROW_COUNT;
          r_op = 'CREATED';
        EXCEPTION WHEN unique_violation THEN
          op = 'UPDATE';
        END;
      END IF;
      IF op = 'UPDATE' THEN
        IF uuid IS NOT NULL THEN
          EXECUTE update_atomic_stmt USING feature, geo, id, uuid INTO r_feature;
          GET DIAGNOSTICS rows_affected = ROW_COUNT;
        ELSE
          EXECUTE update_stmt USING feature, geo, id INTO r_feature;
          GET DIAGNOSTICS rows_affected = ROW_COUNT;
        END IF;
        r_op = 'UPDATED';
      ELSEIF op = 'DELETE' OR op = 'PURGE' THEN
        IF uuid IS NOT NULL THEN
          EXECUTE delete_atomic_stmt USING id, uuid INTO r_feature, r_geometry;
          GET DIAGNOSTICS rows_affected = ROW_COUNT;
        ELSE
          EXECUTE delete_stmt USING id INTO r_feature, r_geometry;
          GET DIAGNOSTICS rows_affected = ROW_COUNT;
        END IF;
        e_uuid = NULL;
        IF rows_affected = 1 THEN
          uid = nk_get_config(nk_const_del_uid())::int8;
          e_uuid = nk_guid(collection_id, current_timestamp, uid);
          IF NOT errors_only THEN
            r_op = 'DELETED';
            r_feature = nk_set_xyz_namespace_delete(collection_id, uid, r_feature);
            r_uuid = naksha_feature_uuid(r_feature);
            r_type = naksha_feature_type(r_feature);
            r_ptype = naksha_feature_ptype(r_feature);
            IF min_result THEN
              r_feature = NULL;
              r_geometry = NULL;
            END IF;
            RETURN NEXT;
          END IF;
          IF op = 'DELETE' THEN
            -- When this was a DELETE operation, we are done successfully.
            i = i + 1;
            CONTINUE;
          END IF;
        END IF;
        IF op = 'PURGE' THEN
          IF uuid IS NOT NULL AND e_uuid IS NOT NULL THEN
            --RAISE NOTICE 'Adjust uuid from % to % for following PURGE', uuid, r_uuid;
            uuid = e_uuid;
            r_uuid = e_uuid;
          END IF;
          IF uuid IS NOT NULL THEN
            EXECUTE purge_atomic_stmt USING id, uuid INTO r_feature, r_geometry;
            GET DIAGNOSTICS rows_affected = ROW_COUNT;
          ELSE
            EXECUTE purge_stmt USING id INTO r_feature, r_geometry;
            GET DIAGNOSTICS rows_affected = ROW_COUNT;
          END IF;
          r_op = 'PURGED';
        END IF;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      -- Note: For errors we ignore min_results.
      r_op = 'ERROR';
      EXECUTE select_head_stmt USING id INTO r_feature, r_geometry;
      r_uuid = naksha_feature_uuid(r_feature, uuid);
      r_type = naksha_feature_type(r_feature);
      r_ptype = naksha_feature_ptype(r_feature);
      IF op = 'CREATE' AND SQLSTATE = '23505' THEN
        r_err = nk_err_unique_violation(format('The feature with the id %L does exist already', id));
      ELSE
        r_err = nk_set_error(SQLSTATE, SQLERRM);
      END IF;
      RETURN NEXT;
      i = i + 1;
      CONTINUE;
    END;

    -- We won't reach this point for CREATE!
    IF rows_affected = 0 THEN
      IF uuid IS NULL AND op = 'DELETE' OR op = 'PURGE' THEN
        -- We ignore when a DELETE or PURGE, that is not atomic, does not work.
        i = i + 1;
        CONTINUE;
      END IF;
      r_op = 'ERROR';
      IF uuid IS NULL THEN
        r_err = nk_err_no_data(format('Operation failed, no feature with the id %L exists', id));
      ELSE
        -- Note: For errors we ignore min_results.
        EXECUTE select_head_stmt USING id INTO r_feature, r_geometry;
        e_uuid = naksha_feature_uuid(r_feature);
        IF e_uuid IS NULL THEN
          IF op = 'PURGE' THEN
            EXECUTE select_del_stmt USING id INTO r_feature, r_geometry;
            e_uuid = naksha_feature_uuid(r_feature);
          END IF;
        END IF;
        IF e_uuid IS NULL THEN
          r_err = nk_err_no_data(format('Operation failed, no feature with the id %L exists', id));
        ELSE
          r_err = nk_err_check_violation(format('Operation failed, feature %L in invalid state, expected %L, but found %L', id, uuid, e_uuid));
        END IF;
      END IF;
    END IF;
    IF r_err IS NOT NULL OR NOT errors_only THEN
      r_uuid = naksha_feature_uuid(r_feature);
      r_type = naksha_feature_type(r_feature);
      r_ptype = naksha_feature_ptype(r_feature);
      IF r_err IS NULL AND min_result THEN
        r_feature = NULL;
        r_geometry = NULL;
      END IF;
      RETURN next;
    END IF;
    i = i + 1;
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION nk__________PUBLIC_________() RETURNS void LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN END $$;

-- Returns current transaction number, i.e. 2023100600000000010, which is build as yyyyMMddXXXXXXXXXXX.
CREATE OR REPLACE FUNCTION naksha_txn() RETURNS int8 LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
  LOCK_ID constant int8 := nk_lock_id('naksha_tx_object_id_seq');
  SEQ_DIVIDER constant int8 := 100000000000;
  value    text;
  txi      int8;
  txn      int8;
  tx_date  int4;
  seq_date int4;
BEGIN
  value := current_setting('naksha.txn', true);
  IF coalesce(value, '') <> '' THEN
--RAISE NOTICE 'found value = %', value;
    return value::int8;
  END IF;

  -- prepare current yyyyMMdd as number i.e. 20231006
  tx_date := extract('year' from current_timestamp) * 10000 + extract('month' from current_timestamp) * 100 + extract('day' from current_timestamp);

  txi := nextval('naksha_tx_id_seq');
  -- txi should start with current date  20231006 with seq number "at the end"
  -- example: 2023100600000000007
  seq_date := txi / SEQ_DIVIDER; -- returns as number seq prefix which is yyyyMMdd  i.e. 20231006

  -- verification, if current day is not same as day in txi we have to reset sequence to new date and counting from start.
  IF seq_date <> tx_date then
    -- not sure if this lock it's enough, 'cause this is session lock that might be acquired multiple times within same session
    -- it has to be discussed if there is a chance of calling this function multiple times in parallel in same session.
    PERFORM pg_advisory_lock(LOCK_ID);
    BEGIN
      txi := nextval('naksha_tx_id_seq');
      seq_date := txi / SEQ_DIVIDER ;

      IF seq_date <> tx_date then
          txn := tx_date * SEQ_DIVIDER;
          -- is_called set to true guarantee that next val will be +1
          PERFORM setval('naksha_tx_id_seq', txn, true);
      ELSE
          txn := txi;
      END IF;
      PERFORM pg_advisory_unlock(LOCK_ID);
    EXCEPTION WHEN OTHERS THEN
      PERFORM pg_advisory_unlock(LOCK_ID);
      RAISE;
    END;
  ELSE
    txn := txi;
  END IF;
  PERFORM SET_CONFIG('naksha.txn', txn::text, true);
  RETURN txn::int8;
END $$;

-- Create a transaction number from the given timestamp and id.
-- Example: SELECT naksha_txn(current_timestamp, 98765432101);
CREATE OR REPLACE FUNCTION naksha_txn(ts timestamptz, id int8) RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $$
BEGIN
  RETURN (extract('year' FROM ts) * 10000 + extract('month' FROM ts) * 100 + extract('day' FROM ts)) * 100000000000 + id;
END $$;

-- Extracts the different values from a transaction number, so year, month, day and id.
-- Example: SELECT * FROM naksha_txn(2023123198765432101);
CREATE OR REPLACE FUNCTION naksha_txn(txn int8) RETURNS TABLE (year int, month int, day int, id int8) LANGUAGE 'plpgsql' IMMUTABLE AS $$
DECLARE
  SEQ_DIVIDER constant int8 := 100000000000;
  v int8;
BEGIN
  id = txn % SEQ_DIVIDER;
  v = txn / SEQ_DIVIDER;
  day = v % 100;
  v = v / 100;
  month = v % 100;
  year = v / 100;
  RETURN NEXT;
END $$;

-- SET SESSION application_name TO

-- Start the session, automatically called by lib-psql whenever a new session is started.
CREATE OR REPLACE FUNCTION naksha_start_session(app_name text, app_id text, author text, stream_id text) RETURNS void LANGUAGE 'plpgsql' VOLATILE AS $$
DECLARE
  sql text;
BEGIN
  IF app_id IS NULL OR app_id = '' THEN
    CALL nk_raise_invalid_parameter_value('Missing app_id, app_id is required');
  END IF;
  IF app_name IS NULL OR app_name = '' THEN
    CALL nk_raise_invalid_parameter_value('Missing app_name, app_name is required');
  END IF;
  IF current_schema() <> naksha_schema() THEN
    CALL nk_raise_check_violation(format('The current schema is %L, but expected to be %L', current_schema(), naksha_schema()));
  END IF;
  -- See: https://www.postgresql.org/docs/current/runtime-config-query.html
  sql = format('SET SESSION application_name TO %L;'
  || 'SELECT'
  || ' SET_CONFIG(''plan_cache_mode'', ''force_generic_plan'', false)'
  || ',SET_CONFIG(''cursor_tuple_fraction'', ''1.0'', false)'
  || ',SET_CONFIG(''geqo'', ''false'', false)'
  || ',SET_CONFIG(''work_mem'', ''1024 MB'', false)'
  || ',SET_CONFIG(''maintenance_work_mem'', ''1024 MB'', false)'
  || ',SET_CONFIG(''constraint_exclusion'', ''partition'', false)' -- default partition
  || ',SET_CONFIG(''enable_seqscan'', ''off'', false)'
  || ',SET_CONFIG(''enable_partitionwise_join'', ''on'', false)'
  || ',SET_CONFIG(''enable_partitionwise_aggregate'', ''on'', false)'
  || ',SET_CONFIG(''random_page_cost'', ''1.0'', false)' -- default: 4.0
  || ',SET_CONFIG(''seq_page_cost'', ''1.0'', false)'
  || ',SET_CONFIG(''max_parallel_workers_per_gather'', ''64'', false)'
  || ',SET_CONFIG(''parallel_setup_cost'', ''10.0'', false)' -- default: 1000
  || ',SET_CONFIG(''parallel_tuple_cost'', ''0.01'', false)' -- default: 0.1
  -- When debugging is enabled
--VERBOSE || ',SET_CONFIG(''pg_hint_plan.enable_hint'', ''on'', false)'
--VERBOSE || ',SET_CONFIG(''pg_hint_plan.enable_hint_table'', ''on'', false)'
--VERBOSE || ',SET_CONFIG(''pg_hint_plan.parse_messages'', ''debug'', false)'
--VERBOSE || ',SET_CONFIG(''pg_hint_plan.debug_print'', ''verbose'', false)'
--VERBOSE || ',SET_CONFIG(''pg_hint_plan.message_level'', ''notice'', false)'
-- || ',SET_CONFIG(''enable_presorted_aggregate'', ''on'', false)' -- only for Postgres 16
  || ',SET_CONFIG(''jit'', ''OFF'', false)'
  || ',SET_CONFIG(''naksha.app_id'', %L::text, false)'
  || ',SET_CONFIG(''naksha.author'', %L::text, false)'
  || ',SET_CONFIG(''naksha.stream_id'', %L::text, false)'
  || ',SET_CONFIG(''naksha.err_no'', '''', true)'
  || ',SET_CONFIG(''naksha.err_msg'', '''', true)'
    , app_name, app_id, author, stream_id);
  --RAISE NOTICE '%', sql;
  EXECUTE sql;
END $$;

CREATE OR REPLACE FUNCTION naksha_end_session() RETURNS void LANGUAGE 'plpgsql' VOLATILE AS $$ BEGIN
  PERFORM SET_CONFIG('naksha.app_id', NULL::text, false);
END; $$;

CREATE OR REPLACE FUNCTION naksha_err_no() RETURNS text LANGUAGE 'plpgsql' VOLATILE AS $$ BEGIN
  RETURN nk_get_config(nk_const_err_no());
END $$;

CREATE OR REPLACE FUNCTION naksha_err_msg() RETURNS text LANGUAGE 'plpgsql' VOLATILE AS $$ BEGIN
  RETURN nk_get_config(nk_const_err_msg());
END $$;

CREATE OR REPLACE FUNCTION naksha_application_name() RETURNS text LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
  value text;
BEGIN
  value := current_setting('application_name', true);
  IF value = '' THEN
      RETURN NULL;
  END IF;
  RETURN value;
END $$;

CREATE OR REPLACE FUNCTION naksha_app_id() RETURNS text LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
  value text;
BEGIN
  value := current_setting('naksha.app_id', true);
  IF value = '' THEN
      RETURN NULL;
  END IF;
  RETURN value;
END $$;

CREATE OR REPLACE FUNCTION naksha_author(_alternative text) RETURNS text LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
  value text;
BEGIN
  value := current_setting('naksha.author', true);
  IF value IS NULL OR value = '' THEN
      RETURN _alternative;
  END IF;
  RETURN value;
END $$;

CREATE OR REPLACE FUNCTION naksha_stream_id() RETURNS text LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
  value text;
BEGIN
  value := current_setting('naksha.stream_id', true);
  IF value = '' THEN
      RETURN NULL;
  END IF;
  RETURN value;
END $$;

CREATE OR REPLACE FUNCTION naksha_read_collections( ids text[], read_deleted bool )
  RETURNS TABLE (r_op text, r_id text, r_uuid text, r_type text, r_ptype text, r_feature jsonb, r_geometry geometry)
  LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
  actions text[];
BEGIN
  -- TODO: We need to add the estimated feature count
  --       For partitioned tables, (relkind='p'), we need to get the count for all partitions
  --       Lets as well gather the amount of deleted features.
  --       We should utilize the nk_get_collection() methods instead of making this too complicated.
  IF read_deleted THEN
    actions = array['CREATE','UPDATE','DELETE'];
  ELSE
    actions = array['CREATE','UPDATE'];
  END IF;
  IF ids IS NOT NULL THEN
    RETURN QUERY
    WITH table_info AS (
      SELECT relname::text AS "id",
             reltuples::int8 AS "feature_count",
             oid,
             obj_description(oid) AS "jsontext"
      FROM pg_class WHERE (relkind='r' OR relkind='p') AND relnamespace = nk_const_schema_oid() AND relname::text = ANY(ids)
    ), features AS (
      SELECT id, feature_count, obj_description(oid)::jsonb AS "feature"
      FROM table_info WHERE jsonb_is(obj_description(oid)::jsonb, 'object')
    )
    SELECT 'READ' as "r_op",
           naksha_feature_id(feature) as "r_id",
           naksha_feature_uuid(feature) as "r_uuid",
           naksha_feature_type(feature) as "r_type",
           naksha_feature_ptype(feature) as "r_ptype",
           jsonb_set(feature, '{"estimatedFeatureCount"}', to_jsonb(feature_count), true) AS "r_feature",
           null::geometry as "r_geometry"
    FROM features
    WHERE naksha_feature_type(feature) = nk_const_collection_type() AND naksha_feature_id(feature) = id AND naksha_feature_action(feature) = ANY(actions);
  ELSE
    RETURN QUERY
    WITH table_info AS (
      SELECT relname::text AS "id",
             reltuples::int8 AS "feature_count",
             oid,
             obj_description(oid) AS "jsontext"
      FROM pg_class WHERE (relkind='r' OR relkind='p') AND relnamespace = nk_const_schema_oid()
    ), features AS (
      SELECT id, feature_count, obj_description(oid)::jsonb AS "feature"
      FROM table_info WHERE jsonb_is(obj_description(oid)::jsonb, 'object')
    )
    SELECT 'READ' as "r_op",
           naksha_feature_id(feature) as "r_id",
           naksha_feature_uuid(feature) as "r_uuid",
           naksha_feature_type(feature) as "r_type",
           naksha_feature_ptype(feature) as "r_ptype",
           jsonb_set(feature, '{"estimatedFeatureCount"}', to_jsonb(feature_count), true) AS "r_feature",
           null::geometry as "r_geometry"
    FROM features
    WHERE naksha_feature_type(feature) = nk_const_collection_type() AND naksha_feature_id(feature) = id AND naksha_feature_action(feature) = ANY(actions);
  END IF;
END
$$;

-- naksha_write_collections(array['{"op":"CREATE","feature":{"id":"test"}}'::jsonb], false)
-- write_ops: {op,feature,id,uuid}
-- feature: {id,partition,pointsOnly}
CREATE OR REPLACE FUNCTION naksha_write_collections( write_ops jsonb[] )
  RETURNS TABLE (r_op text, r_id text, r_uuid text, r_type text, r_ptype text, r_feature jsonb, r_geometry geometry, r_err jsonb)
  LANGUAGE 'plpgsql' VOLATILE AS $$
DECLARE
  write_op jsonb;
  w_op text;
  w_id text;
  w_uuid text;
  w_feature jsonb;
  w_partition bool;
  w_unlogged bool;
  w_points_only bool;
  sql text;
  lock_id int8;
  e_feature jsonb;
  e_estimated_feature_count int8;
  e_partition bool;
  e_unlogged bool;
  e_partition_count int;
  e_points_only bool;
BEGIN
  -- TODO: We do not yet take UUID into account, but we should!
  FOREACH write_op IN ARRAY write_ops
  LOOP
    w_op = write_op->>'op';
    w_feature = write_op->'feature';
    IF jsonb_is(w_feature->'id', 'string') THEN
      w_id = w_feature->>'id';
    ELSE
      w_id = write_op->>'id';
    END IF;
    IF w_id IS NULL OR w_id = '' THEN
      CALL nk_raise_invalid_parameter_value('Missing collection identifier (id or feature.id');
    END IF;
    IF jsonb_is(w_feature->'properties'->'@ns:com:here:xyz'->'uuid', 'string') THEN
      w_uuid = w_feature->'properties'->'@ns:com:here:xyz'->>'uuid';
    ELSE
      w_uuid = write_op->>'uuid';
    END IF;
    -- Acquire a lock for the collection.
    lock_id = nk_lock_id(w_id);
    PERFORM pg_advisory_lock(lock_id);
    BEGIN
      -- Lets read the existing collection, if any.
      e_feature = nk_get_collection(w_id);
      IF jsonb_is(e_feature->'partition', 'boolean') THEN
        e_partition = (e_feature->'partition')::bool;
        IF e_partition THEN
          e_partition_count = 64;
        ELSE
          e_partition_count = 0;
        END IF;
      ELSE
        e_partition = false;
        e_partition_count = 0;
      END IF;
      IF jsonb_is(e_feature->'estimatedFeatureCount', 'number') THEN
        e_estimated_feature_count = (e_feature->'estimatedFeatureCount')::int8;
      ELSE
        e_estimated_feature_count = 0::int8;
      END IF;
      IF jsonb_is(e_feature->'pointsOnly', 'boolean') THEN
        e_points_only = (e_feature->'pointsOnly')::bool;
      ELSE
        e_points_only = false;
      END IF;
      IF jsonb_is(e_feature->'unlogged', 'boolean') THEN
        e_unlogged = (e_feature->'unlogged')::bool;
      ELSE
        e_unlogged = false;
      END IF;
      IF jsonb_is(w_feature->'partition', 'boolean') THEN
        w_partition = (w_feature->'partition')::bool;
      ELSE
        -- If the client does not provide the info, we assume he want it unchanged.
        w_partition = e_partition;
      END IF;
      IF jsonb_is(w_feature->'pointsOnly', 'boolean') THEN
        w_points_only = (w_feature->'pointsOnly')::bool;
      ELSE
        -- If the client does not provide the info, we assume he want it unchanged.
        w_points_only = e_points_only;
      END IF;
      IF jsonb_is(w_feature->'unlogged', 'boolean') THEN
        w_unlogged = (w_feature->'unlogged')::bool;
      ELSE
        w_unlogged = e_unlogged;
      END IF;
      IF w_op = 'PUT' THEN
        IF e_feature IS NULL THEN
          w_op = 'CREATE';
        ELSE
          w_op = 'UPDATE';
        END IF;
      END IF;
      IF w_op = 'PURGE' AND e_feature IS NULL THEN
        -- When the collection does not exist, ignore the operation.
        --RAISE NOTICE 'No such collection exists, ignore PURGE %', w_id;
        PERFORM pg_advisory_unlock(lock_id);
        CONTINUE;
      END IF;
      IF w_op = 'DELETE' AND (e_feature IS NULL OR naksha_feature_action(e_feature) = 'DELETE') THEN
        -- When the collection does not exist or is already deleted, ignore the operation.
        --RAISE NOTICE 'Collection % is already deleted, ignore DELETE action', w_id;
        PERFORM pg_advisory_unlock(lock_id);
        CONTINUE;
      END IF;
      IF w_op = 'CREATE' THEN
        IF NOT jsonb_is(w_feature, 'object') THEN
          CALL nk_raise_invalid_parameter_value('Invalid or missing feature');
        END IF;
        IF e_feature IS NOT NULL THEN
          CALL nk_raise_collection_exists(w_id);
        END IF;
        IF naksha_feature_action(e_feature) = 'DELETE' THEN
          CALL nk_raise_check_violation(format('The collection %L in deleted state, failed to create it', w_id));
        END IF;
        w_feature = jsonb_set_lax(w_feature, array['id'], to_jsonb(w_id), true, 'raise_exception');
        w_feature = nk_set_xyz_namespace_insert(w_id, nextval('naksha_collection_id_seq'), w_feature, null::geometry);
        w_feature = jsonb_set_lax(w_feature, array['type'], to_jsonb(nk_const_collection_type()), true, 'raise_exception');
        w_feature = jsonb_set_lax(w_feature, array['partition'], to_jsonb(w_partition), true, 'raise_exception');
        w_feature = jsonb_set_lax(w_feature, array['unlogged'], to_jsonb(w_unlogged), true, 'raise_exception');
        w_feature = jsonb_set_lax(w_feature, array['pointsOnly'], to_jsonb(w_points_only), true, 'raise_exception');
        IF w_partition THEN
          w_feature = jsonb_set_lax(w_feature, array['partitionCount'], to_jsonb(64), true, 'raise_exception');
        ELSE
          w_feature = jsonb_set_lax(w_feature, array['partitionCount'], to_jsonb(0), true, 'raise_exception');
        END IF;
        w_feature = jsonb_set_lax(w_feature, array['estimatedFeatureCount'], to_jsonb(e_estimated_feature_count), true, 'raise_exception');
        -- TODO: log the transaction!
        PERFORM nk_upsert_collection(w_id, w_partition, w_points_only, w_unlogged);
        sql = format('COMMENT ON TABLE %I IS %L;', w_id, w_feature);
        --RAISE NOTICE '%', sql;
        EXECUTE sql;
        r_op = 'CREATED';
        r_id = w_id;
        r_uuid = naksha_feature_uuid(w_feature);
        r_type = nk_const_collection_type();
        r_ptype = naksha_feature_ptype(w_feature);
        r_feature = w_feature;
        RETURN NEXT;
        PERFORM pg_advisory_unlock(lock_id);
        CONTINUE;
      END IF;
      IF w_op = 'PURGE' THEN
        PERFORM nk_drop_collection(w_id);
        IF naksha_feature_action(e_feature) <> 'DELETE' THEN
          -- The table was not previously deleted, simulate this now.
          e_feature = nk_set_xyz_namespace_delete(w_id, nextval('naksha_collection_id_seq'), e_feature);
        END IF;
        r_op = 'PURGED';
        r_id = w_id;
        r_uuid = naksha_feature_uuid(e_feature);
        r_type = nk_const_collection_type();
        r_ptype = naksha_feature_ptype(e_feature);
        r_feature = e_feature;
        RETURN NEXT;
        PERFORM pg_advisory_unlock(lock_id);
        CONTINUE;
      END IF;
      IF w_op = 'UPDATE' OR w_op = 'DELETE' OR w_op = 'RESTORE' THEN
        --RAISE NOTICE 'w_op = %', w_op;
        IF e_feature IS NULL THEN
          CALL nk_raise_collection_not_exists(w_id);
        END IF;
        IF w_op = 'RESTORE' THEN
          r_op = 'RESTORED';
          --RAISE NOTICE 'r_op = %', r_op;
          IF NOT jsonb_is(w_feature, 'object') THEN
            w_feature = nk_set_xyz_namespace_update(w_id, nextval('naksha_collection_id_seq'), e_feature, null::geometry, e_feature);
          ELSE
            w_feature = jsonb_set_lax(w_feature, array['id'], to_jsonb(w_id), true, 'raise_exception');
            w_feature = nk_set_xyz_namespace_update(w_id, nextval('naksha_collection_id_seq'), w_feature, null::geometry, e_feature);
          END IF;
        ELSIF w_op = 'DELETE' THEN
          r_op = 'DELETED';
          --RAISE NOTICE 'r_op = %', r_op;
          w_feature = nk_set_xyz_namespace_delete(w_id, nextval('naksha_collection_id_seq'), e_feature);
        ELSE -- UPDATE
          IF naksha_feature_action(e_feature) = 'DELETE' THEN
            CALL nk_raise_check_violation(format('Failed to update collection %L, collection is in deleted state', w_id));
          END IF;
          r_op = 'UPDATED';
          --RAISE NOTICE 'r_op = %', r_op;
          IF NOT jsonb_is(w_feature, 'object') THEN
            CALL nk_raise_invalid_parameter_value('Invalid or missing feature');
          END IF;
          IF e_partition <> w_partition THEN
            CALL nk_raise_invalid_parameter_value('Can''t change the partitioning, this can only be done when creating a collection');
          END IF;
          IF e_unlogged <> w_unlogged THEN
            CALL nk_raise_invalid_parameter_value('Can''t change unlogged option, this can only be done when creating a collection');
          END IF;
          IF e_points_only <> w_points_only THEN
            CALL nk_raise_invalid_parameter_value('Can''t change points-only behavior, this can only be done when creating a collection');
          END IF;
          w_feature = nk_set_xyz_namespace_update(w_id, nextval('naksha_collection_id_seq'), w_feature, null::geometry, e_feature);
        END IF;
        w_feature = jsonb_set_lax(w_feature, array['id'], to_jsonb(w_id), true, 'raise_exception');
        w_feature = jsonb_set_lax(w_feature, array['type'], to_jsonb(nk_const_collection_type()), true, 'raise_exception');
        w_feature = jsonb_set_lax(w_feature, array['partition'], to_jsonb(e_partition), true, 'raise_exception');
        w_feature = jsonb_set_lax(w_feature, array['unlogged'], to_jsonb(e_unlogged), true, 'raise_exception');
        w_feature = jsonb_set_lax(w_feature, array['pointsOnly'], to_jsonb(e_points_only), true, 'raise_exception');
        IF e_partition THEN
          w_feature = jsonb_set_lax(w_feature, array['partitionCount'], to_jsonb(64), true, 'raise_exception');
        ELSE
          w_feature = jsonb_set_lax(w_feature, array['partitionCount'], to_jsonb(0), true, 'raise_exception');
        END IF;
        w_feature = jsonb_set_lax(w_feature, array['estimatedFeatureCount'], to_jsonb(e_estimated_feature_count), true, 'raise_exception');
        -- TODO: log the transaction!
        sql = format('COMMENT ON TABLE %I IS %L;', w_id, w_feature);
        --RAISE NOTICE '%', sql;
        EXECUTE sql;
        r_id = w_id;
        r_uuid = naksha_feature_uuid(w_feature);
        r_type = nk_const_collection_type();
        r_ptype = naksha_feature_ptype(w_feature);
        r_feature = w_feature;
        RETURN NEXT;
        PERFORM pg_advisory_unlock(lock_id);
        CONTINUE;
      END IF;
      RAISE EXCEPTION 'Unknown write operation: %', w_op;
    EXCEPTION WHEN OTHERS THEN
      -- Release the lock of the collection, when any error happened.
      PERFORM pg_advisory_unlock(lock_id);
      -- Re-raise the exception.
      RAISE;
    END;
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION naksha_write_features( collection_id text, write_ops jsonb[], geometries geometry[])
  RETURNS TABLE (r_op text, r_id text, r_uuid text, r_type text, r_ptype text, r_feature jsonb, r_geometry geometry)
  LANGUAGE 'plpgsql' VOLATILE STRICT AS $$
DECLARE
  collection      jsonb;
  table_name      text;
  arr_size        int;
  id_arr          text array;
  feature_arr     jsonb array;
  id_index_arr    int array;
  write_op        jsonb;
  e_id_arr        text array;
  e_uuid_arr      text array;
  e_uuid_i        int;
  e_uuid_len      int;
  i               int;
  index           int;
  id              text;
  uuid            text;
  feature         jsonb;
  geo             geometry;
  e_uuid          text;
  op              text;
  stmt            text;
  insert_stmt     text;
  update_stmt     text;
  delete_stmt     text;
  purge_stmt      text;
  ids_map         hstore;
BEGIN
  --RAISE NOTICE '------------------ START ----------------------------';
  -- See: https://www.postgresql.org/docs/current/errcodes-appendix.html
  -- 22023 invalid_parameter_value
  collection = naksha_collection_get(collection_id);
  IF collection IS NULL THEN
    CALL nk_raise_invalid_parameter_value(format('Unknown collection %I', collection_id));
  END IF;
  table_name = collection_id;
  arr_size = array_length(write_ops, 1);
  IF geometries IS NULL OR array_length(geometries, 1) != arr_size THEN
    CALL nk_raise_invalid_parameter_value('"geometries" must not be null and have same size as the write_ops array');
  END IF;

  --RAISE NOTICE 'Start';
  id_arr = ARRAY[]::text[];
  feature_arr = ARRAY[]::jsonb[];
  i = 1;
  WHILE i <= arr_size
  LOOP
    write_op = write_ops[i];
    IF NOT jsonb_is(write_op, 'object') THEN
      CALL nk_raise_invalid_parameter_value(format('[%s] The write_op is no valid JSON object', i));
    END IF;
    op = write_op->>'op';
    IF op IS NULL THEN
      CALL nk_raise_invalid_parameter_value(format('[%s] Invalid or missing operation', i));
    END IF;
    IF op != 'CREATE' AND op != 'UPDATE' AND op != 'PUT' AND op != 'DELETE' AND op != 'PURGE' THEN
      CALL nk_raise_invalid_parameter_value(format('[%] Unknown operation %L', i, op));
    END IF;
    feature = write_op->'feature';
    id = naksha_feature_id(feature);
    IF id IS NULL THEN
      id = write_op->>'id';
      IF id IS NULL THEN
        id = md5(random()::text);
        --RAISE NOTICE 'Generate id for write_ops[%]: %', i, id;
        IF NOT jsonb_is(feature, 'object') THEN
          CALL nk_raise_invalid_parameter_value(format('[%s] No feature and no id given for operation %L', i, op));
        END IF;
        feature = jsonb_set_lax(feature, array['id'], to_jsonb(id), true, 'raise_exception');
      END IF;
    END IF;
    uuid = naksha_feature_uuid(feature);
    IF uuid IS NULL THEN
      uuid = write_op->>'uuid';
    END IF;
    IF ids_map ? id THEN
      CALL nk_raise_invalid_parameter_value(format('[%s] Duplicate id %L found', i, id));
    END IF;
    ids_map[id] = uuid;
    id_arr = array_append(id_arr, id);
    feature_arr = array_append(feature_arr, feature);
    i = i + 1;
  END LOOP;
  -- Order ids and attach ordinal (index), then select back into arrays.
  WITH ordered_ids AS (SELECT "unnest", "ordinality" FROM unnest(id_arr) WITH ORDINALITY ORDER BY "unnest")
  SELECT ARRAY(SELECT "unnest" FROM ordered_ids), ARRAY(SELECT "ordinality" FROM ordered_ids)
  INTO id_arr, id_index_arr;
  --RAISE NOTICE 'Ordered ids: % %', id_arr, id_index_arr;

  -- Read ids and their uuid, lock rows for update.
  stmt = format('WITH id_and_uuid AS ('
      || 'SELECT jsondata->>''id'' as "id", jsondata->''properties''->''@ns:com:here:xyz''->>''uuid'' as "uuid" '
      || 'FROM %I WHERE jsondata->>''id'' = ANY($1) '
      --|| 'ORDER BY jsondata->>''id'' '
      || 'FOR UPDATE '
      || ') SELECT ARRAY(SELECT id FROM id_and_uuid), ARRAY(SELECT uuid FROM id_and_uuid) FROM id_and_uuid '
      || 'LIMIT 1', collection_id);
  --RAISE NOTICE 'Select ids and uuids: %', stmt;
  EXECUTE stmt USING id_arr INTO e_id_arr, e_uuid_arr;
  --RAISE NOTICE 'ids, uuids: % %', e_id_arr, e_uuid_arr;

  --RAISE NOTICE 'Perform all actions';
  insert_stmt = format('INSERT INTO %I (jsondata, geo) VALUES ($1, ST_Force3D($2)) RETURNING jsondata;', table_name);
  update_stmt = format('UPDATE %I SET jsondata=$1, geo=ST_Force3D($2) WHERE jsondata->>''id''=$3 RETURNING jsondata;', table_name);
  delete_stmt = format('DELETE FROM %I WHERE jsondata->>''id''=$1 RETURNING jsondata, geo;', table_name);
  purge_stmt = format('DELETE FROM %I WHERE jsondata->>''id''=$1 RETURNING jsondata, geo;', collection_id||'_del');
  i = 1;
  e_uuid_i = 1;
  e_uuid_len = array_length(e_uuid_arr, 1);
  WHILE i <= arr_size
  LOOP
    id = id_arr[i];
    IF e_uuid_i <= e_uuid_len AND id = e_id_arr[e_uuid_i] THEN
        e_uuid = e_uuid_arr[e_uuid_i];
        e_uuid_i = e_uuid_i + 1;
    ELSE
        e_uuid = NULL;
    END IF;
    index = id_index_arr[i];
    feature = feature_arr[index];
    geo = geometries[index];
    uuid = ids_map[id];
    op = write_ops[index]->>'op';
    r_op = NULL;
    --RAISE NOTICE 'Op ''%'' feature ''%'' (uuid: ''%'', expected: ''%'')', op, id, e_uuid, uuid;

    -- TODO HP_QUERY : Is it better to return '23505' (unique_violation) for ID / UUID mismatches?
    -- TODO HP_QUERY : And '02000' for no_data found situation
    IF op = 'CREATE' THEN
      IF e_uuid IS NOT NULL THEN
        CALL nk_raise_unique_violation(format('[%s] Failed to create already existing feature %L', index, id));
      END IF;

      EXECUTE insert_stmt USING feature, geo INTO feature;
      r_op = 'CREATED';
    ELSEIF op = 'UPDATE' THEN
      IF uuid IS NOT NULL THEN
        IF uuid != e_uuid THEN
          CALL nk_raise_check_violation(format('[%s] The feature %L is not in expected state %L, found: %L', index, id, uuid, e_uuid));
        END IF;
      END IF;
      IF e_uuid IS NULL THEN
        CALL nk_raise_check_violation(format('[%s] Failed to update not existing feature %L', index, id));
      END IF;

      EXECUTE update_stmt USING feature, geo, id INTO feature;
      r_op = 'UPDATED';
    ELSEIF op = 'PUT' THEN
      IF uuid IS NOT NULL THEN
        IF uuid != e_uuid THEN
          CALL nk_raise_check_violation(format('[%s] The feature %L is not in expected state %L, found: %L', index, id, uuid, e_uuid));
        END IF;
      END IF;

      IF e_uuid IS NOT NULL THEN
        EXECUTE update_stmt USING feature, geo, id INTO feature;
        r_op = 'UPDATED';
      ELSE
        EXECUTE insert_stmt USING feature, geo INTO feature;
        r_op = 'CREATED';
      END IF;
    ELSEIF op = 'DELETE' THEN
      IF uuid IS NOT NULL THEN
        IF uuid != e_uuid THEN
          CALL nk_raise_check_violation(format('[%s] The feature %L is not in expected state %L, found: %L', index, id, uuid, e_uuid));
        END IF;
      END IF;

      IF e_uuid IS NOT NULL THEN
        EXECUTE delete_stmt USING id INTO feature, geo;
        -- We can't change what is returned by a DELETE (PostgresQL does not allow this), therefore we simulate it.
        -- We know, that this will produce exactly the state that now is in the _del table, but this avoids that we need to query it.
        feature = nk_set_xyz_namespace_delete(collection_id, nk_get_config(nk_const_del_uid())::int8, feature);
        r_op = 'DELETED';
      END IF;
    ELSEIF op = 'PURGE' THEN
      IF uuid IS NOT NULL THEN
        IF uuid != e_uuid THEN
          CALL nk_raise_check_violation(format('[%s] The feature %L is not in expected state %L, found: %L', index, id, uuid, e_uuid));
        END IF;
      END IF;

      IF e_uuid IS NOT NULL THEN
        EXECUTE delete_stmt USING id INTO feature, geo;
      END IF;
      EXECUTE purge_stmt USING id INTO feature, geo;
      r_op = 'PURGED';
    END IF;

    IF r_op IS NOT NULL THEN
      r_id = id;
      r_uuid = naksha_feature_uuid(feature);
      r_type = naksha_feature_type(feature);
      r_ptype = naksha_feature_ptype(feature);
      IF NOT min_result THEN
        r_feature = feature;
        r_geometry = geo;
      END IF;
      RETURN next;
    END IF;
    i = i + 1;
  END LOOP;
END $$;

-- Returns the id of the given JSON feature or null, if the given element is no valid JSON feature.
CREATE OR REPLACE FUNCTION naksha_feature_id(t anyelement) RETURNS text LANGUAGE 'plpgsql' STABLE AS $BODY$
DECLARE
  j jsonb;
BEGIN
  j = t::json;
  IF jsonb_typeof(j->'id') = 'string' THEN
    RETURN j->>'id';
  END IF;
  RETURN NULL;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END;
$BODY$;

-- Returns the uuid of the given JSON feature or null, if the given element is no valid JSON feature
-- or does not have a valid UUID.
CREATE OR REPLACE FUNCTION naksha_feature_uuid(t anyelement) RETURNS text LANGUAGE 'plpgsql' STABLE AS $$ BEGIN
  RETURN naksha_feature_uuid(t, null);
END; $$;

-- Returns the uuid of the given JSON feature or the given alternative, if the given element is no valid JSON feature
-- or does not have a valid UUID.
CREATE OR REPLACE FUNCTION naksha_feature_uuid(t anyelement, alternative text) RETURNS text LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
  j jsonb;
BEGIN
  j = t::json;
  IF jsonb_typeof(j->'properties'->'@ns:com:here:xyz'->'uuid') = 'string' THEN
    RETURN j->'properties'->'@ns:com:here:xyz'->>'uuid';
  END IF;
  RETURN alternative;
EXCEPTION WHEN OTHERS THEN
  RETURN alternative;
END; $$;

-- Returns the type of the given JSON or null, if the given text is no valid JSON feature.
CREATE OR REPLACE FUNCTION naksha_feature_type(t anyelement) RETURNS text LANGUAGE 'plpgsql' STABLE AS $BODY$
DECLARE
  f jsonb;
BEGIN
  f = t::jsonb;
  IF jsonb_typeof(f->'type') = 'string' THEN
    RETURN f->>'type';
  END IF;
  RETURN NULL;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END;
$BODY$;

-- Returns the properties-type of the given JSON or null,
-- if the given text is no valid JSON feature
-- or does not have properties
-- or the properties do not have a type.
CREATE OR REPLACE FUNCTION naksha_feature_ptype(t anyelement) RETURNS text LANGUAGE 'plpgsql' STABLE AS $BODY$
DECLARE
  f jsonb;
BEGIN
  f = t::jsonb;
  IF jsonb_typeof(f->'properties'->'type') = 'string' THEN
    RETURN f->'properties'->>'type';
  END IF;
  IF jsonb_typeof(f->'properties'->'featureType') = 'string' THEN
    RETURN f->'properties'->>'featureType';
  END IF;
  IF jsonb_typeof(f->'momType') = 'string' THEN
    RETURN f->>'momType';
  END IF;
  RETURN NULL;
EXCEPTION
  WHEN OTHERS THEN RETURN NULL;
END;
$BODY$;

-- Returns the action of the given JSON feature or null, if the given element is no valid JSON feature.
CREATE OR REPLACE FUNCTION naksha_feature_action(t anyelement) RETURNS text LANGUAGE 'plpgsql' STABLE AS $BODY$
DECLARE
  j jsonb;
BEGIN
  j = t::json;
  IF jsonb_typeof(j->'properties'->'@ns:com:here:xyz'->'action') = 'string' THEN
    RETURN j->'properties'->'@ns:com:here:xyz'->>'action';
  END IF;
  RETURN NULL;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END;
$BODY$;

-- Returns the merged reference id.
-- This is the "crid" of the given JSON feature, if it has any.
-- If the feature does not have a "crid", but a "grid", the "grid" is returned.
-- If the feature is basically not well-formatted, the method returns null (should normally not happen).
CREATE OR REPLACE FUNCTION naksha_feature_mrid( t anyelement ) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
DECLARE
  j jsonb;
BEGIN
  j = t::json;
  IF jsonb_typeof(j->'properties'->'@ns:com:here:xyz'->'crid') = 'string' THEN
    RETURN j->'properties'->'@ns:com:here:xyz'->>'crid';
  END IF;
  IF jsonb_typeof(j->'properties'->'@ns:com:here:xyz'->'grid') = 'string' THEN
    RETURN j->'properties'->'@ns:com:here:xyz'->>'grid';
  END IF;
  RETURN NULL;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END;
$BODY$;

CREATE OR REPLACE FUNCTION nk__________OLD_________() RETURNS void LANGUAGE 'plpgsql' IMMUTABLE AS $$ BEGIN END $$;

-- Returns a packed Naksha extension version from the given parameters (major, minor, revision).
CREATE OR REPLACE FUNCTION naksha_version_of(major int, minor int, revision int) RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $$
BEGIN
  return ((major::int8 & x'ffff'::int8) << 32)
    | ((minor::int8 & x'ffff'::int8) << 16)
    | (revision::int8 & x'ffff'::int8);
END $$;

-- Unpacks the given packed Naksha version into its parts (major, minor, revision).
CREATE OR REPLACE FUNCTION naksha_version_extract(version int8)
    RETURNS TABLE (major int, minor int, revision int)
    LANGUAGE 'plpgsql' STRICT IMMUTABLE AS $$
BEGIN
    RETURN QUERY SELECT ((version >> 32) & x'ffff'::int8)::int  as "major",
                        ((version >> 16) & x'ffff'::int8)::int  as "minor",
                        (version & x'ffff'::int8)::int          as "revision";
END; $$;

-- Unpacks the given Naksha extension version into a human readable text.
CREATE OR REPLACE FUNCTION naksha_version_to_text(version int8) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $$
BEGIN
    return format('%s.%s.%s',
                  (version >> 32) & x'ffff'::int8,
                  (version >> 16) & x'ffff'::int8,
                  (version & x'ffff'::int8));
END $$;

-- Parses a human readable Naksha extension version and returns a packed version.
CREATE OR REPLACE FUNCTION naksha_version_parse(version text) RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
DECLARE
    v text[];
BEGIN
    v := string_to_array(version, '.');
    return naksha_version_of(v[1]::int, v[2]::int, v[3]::int);
END $BODY$;

-- Helper to test if a text is a valid JSON.
CREATE OR REPLACE FUNCTION naksha_is_json(t TEXT) RETURNS boolean LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
  RETURN (t::json IS NOT NULL);
EXCEPTION
  WHEN OTHERS THEN RETURN FALSE;
END;
$BODY$;

-- Converts the UUID into a 16 byte array.
CREATE OR REPLACE FUNCTION naksha_uuid_to_bytes(the_uuid uuid)
    RETURNS bytea
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
BEGIN
    return DECODE(REPLACE(the_uuid::text, '-', ''), 'hex');
END
$BODY$;

-- Converts the 16 byte long byte-array into a UUID.
CREATE OR REPLACE FUNCTION naksha_uuid_from_bytes(raw_uuid bytea)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
BEGIN
    RETURN CAST(ENCODE(raw_uuid, 'hex') AS UUID);
END
$BODY$;

-- Create a UUID from the given object identifier, timestamp, type and storage identifier.
-- Review here: https://realityripple.com/Tools/UnUUID/
CREATE OR REPLACE FUNCTION naksha_uuid_of(object_id int8, ts timestamptz, type int, storage_id int8)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
DECLARE
    raw_uuid bytea;
    year int8;
    month int8;
    day int8;
    t int8;
BEGIN
    object_id := object_id & x'0fffffffffffffff'::int8; -- 60 bit
    year := (EXTRACT(year FROM ts)::int8 - 2000::int8) & x'00000000000003ff'::int8;
    month := EXTRACT(month FROM ts)::int8;
    day := EXTRACT(day FROM ts)::int8;
    t := type::int8 & x'0000000000000007'::int8; -- 3 bit
    storage_id := storage_id & x'0000000fffffffff'::int8; -- 40 bit
    raw_uuid := set_byte('\x00000000000000000000000000000000'::bytea, 0, 0);
    -- 48 high bit of object_id in big endian order (6 byte)
    raw_uuid := set_byte(raw_uuid, 0, ((object_id >> 52) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 1, ((object_id >> 44) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 2, ((object_id >> 36) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 3, ((object_id >> 28) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 4, ((object_id >> 20) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 5, ((object_id >> 12) & x'ff'::int8)::int);
    -- 4 bit version (1000), 4 bit of object_id
    raw_uuid := set_byte(raw_uuid, 6, (x'40'::int8 | ((object_id >> 8) & x'0f'::int8))::int);
    -- low 8 bit of object_id
    raw_uuid := set_byte(raw_uuid, 7, (object_id & x'ff'::int8)::int);
    -- 2 bit variant (10), 6 high bit of year
    raw_uuid := set_byte(raw_uuid, 8, (((year >> 4) & x'3f'::int8) | 128)::int);
    -- 4 low bit year, month (has only 4 bit)
    raw_uuid := set_byte(raw_uuid, 9, (((year & x'0f'::int8) << 4) | month)::int);
    -- 5 bit day, 3 bit object type
    raw_uuid := set_byte(raw_uuid, 10, ((day << 3) | t)::int);
    -- 40 bit connector_id (5 byte)
    raw_uuid := set_byte(raw_uuid, 11, ((storage_id >> 32) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 12, ((storage_id >> 24) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 13, ((storage_id >> 16) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 14, ((storage_id >> 8) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 15, (storage_id & x'ff'::int8)::int);
    RETURN CAST(ENCODE(raw_uuid, 'hex') AS UUID);
END
$BODY$;

-- Extracts the object_id from the given Naksha UUID bytes.
CREATE OR REPLACE FUNCTION naksha_uuid_bytes_get_object_id(raw_uuid bytea)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
BEGIN
    return ((get_byte(raw_uuid, 0)::int8) << 52)
         | ((get_byte(raw_uuid, 1)::int8) << 44)
         | ((get_byte(raw_uuid, 2)::int8) << 36)
         | ((get_byte(raw_uuid, 3)::int8) << 28)
         | ((get_byte(raw_uuid, 4)::int8) << 20)
         | ((get_byte(raw_uuid, 5)::int8) << 12)
         | ((get_byte(raw_uuid, 6)::int8 & x'0f'::int8) << 8)
         | ((get_byte(raw_uuid, 7)::int8));
END
$BODY$;

-- Extracts the timestamp from the given Naksha UUID bytes.
CREATE OR REPLACE FUNCTION naksha_uuid_bytes_get_ts(raw_uuid bytea)
    RETURNS timestamptz
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
DECLARE
    year int;
    month int;
    day int;
BEGIN
    -- 6 high bit from byte 8 plus 4 low bit from byte 9
    year = 2000 + (((get_byte(raw_uuid, 8) & x'3f'::int) << 4) | (get_byte(raw_uuid, 9) >> 4));
    -- 4 low bit from byte 9
    month = get_byte(raw_uuid, 9) & x'0f'::int;
    -- 5 high bit from byte 10
    day = get_byte(raw_uuid, 10) >> 3;
    return format('%s-%s-%s', year, month, day)::timestamptz;
END
$BODY$;

-- Returns the partition identifier from the given Naksha UUID bytes.
CREATE OR REPLACE FUNCTION naksha_uuid_bytes_get_part_id(raw_uuid bytea)
    RETURNS int
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
DECLARE
    year int;
    month int;
    day int;
BEGIN
    -- 6 high bit from byte 8 plus 4 low bit from byte 9
    year = 2000 + (((get_byte(raw_uuid, 8) & x'3f'::int) << 4) | (get_byte(raw_uuid, 9) >> 4));
    -- 4 low bit from byte 9
    month = get_byte(raw_uuid, 9) & x'0f'::int;
    -- 5 high bit from byte 10
    day = get_byte(raw_uuid, 10) >> 3;
    return ((year << 9) | (month << 5) | day)::int;
END
$BODY$;

-- Extracts the object type from the given Naksha UUID bytes.
-- 0 = transaction
-- 1 = feature
CREATE OR REPLACE FUNCTION naksha_uuid_bytes_get_type(raw_uuid bytea)
    RETURNS int
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
BEGIN
    return get_byte(raw_uuid, 10) & x'07'::int;
END
$BODY$;

-- Extracts the storage identifier from the given Naksha UUID bytes.
CREATE OR REPLACE FUNCTION naksha_uuid_bytes_get_storage_id(raw_uuid bytea)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
BEGIN
    return ((get_byte(raw_uuid, 11)::int8) << 32)
         | ((get_byte(raw_uuid, 12)::int8) << 24)
         | ((get_byte(raw_uuid, 13)::int8) << 16)
         | ((get_byte(raw_uuid, 14)::int8) << 8)
         | ((get_byte(raw_uuid, 15)::int8));
END
$BODY$;

-- Fully extracts all values encoded into a Naksha UUID.
CREATE OR REPLACE FUNCTION naksha_uuid_extract(the_uuid uuid)
    RETURNS TABLE
            (
                object_id int8,
                year      int,
                month     int,
                day       int,
                type      int,
                connector_id     int8
            )
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
DECLARE
    uuid_bytes bytea;
    object_id int8;
    ts timestamptz;
    type int;
    storage_id int8;
    year int;
    month int;
    day int;
BEGIN
    uuid_bytes := naksha_uuid_to_bytes(the_uuid);
    object_id := naksha_uuid_bytes_get_object_id(uuid_bytes);
    ts := naksha_uuid_bytes_get_ts(uuid_bytes);
    year := EXTRACT(year FROM ts);
    month := EXTRACT(month FROM ts);
    day := EXTRACT(day FROM ts);
    type := naksha_uuid_bytes_get_type(uuid_bytes);
    storage_id := naksha_uuid_bytes_get_storage_id(uuid_bytes);
    RETURN QUERY SELECT object_id    as "object_id",
                        year         as "year",
                        month        as "month",
                        day          as "day",
                        type         as "type",
                        storage_id   as "storage_id";
END
$BODY$;

-- Returns the partition identifier for the given timestamp.
CREATE OR REPLACE FUNCTION naksha_part_id_from_ts(ts timestamptz)
    RETURNS int
    LANGUAGE 'plpgsql' IMMUTABLE
AS $$
DECLARE
    year int;
    month int;
    day int;
BEGIN
    year := EXTRACT(year FROM ts)::int;
    month := EXTRACT(month FROM ts)::int;
    day := EXTRACT(day FROM ts)::int;
    RETURN (year << 9) | (month << 5) | day;
END;
$$;

-- Returns the partition identifier for the given UUID.
CREATE OR REPLACE FUNCTION naksha_part_id_from_uuid(the_uuid uuid)
    RETURNS int
    LANGUAGE 'plpgsql' IMMUTABLE
AS $$
BEGIN
    RETURN naksha_uuid_bytes_get_part_id(naksha_uuid_to_bytes(the_uuid));
END;
$$;

-- Extracts the year, month and day being part of the partition identifier.
CREATE OR REPLACE FUNCTION naksha_part_id_extract(date_id int)
    RETURNS TABLE
            (
                year      int,
                month     int,
                day       int
            )
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
BEGIN
    RETURN QUERY SELECT (date_id >> 9) as "year",
                        ((date_id >> 5) & x'1f'::int) as "month",
                        (date_id & x'1f'::int) as "day";
END
$BODY$;

-- Creates a feature UUID from the given object_id and timestamp.
CREATE OR REPLACE FUNCTION naksha_feature_uuid_from_object_id_and_ts(object_id int8, ts timestamptz)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS $$
-- noinspection SqlUnused
DECLARE
    TRANSACTION CONSTANT int := 0;
    FEATURE CONSTANT int := 1;
BEGIN
    RETURN naksha_uuid_of(object_id, ts, FEATURE, naksha_storage_id());
END;
$$;

-- Creates a transaction UUID (txn) from the given object_id and timestamp.
CREATE OR REPLACE FUNCTION naksha_txn_from_object_id_and_ts(object_id int8, ts timestamptz)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS $$
-- noinspection SqlUnused
DECLARE
    TRANSACTION CONSTANT int := 0;
    FEATURE CONSTANT int := 1;
BEGIN
    RETURN naksha_uuid_of(object_id, ts, TRANSACTION, naksha_storage_id());
END;
$$;


-- Returns the XYZ namespace of the given feature or null, if no valid JSON feature given or it has no XYZ namespace.
CREATE OR REPLACE FUNCTION naksha_feature_xyz(t anyelement) RETURNS jsonb LANGUAGE 'plpgsql' STABLE AS $BODY$
DECLARE
  f jsonb;
BEGIN
  f = t::jsonb;
  IF jsonb_typeof(f->'properties'->'@ns:com:here:xyz') = 'object' THEN
    RETURN f->'properties'->'@ns:com:here:xyz';
  END IF;
  RETURN NULL;
EXCEPTION
  WHEN OTHERS THEN RETURN NULL;
END;
$BODY$;

-- Returns the "version" from the given feature.
CREATE OR REPLACE FUNCTION naksha_feature_get_version(f jsonb)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'version')::int8;
EXCEPTION WHEN OTHERS THEN
    RETURN 1::int8;
END
$BODY$;

-- Returns the "action" from the given feature (CREATE, UPDATE, DELETE or PURGE).
CREATE OR REPLACE FUNCTION naksha_feature_get_action(f jsonb)
    RETURNS text
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN f->'properties'->'@ns:com:here:xyz'->>'action';
END
$BODY$;

-- Returns the author of the given feature state.
CREATE OR REPLACE FUNCTION naksha_feature_get_author(f jsonb)
    RETURNS text
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN f->'properties'->'@ns:com:here:xyz'->>'author';
END
$BODY$;

-- Returns the application identifier of the given feature state.
CREATE OR REPLACE FUNCTION naksha_feature_get_app_id(f jsonb)
    RETURNS text
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN f->'properties'->'@ns:com:here:xyz'->>'appId';
END
$BODY$;

-- Returns the transaction UUID from the given feature.
CREATE OR REPLACE FUNCTION naksha_feature_get_txn(f jsonb)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    return (f->'properties'->'@ns:com:here:xyz'->>'txn')::uuid;
END
$BODY$;

-- Returns the partition identifier encoded in the transaction number of the given feature.
CREATE OR REPLACE FUNCTION naksha_feature_get_part_id(f jsonb)
    RETURNS int
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    return naksha_uuid_bytes_get_part_id(naksha_uuid_to_bytes((f->'properties'->'@ns:com:here:xyz'->>'txn')::uuid));
END
$BODY$;

-- Returns the state identifier UUID from the given feature.
CREATE OR REPLACE FUNCTION naksha_feature_get_uuid(f jsonb)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'uuid')::uuid;
END
$BODY$;

-- Returns the previous state identifier UUID from the given feature.
CREATE OR REPLACE FUNCTION naksha_feature_get_puuid(f jsonb)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'puuid')::uuid;
END
$BODY$;

-- Returns the createAt epoch timestamp in milliseconds of when the feature was created.
CREATE OR REPLACE FUNCTION naksha_feature_get_createdAt(f jsonb)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'createdAt')::int8;
END
$BODY$;

-- Returns the createAt epoch timestamp in milliseconds of when the feature was modified.
CREATE OR REPLACE FUNCTION naksha_feature_get_updatedAt(f jsonb)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'updatedAt')::int8;
END
$BODY$;

-- Returns the real-time epoch timestamp in milliseconds of when the feature was created.
CREATE OR REPLACE FUNCTION naksha_feature_get_rtcts(f jsonb)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'rtcts')::int8;
END
$BODY$;

-- Returns the real-time epoch timestamp in milliseconds of when the feature was modified.
CREATE OR REPLACE FUNCTION naksha_feature_get_rtuts(f jsonb)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'rtuts')::int8;
END
$BODY$;

-- Returns the author that did the last modification from the given feature.
CREATE OR REPLACE FUNCTION naksha_feature_get_lastUpdatedBy(f jsonb)
    RETURNS text
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN f->'properties'->'@ns:com:here:mom:meta'->>'lastUpdatedBy';
END
$BODY$;

-- Returns the current milliseconds (epoch time) form the given timestamp.
CREATE OR REPLACE FUNCTION naksha_current_millis(ts timestamptz)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    -- Note: "epoch": returns the number of seconds, including fractional parts.
    return (EXTRACT(epoch from ts) * 1000)::int8;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_part_id_from_ts(ts timestamptz)
    RETURNS int
    LANGUAGE 'plpgsql' IMMUTABLE
AS $$
DECLARE
    year int;
    month int;
    day int;
BEGIN
    year := EXTRACT(year FROM ts)::int;
    month := EXTRACT(month FROM ts)::int;
    day := EXTRACT(day FROM ts)::int;
    RETURN (year << 9) | (month << 5) | day;
END;
$$;

-- Returns the current microseconds (epoch time) form the given timestamp.
CREATE OR REPLACE FUNCTION naksha_current_micros(ts timestamptz)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    -- Note: "epoch": returns the number of seconds, including fractional parts.
    return (EXTRACT(epoch from ts) * 1000000)::int8;
END
$BODY$;

CREATE OR REPLACE FUNCTION __naksha_get_postgres_version()
    RETURNS jsonb
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
DECLARE
    version_number VARCHAR;
    major_version INTEGER;
    minor_version INTEGER;
    result_json jsonb;
BEGIN
    SELECT split_part(version(), ' ', 2) INTO version_number;
    SELECT split_part(version_number, '.', 1) INTO major_version;
    SELECT split_part(version_number, '.', 2) INTO minor_version;

    result_json := jsonb_build_object('major', major_version, 'minor', minor_version);
    RETURN result_json;
END;
$BODY$;

CREATE OR REPLACE FUNCTION __naksha_pg_version()
    RETURNS int
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (__naksha_get_postgres_version()->>'major')::int;
END;
$BODY$;

-- A trigger attached to the HEAD table of all collections to fix the XYZ namespace.
-- In a nutshell, fix: jsondata->'properties'->'@ns:com:here:xyz'
CREATE OR REPLACE FUNCTION __naksha_trigger_fix_ns_xyz()
    RETURNS trigger
    LANGUAGE 'plpgsql' STABLE
AS $BODY$
DECLARE
    author text;
    app_id text;
    new_uuid uuid;
    txn uuid;
    ts_millis int8;
    rts_millis int8;
    i int8;
    xyz jsonb;
BEGIN
    rts_millis := naksha_current_millis(clock_timestamp());
    ts_millis := naksha_current_millis(current_timestamp);
    txn := naksha_tx_current();
    i = nextval('"'||TG_TABLE_SCHEMA||'"."'||TG_TABLE_NAME||'_i_seq"');
    new_uuid := naksha_feature_uuid_from_object_id_and_ts(i, current_timestamp);
    app_id := naksha_tx_get_app_id();

    IF TG_OP = 'INSERT' THEN
        -- RAISE NOTICE '__naksha_trigger_fix_ns_xyz % %', TG_OP, NEW.jsondata;
        author := naksha_tx_get_author(null);
        xyz := jsonb_build_object(
            'action', 'CREATE',
            'version', 1::int8,
            'collection', TG_TABLE_NAME,
            'author', author,
            'appId', app_id,
            'puuid', null,
            'uuid', new_uuid::text,
            'txn', txn::text,
            'createdAt', ts_millis,
            'updatedAt', ts_millis,
            'rtcts', rts_millis,
            'rtuts', rts_millis
        );
        --RAISE NOTICE '__naksha_trigger_fix_ns_xyz %', xyz;
        IF NEW.jsondata->'properties' IS NULl THEN
            NEW.jsondata = jsonb_set(NEW.jsondata, '{"properties"}', '{}'::jsonb, true);
        ELSEIF NEW.jsondata->'properties'->'@ns:com:here:xyz' IS NOT NULL
            AND NEW.jsondata->'properties'->'@ns:com:here:xyz'->'tags' IS NOT NULL THEN
            xyz := jsonb_set(xyz, '{"tags"}', NEW.jsondata->'properties'->'@ns:com:here:xyz'->'tags', true);
        END IF;
        --RAISE NOTICE '__naksha_trigger_fix_ns_xyz %', xyz;
        NEW.jsondata = jsonb_set(NEW.jsondata, '{"properties","@ns:com:here:xyz"}', xyz, true);
        NEW.i = i;
        --RAISE NOTICE '__naksha_trigger_fix_ns_xyz return %', NEW.jsondata;
        return NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        --RAISE NOTICE '__naksha_trigger_fix_ns_xyz % %', TG_OP, NEW.jsondata;
        author := naksha_tx_get_author(OLD.jsondata);
        xyz := jsonb_build_object(
            'action', 'UPDATE',
            'version', (OLD.jsondata->'properties'->'@ns:com:here:xyz'->>'version')::int8 + 1::int8,
            'collection', TG_TABLE_NAME,
            'author', author,
            'appId', app_id,
            'puuid', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'uuid',
            'uuid', new_uuid::text,
            'txn', txn::text,
            'createdAt', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'createdAt',
            'updatedAt', ts_millis,
            'rtcts', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'rtcts',
            'rtuts', rts_millis
        );
        IF NEW.jsondata->'properties' IS NULl THEN
            NEW.jsondata = jsonb_set(NEW.jsondata, '{"properties"}', '{}'::jsonb, true);
        ELSEIF NEW.jsondata->'properties'->'@ns:com:here:xyz' IS NOT NULL
            AND NEW.jsondata->'properties'->'@ns:com:here:xyz'->'tags' IS NOT NULL THEN
            xyz := jsonb_set(xyz, '{"tags"}', NEW.jsondata->'properties'->'@ns:com:here:xyz'->'tags', true);
        END IF;
        NEW.jsondata = jsonb_set(NEW.jsondata, '{"properties","@ns:com:here:xyz"}', xyz, true);
        NEW.i = i;
        -- RAISE NOTICE '__naksha_trigger_fix_ns_xyz return %', NEW.jsondata;
        return NEW;
    END IF;

    -- DELETE
    -- RAISE NOTICE '__naksha_trigger_fix_ns_xyz % return %', TG_OP, OLD.jsondata;
    RETURN OLD;
END
$BODY$;

-- Trigger that writes into the transaction table.
CREATE OR REPLACE FUNCTION __naksha_trigger_write_tx()
    RETURNS trigger
    LANGUAGE 'plpgsql' STABLE
AS $BODY$
BEGIN
    PERFORM __naksha_tx_action_modify_features(TG_TABLE_NAME);
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        RETURN NEW;
    END IF;
    RETURN OLD;
END
$BODY$;

-- Trigger that writes into the history table.
CREATE OR REPLACE FUNCTION __naksha_trigger_write_hst()
    RETURNS trigger
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    stmt text;
    author text;
    app_id text;
    new_uuid uuid;
    txn uuid;
    ts_millis int8;
    rts_millis int8;
    i int8;
    xyz jsonb;
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        -- purge feature from deletion table and write history.
        stmt := format('INSERT INTO %I (jsondata,geo,i) VALUES($1,$2,$3);'
                    || 'DELETE FROM %I WHERE jsondata->>''id'' = $4;',
                        format('%s_hst', tg_table_name),
                        format('%s_del', tg_table_name)
        );
        --RAISE NOTICE '%', stmt;
        EXECUTE stmt USING NEW.jsondata, NEW.geo, NEW.i, NEW.jsondata->>'id';
        RETURN NEW;
    END IF;

    --RAISE NOTICE '__naksha_trigger_write_hst % %', TG_OP, OLD.jsondata;
    rts_millis := naksha_current_millis(clock_timestamp());
    ts_millis := naksha_current_millis(current_timestamp);
    txn := naksha_tx_current();
    i = nextval('"'||TG_TABLE_SCHEMA||'"."'||TG_TABLE_NAME||'_i_seq"');
    new_uuid := naksha_feature_uuid_from_object_id_and_ts(i, current_timestamp);
    author := naksha_tx_get_author(OLD.jsondata);
    app_id := naksha_tx_get_app_id();

    -- We do these updates, because in the "after-trigger" we only write into history.
    xyz := jsonb_build_object(
        'action', 'DELETE',
        'version', (OLD.jsondata->'properties'->'@ns:com:here:xyz'->>'version')::int8 + 1::int8,
        'collection', TG_TABLE_NAME,
        'author', author,
        'appId', app_id,
        'puuid', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'uuid',
        'uuid', new_uuid::text,
        'txn', txn::text,
        'createdAt', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'createdAt',
        'updatedAt', ts_millis,
        'rtcts', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'rtcts',
        'rtuts', rts_millis
    );
    IF OLD.jsondata->'properties'->'@ns:com:here:xyz'->'tags' IS NOT NULL THEN
        xyz := jsonb_set(xyz, '{"tags"}', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'tags', true);
    END IF;
    OLD.jsondata = jsonb_set(OLD.jsondata, '{"properties","@ns:com:here:xyz"}', xyz, true);
    OLD.i = i;

    -- write delete.
    stmt := format('INSERT INTO %I (jsondata,geo,i) VALUES($1,$2,$3);'
                || 'INSERT INTO %I (jsondata,geo,i) VALUES($1,$2,$3);',
                    format('%s_hst', tg_table_name),
                    format('%s_del', tg_table_name)
    );
    -- RAISE NOTICE '%', stmt;
    EXECUTE stmt USING OLD.jsondata, OLD.geo, OLD.i;

    -- Note: PostgresQL does not support returning modified old records.
    --       Therefore, the next trigger will see the unmodified OLD.jsondata again!
    RETURN OLD;
END
$BODY$;

-- Returns the details about the columns for internal purpose.
CREATE OR REPLACE FUNCTION __naksha_get_table_attributes(_table text)
    RETURNS jsonb
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    attr jsonb;
    attr_size int;
    i int;
    keys text array;
    values text array;
BEGIN
    with "attributes" as (select att.attname as "name", case att.attstorage
          when 'p' then 'plain'
          when 'm' then 'main'
          when 'e' then 'external'
          when 'x' then 'extended'
    end as "value"
    from pg_attribute att
    join pg_class tbl on tbl.oid = att.attrelid
    join pg_namespace ns on tbl.relnamespace = ns.oid
    where tbl.relname = _table and ns.nspname = current_schema() and not att.attisdropped)
    select array_agg("name"), array_agg("value") FROM "attributes" INTO keys, values;

    attr_size := array_length(keys, 1);
    attr := jsonb_build_object();
    i := 1;
    while i <= attr_size
    loop
        attr := jsonb_set(attr, ARRAY[keys[i]]::text[], to_jsonb(values[i]), true);
        i := i + 1;
    end loop;
    return attr;
END
$BODY$;

-- Internal helper to create the MAIN and DELETE tables with default indices.
-- This function does not create any triggers, only the table and the default indices!
CREATE OR REPLACE FUNCTION __naksha_optimize_head_table(_table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    sql text;
BEGIN
    sql := format('ALTER TABLE %I ALTER COLUMN "jsondata" SET STORAGE MAIN;'
               || 'ALTER TABLE %I ALTER COLUMN "jsondata" SET COMPRESSION lz4;'
               || 'ALTER TABLE %I ALTER COLUMN "geo" SET STORAGE MAIN;'
               || 'ALTER TABLE %I ALTER COLUMN "geo" SET COMPRESSION lz4;'
               || 'ALTER TABLE %I SET ('
               || 'toast_tuple_target=8160'
               || ',fillfactor=50'
               -- Specifies the minimum number of updated or deleted tuples needed to trigger a VACUUM in any one table.
               || ',autovacuum_vacuum_threshold=10000, toast.autovacuum_vacuum_threshold=10000'
               -- Specifies the number of inserted tuples needed to trigger a VACUUM in any one table.
               || ',autovacuum_vacuum_insert_threshold=10000, toast.autovacuum_vacuum_insert_threshold=10000'
               -- Specifies a fraction of the table size to add to autovacuum_vacuum_threshold when deciding whether to trigger a VACUUM.
               || ',autovacuum_vacuum_scale_factor=0.1, toast.autovacuum_vacuum_scale_factor=0.1'
               -- Specifies a fraction of the table size to add to autovacuum_analyze_threshold when deciding whether to trigger an ANALYZE.
               || ',autovacuum_analyze_threshold=10000, autovacuum_analyze_scale_factor=0.1'
               || ');',
                  _table, _table, _table, _table, _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;
END
$BODY$;

-- Internal helper to create the MAIN and DELETE tables with default indices.
-- This function does not create any triggers, only the table and the default indices!
CREATE OR REPLACE FUNCTION __naksha_optimize_hst_table(_table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    sql text;
BEGIN
    -- For history we disable auto-vacuum, because we only have inserts and then end writing into the table.
    -- We rather run this job by ourself ones for every "closed" table when doing maintenance.
    sql := format('ALTER TABLE %I ALTER COLUMN "jsondata" SET STORAGE MAIN;'
               || 'ALTER TABLE %I ALTER COLUMN "jsondata" SET COMPRESSION lz4;'
               || 'ALTER TABLE %I ALTER COLUMN "geo" SET STORAGE MAIN;'
               || 'ALTER TABLE %I ALTER COLUMN "geo" SET COMPRESSION lz4;'
               || 'ALTER TABLE %I SET ('
               || 'toast_tuple_target=8160'
               || ',fillfactor=100'
               || ',autovacuum_enabled=OFF, toast.autovacuum_enabled=OFF'
               || ');',
                  _table, _table, _table, _table, _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;
END
$BODY$;

-- Internal helper to create the MAIN and DELETE tables with default indices.
-- This function does not create any triggers, only the table and the default indices!
CREATE OR REPLACE FUNCTION __naksha_create_head_table(_table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    sql text;
BEGIN
    sql = format('CREATE TABLE IF NOT EXISTS %I ('
              || 'jsondata   JSONB '
              || ',geo       GEOMETRY(GeometryZ, 4326) '
              || ',i         int8 PRIMARY KEY NOT NULL '
              || ')',
              _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;
    PERFORM __naksha_optimize_head_table(_table);

    -- Primary index to avoid that two features with the same "id" are created.
    sql = format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I '
              || 'USING btree ((jsondata->>''id'')) '
              || 'WITH (fillfactor=50)',
                 format('%s_id_idx', _table), _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Index to search for one specific feature by its state UUID.
    sql = format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I '
              || 'USING btree ((jsondata->''properties''->''@ns:com:here:xyz''->>''uuid'')) '
              || 'WITH (fillfactor=50)',
                 format('%s_uuid_idx', _table), _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Index to search for features by geometry.
    sql = format('CREATE INDEX IF NOT EXISTS %I ON %I '
              || 'USING gist (geo) '
              || 'WITH (buffering=ON,fillfactor=50)',
                 format('%s_geo_idx', _table), _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Index to search for features by tags.
    sql = format('CREATE INDEX IF NOT EXISTS %I ON %I '
              || 'USING gin ((jsondata->''properties''->''@ns:com:here:xyz''->''tags'')) '
              || 'WITH (fastupdate=OFF)',
                 format('%s_tags_idx', _table), _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Index to search for features that have been part of a certain transaction, using "i" for paging.
    sql = format('CREATE INDEX IF NOT EXISTS %I ON %I '
              || 'USING btree ((jsondata->''properties''->''@ns:com:here:xyz''->>''txn'') ASC, i DESC) '
              || 'WITH (fillfactor=50)',
                 format('%s_txn_i_idx', _table), _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Index to search for features that have been created within a certain time window, using "i" for paging.
    sql = format('CREATE INDEX IF NOT EXISTS %I ON %I '
              || 'USING btree ((jsondata->''properties''->''@ns:com:here:xyz''->>''createdAt'') DESC, i DESC) '
              || 'WITH (fillfactor=50)',
                 format('%s_createdAt_i_idx', _table), _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Index to search for features that have been updated within a certain time window, using "i" for paging.
    sql = format('CREATE INDEX IF NOT EXISTS %I ON %I '
              || 'USING btree ((jsondata->''properties''->''@ns:com:here:xyz''->>''updatedAt'') DESC, i DESC) '
              || 'WITH (fillfactor=50)',
                 format('%s_updatedAt_i_idx', _table), _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Index to search what a user has updated (newest first), results order descending by update-time, id and version.
    sql = format('CREATE INDEX IF NOT EXISTS %I ON %I '
              || 'USING btree ('
              || '   (jsondata->''properties''->''@ns:com:here:xyz''->>''author'') DESC '
              || '  ,((jsondata->''properties''->''@ns:com:here:xyz''->>''updatedAt'')::int8) DESC '
              || '  ,(jsondata->>''id'') DESC '
              || '  ,((jsondata->''properties''->''@ns:com:here:xyz''->>''version'')::int8) DESC '
              || ') WITH (fillfactor=50)',
                 format('%s_author_idx', _table), _table);
    -- RAISE NOTICE '%', sql;
    EXECUTE sql;
END
$BODY$;

-- Ensure that the partition for the given day exists.
CREATE OR REPLACE FUNCTION __naksha_create_hst_partition_for_day(collection text, from_ts timestamptz)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    sql text;
    from_day text;
    from_part_id int;
    to_day text;
    to_ts timestamptz;
    to_part_id int;
    hst_name text;
    hst_part_name text;
BEGIN
    from_day := to_char(from_ts, 'YYYY_MM_DD');
    from_part_id := naksha_part_id_from_ts(from_ts);
    to_ts := from_ts + '1 day'::interval;
    to_day := to_char(to_ts, 'YYYY_MM_DD');
    to_part_id := naksha_part_id_from_ts(to_ts);
    hst_name := format('%s_hst', collection);
    hst_part_name := format('%s_hst_%s', collection, from_day); -- example: foo_hst_2023_03_01

    sql := format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L);',
                  hst_part_name, hst_name, from_part_id, to_part_id);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;
    PERFORM __naksha_optimize_hst_table(hst_part_name);

    -- Note: The history table is updated only for one day and then never touched again, therefore
    --       we want to fill the indices as much as we can, even while this may have a bad effect
    --       when doing bulk loads, because we may have more page splits.
    -- Therefore: Disable history for bulk loads by removing the triggers!

    sql := format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I '
               || 'USING btree ('
               || '  (jsondata->''properties''->''@ns:com:here:xyz''->>''uuid'') DESC '
               || ') WITH (fillfactor=100) ',
                  format('%s_uuid_idx', hst_part_name), hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I '
               || 'USING btree ('
               || '  (jsondata->''properties''->''@ns:com:here:xyz''->>''txn'') DESC '
               || '  ,(jsondata->''id'') DESC '
               || ') WITH (fillfactor=100) ',
                  format('%s_txn_id_idx', hst_part_name), hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I '
               || 'USING btree ('
               || '  (jsondata->''id'') DESC '
               || '  ,(jsondata->''properties''->''@ns:com:here:xyz''->>''txn'') DESC'
               || ') WITH (fillfactor=100) ',
                  format('%s_id_txn_idx', hst_part_name), hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I '
               || 'USING gist ('
               || '  geo '
               || '  ,(jsondata->>''id'')'
               || '  ,(jsondata->''properties''->''@ns:com:here:xyz''->>''txn'')'
               || ') WITH (buffering=ON,fillfactor=100) ',
                  format('%s_geo_id_txn_idx', hst_part_name), hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I '
               || 'USING btree ('
               || '  ((jsondata->''properties''->''@ns:com:here:xyz''->>''updatedAt'')::int8) DESC'
               || ') WITH (fillfactor=100) ',
                  format('%s_updatedAt_idx', hst_part_name), hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I '
               || 'USING btree ('
               || '  (jsondata->''properties''->''@ns:com:here:xyz''->>''lastUpdatedBy'') DESC'
               || '  ,(jsondata->''id'') DESC'
               || '  ,(jsondata->''properties''->''@ns:com:here:xyz''->>''txn'') DESC '
               || ') WITH (fillfactor=100) ',
                  format('%s_lastUpdatedBy_id_txn_idx', hst_part_name), hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;
END
$BODY$;

-- naksha_collection_maintain(name text)
-- naksha_collection_maintain_all()
-- naksha_collection_delete_at(name text, ts timestamptz)

-- Create the collection, if it does not exist, setting max age to 9223372036854775807 and enable history.
CREATE OR REPLACE FUNCTION naksha_collection_upsert(collection text)
    RETURNS jsonb
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
BEGIN
    RETURN naksha_collection_upsert(collection, 9223372036854775807::int8, true);
END;
$BODY$;

-- Create or update the collection, setting the maximum age of historic states and enable/disable history.
CREATE OR REPLACE FUNCTION naksha_collection_upsert(collection text, max_age int8, enable_history bool)
    RETURNS jsonb
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    full_name text;
    trigger_name text;
    sql text;
    ts timestamptz;
    comment text;
    h text;
BEGIN
    -- Create HEAD table
    PERFORM __naksha_create_head_table(collection);
    -- Create object_id sequence (i)
    sql := format('CREATE SEQUENCE IF NOT EXISTS %I AS int8 CACHE 1000 OWNED BY %I.i', format('%s_i_seq', collection), collection);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Create the deletion table, to keep track of deleted objects.
    PERFORM __naksha_create_head_table(format('%s_del', collection));

    -- Create the history table
    sql = format('CREATE TABLE IF NOT EXISTS %I '
              || '(jsondata jsonb, geo geometry(GeometryZ, 4326), i int8 NOT NULL) '
              || 'PARTITION BY RANGE (naksha_feature_get_part_id(jsondata))', format('%s_hst', collection));
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Add the comment to the HEAD table.
    IF enable_history THEN
        h = 'true';
    ELSE
        h = 'false';
    END IF;
    comment = format('{"id":%s, "number":%s, "maxAge":%s, "history":%s}',
        (to_json(collection))::text, naksha_storage_id(), max_age, h
    );
    sql = format('COMMENT ON TABLE %I IS %L;', collection, comment);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Create the first three days of history
    ts = current_timestamp;
    PERFORM __naksha_create_hst_partition_for_day(collection, ts);
    PERFORM __naksha_create_hst_partition_for_day(collection, ts + '1 day'::interval);
    PERFORM __naksha_create_hst_partition_for_day(collection, ts + '2 day'::interval);

    -- Update the XYZ namespace in jsondata.
    trigger_name := format('%s_fix_ns_xyz', collection);
    full_name := format('%I', collection);
    IF NOT EXISTS(SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = full_name::regclass and tgname = trigger_name) THEN
        sql := format('CREATE TRIGGER %I '
                   || 'BEFORE INSERT OR UPDATE ON %I '
                   || 'FOR EACH ROW EXECUTE FUNCTION __naksha_trigger_fix_ns_xyz();',
                      trigger_name, collection);
        --RAISE NOTICE '%', sql;
        EXECUTE sql;
    END IF;

    -- Update the transaction table.
    trigger_name := format('%s_write_tx', collection);
    full_name := format('%I', collection);
    IF NOT EXISTS(SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = full_name::regclass and tgname = trigger_name) THEN
        sql := format('CREATE TRIGGER %I '
                   || 'AFTER INSERT OR UPDATE OR DELETE ON %I '
                   || 'FOR EACH ROW EXECUTE FUNCTION __naksha_trigger_write_tx();',
                      trigger_name, collection);
        --RAISE NOTICE '%', sql;
        EXECUTE sql;
    END IF;

    PERFORM __naksha_tx_action_upsert_collection(collection);

    IF enable_history THEN
        PERFORM naksha_collection_enable_history(collection);
    END IF;

    RETURN comment::jsonb;
END
$BODY$;

-- Returns the JSON feature of the collection or NULL, if no such collection exits.
CREATE OR REPLACE FUNCTION naksha_collection_get(collection text)
    RETURNS jsonb
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
DECLARE
    j jsonb;
    ns_oid oid;
    table_oid oid;
    tupple int8;
BEGIN
    SELECT oid FROM pg_namespace WHERE nspname = naksha_schema() INTO ns_oid;
    SELECT oid, reltuples::int8 FROM pg_class where relname = collection AND relnamespace = ns_oid INTO table_oid, tupple;
    j = obj_description(table_oid, 'pg_class')::jsonb;
    IF j IS NOT NULL AND j->>'id' = collection THEN
      j := jsonb_set(j, '{"estimatedFeatureCount"}', to_jsonb(tupple), true);
      RETURN j;
    END IF;
    RETURN NULL;
EXCEPTION
  WHEN OTHERS THEN RETURN NULL;
END
$BODY$;

-- Returns the information about the collection.
CREATE OR REPLACE FUNCTION naksha_collection_get_all()
    RETURNS TABLE (id text, jsondata jsonb)
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
DECLARE
    ns_oid oid;
BEGIN
    SELECT oid FROM pg_namespace WHERE nspname = naksha_schema() INTO ns_oid;
    --RAISE INFO '"%"', ns_oid;
    RETURN QUERY
	WITH tabinfo AS (SELECT oid, relname::text, reltuples::int8 FROM pg_class WHERE relkind='r' AND relnamespace = ns_oid)
    SELECT relname as "id", jsonb_set(obj_description(oid)::jsonb, '{"estimatedFeatureCount"}', to_jsonb(reltuples), true) AS "jsondata" FROM tabinfo
    WHERE naksha_json_id(obj_description(oid)) = relname;
END
$BODY$;

-- Enable the history by adding the triggers to the main table.
CREATE OR REPLACE FUNCTION naksha_collection_enable_history(collection text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    meta jsonb;
    trigger_name text;
    full_name text;
    sql text;
BEGIN
    meta = obj_description(collection::regclass, 'pg_class')::jsonb;
    IF NOT (meta->'history')::boolean THEN
        meta = jsonb_set(meta, '{"history"}', to_jsonb(true), true);
        sql = format('COMMENT ON TABLE %I IS %L;', collection, meta::text);
        --RAISE NOTICE '%', sql;
        EXECUTE sql;
    END IF;
    trigger_name := format('%s_write_hst', collection);
    full_name := format('%I', collection);
    IF NOT EXISTS(SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = full_name::regclass and tgname = trigger_name) THEN
        sql := format('CREATE TRIGGER %I '
                   || 'AFTER INSERT OR UPDATE OR DELETE ON %I '
                   || 'FOR EACH ROW EXECUTE FUNCTION __naksha_trigger_write_hst();',
                      trigger_name, collection);
        EXECUTE sql;
        PERFORM __naksha_tx_action_enable_history(collection);
    END IF;
END
$BODY$;

-- Disable the history by dropping the triggers from the main table.
CREATE OR REPLACE FUNCTION naksha_collection_disable_history(collection text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    sql text;
    meta jsonb;
    trigger_name text;
BEGIN
    meta = obj_description(collection::regclass, 'pg_class')::jsonb;
    IF (meta->'history')::boolean THEN
        meta = jsonb_set(meta, '{"history"}', to_jsonb(false), true);
        sql = format('COMMENT ON TABLE %I IS %L;', collection, meta::text);
        --RAISE NOTICE '%', sql;
        EXECUTE sql;
    END IF;
    trigger_name := format('%s_write_hst', collection);
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', trigger_name, collection);
    PERFORM __naksha_tx_action_disable_history(collection);
END
$BODY$;

-- Drop the collection instantly, this operation is not revertible.
CREATE OR REPLACE FUNCTION naksha_collection_drop(collection text)
    RETURNS jsonb
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    meta jsonb;
    sql text;
BEGIN
    meta = obj_description(collection::regclass, 'pg_class')::jsonb;
    PERFORM naksha_collection_disable_history(collection);
    sql = format('DROP TABLE IF EXISTS %I CASCADE;', collection||'_hst');
    EXECUTE sql;
    sql = format('DROP TABLE IF EXISTS %I CASCADE;', collection||'_del');
    EXECUTE sql;
    sql = format('DROP TABLE IF EXISTS %I CASCADE;', collection);
    EXECUTE sql;
    PERFORM __naksha_tx_action_purge_collection(collection);
    RETURN meta;
END;
$BODY$;

DROP FUNCTION IF EXISTS naksha_modify_features;
DROP TYPE IF EXISTS naksha_op;

CREATE TYPE naksha_op AS ENUM (
    'INSERT', -- returns the new state
    'UPDATE', -- returns the new state
    'UPSERT', -- returns the new state
    'DELETE' -- returns the deleted feature
);

-- Bulk insert, update, upsert, delete of features.
CREATE OR REPLACE FUNCTION naksha_modify_features(
    collection text, -- the collection
    feature_arr jsonb array, -- the new feature
    geometry_arr bytea array, -- if the geometry should be updated
    expected_uuid_arr text array, -- If atomic updates used, the expected state identifier
    op_arr naksha_op array, -- the operation to perform
    return_results bool
)
    RETURNS TABLE (o naksha_op, f jsonb, g bytea)
    LANGUAGE 'plpgsql' VOLATILE
AS
$BODY$
DECLARE
    col                 jsonb;
    arr_size            int;
    id_arr              text array;
    id_idx_arr          int array;
    existing_id_arr     text array;
    existing_uuid_arr   text array;
    e_uuid_i            int;
    e_uuid_len          int;
    i                   int;
    index               int;
    id                  text;
    feature             jsonb;
    geo                 bytea;
    expected_uuid       text;
    existing_uuid       text;
    op                  naksha_op;
    stmt                text;
    insert_stmt         text;
    update_stmt         text;
    delete_stmt         text;
BEGIN
    --RAISE NOTICE '------------------ START ----------------------------';
    -- See: https://www.postgresql.org/docs/current/errcodes-appendix.html
    -- 22023 invalid_parameter_value
    col := naksha_collection_get(collection);
    IF col IS NULL THEN
        RAISE SQLSTATE '22023' USING MESSAGE = format('Unknown collection %I', collection);
    END IF;
    IF feature_arr IS NULL THEN
        RAISE SQLSTATE '22023' USING MESSAGE = '"feature_arr" must not be null';
    END IF;
    arr_size := array_length(feature_arr, 1);
    IF geometry_arr IS NULL OR array_length(geometry_arr, 1) != arr_size THEN
        RAISE SQLSTATE '22023' USING MESSAGE = '"geometry_arr" must not be null and have same size as all other arrays';
    END IF;
    IF expected_uuid_arr IS NULL OR array_length(expected_uuid_arr, 1) != arr_size THEN
        RAISE SQLSTATE '22023' USING MESSAGE = '"expected_uuid_arr" must not be null and have same size as all other arrays';
    END IF;
    IF op_arr IS NULL OR array_length(op_arr, 1) != arr_size THEN
        RAISE SQLSTATE '22023' USING MESSAGE = '"op_arr" must not be null and have same size as all other arrays';
    END IF;

    -- TODO: Ensure that input does not contain the same feature twice!
    --       In other words, every feature-id must only be given ones!

    --RAISE NOTICE 'Start';
    id_arr := ARRAY[]::text[];
    i := 1;
    WHILE i <= arr_size
    LOOP
        feature := feature_arr[i];
        IF feature IS NULL THEN
            RAISE SQLSTATE '22023' USING MESSAGE = format('"feature_arr[%s]" must not be null', i);
        END IF;
        op := op_arr[i];
        IF op IS NULL THEN
            RAISE SQLSTATE '22023' USING MESSAGE = format('"op_arr[%s]" must not be null', i);
        END IF;
        id := feature->>'id';
        IF id IS NULL THEN
            id := md5(random()::text);
            --RAISE NOTICE 'Generate id: %', id;
            feature_arr[i] = jsonb_set(feature, '{"id"}', to_jsonb(id), true);
        END IF;
        id_arr := array_append(id_arr, id);
        i := i + 1;
    END LOOP;

    -- Order ids and attach ordinal (index), then select back into arrays.
    WITH ordered_ids AS (SELECT "unnest", "ordinality" FROM unnest(id_arr) WITH ORDINALITY ORDER BY "unnest")
    SELECT ARRAY(SELECT "unnest" FROM ordered_ids), ARRAY(SELECT "ordinality" FROM ordered_ids)
    INTO id_arr, id_idx_arr;
    --RAISE NOTICE 'Ordered ids: % %', id_arr, id_idx_arr;

    -- Read ids and their uuid, lock rows for update.
    stmt := format('WITH id_and_uuid AS ('
        || 'SELECT jsondata->>''id'' as "id", jsondata->''properties''->''@ns:com:here:xyz''->>''uuid'' as "uuid" '
        || 'FROM %I WHERE jsondata->>''id'' = ANY($1) '
        || 'ORDER BY jsondata->>''id'' FOR UPDATE '
        || ') SELECT ARRAY(SELECT id FROM id_and_uuid), ARRAY(SELECT uuid FROM id_and_uuid) FROM id_and_uuid '
        || 'LIMIT 1', collection);
    --RAISE NOTICE 'Select ids and uuids: %', stmt;
    EXECUTE stmt USING id_arr INTO existing_id_arr, existing_uuid_arr;
    --RAISE NOTICE 'ids, uuids: % %', existing_id_arr, existing_uuid_arr;

    --RAISE NOTICE 'Perform all actions';
    insert_stmt := format('INSERT INTO %I (jsondata, geo) VALUES ($1, ST_Force3D(ST_GeomFromWKB($2,4326))) RETURNING jsondata;', collection);
    update_stmt := format('UPDATE %I SET jsondata=$1, geo=ST_Force3D(ST_GeomFromWKB($2,4326)) WHERE jsondata->>''id''=$3 RETURNING jsondata;', collection);
    delete_stmt := format('DELETE FROM %I WHERE jsondata->>''id''=$1 RETURNING jsondata, ST_AsEWKB(geo);', collection);
    i := 1;
    e_uuid_i := 1;
    e_uuid_len := array_length(existing_uuid_arr, 1);
    WHILE i <= arr_size
    LOOP
        id := id_arr[i];
        IF e_uuid_i <= e_uuid_len AND id = existing_id_arr[e_uuid_i] THEN
            existing_uuid := existing_uuid_arr[e_uuid_i];
            e_uuid_i = e_uuid_i + 1;
        ELSE
            existing_uuid := NULL;
        END IF;
        index := id_idx_arr[i];
        feature := feature_arr[index];
        geo := geometry_arr[index];
        expected_uuid := expected_uuid_arr[index];
        op := op_arr[index];
        --RAISE NOTICE 'Op ''%'' for ''%'' (uuid: ''%'', expected: ''%'')', op, id, existing_uuid, expected_uuid;

        -- TODO HP_QUERY : Is it better to return '23505' (unique_violation) for ID / UUID mismatches?
        -- TODO HP_QUERY : And '02000' for no_data found situation
        IF op = 'INSERT' THEN
            IF existing_uuid IS NOT NULL THEN
                RAISE SQLSTATE '22023' USING MESSAGE = format('The feature %L exists already', id);
            END IF;
            EXECUTE insert_stmt USING feature, geo INTO feature;
            feature_arr[index] = feature;
        ELSEIF op = 'UPDATE' THEN
            IF expected_uuid IS NOT NULL THEN
                IF expected_uuid != existing_uuid THEN
                    RAISE SQLSTATE '22023' USING
                    MESSAGE = format('The feature %L is not in expected state %L, found: %L', id, expected_uuid, existing_uuid);
                END IF;
            END IF;

            IF existing_uuid IS NULL THEN
                RAISE SQLSTATE '22023' USING
                MESSAGE = format('The feature %L does not exist', id);
            END IF;
            EXECUTE update_stmt USING feature, geo, id INTO feature;
            feature_arr[index] = feature;
        ELSEIF op = 'UPSERT' THEN
            IF expected_uuid IS NOT NULL THEN
                IF expected_uuid != existing_uuid THEN
                    RAISE SQLSTATE '22023' USING
                    MESSAGE = format('The feature %L is not in expected state %L, found: %L', id, expected_uuid, existing_uuid);
                END IF;
            END IF;

            IF existing_uuid IS NOT NULL THEN
                EXECUTE update_stmt USING feature, geo, id INTO feature;
                feature_arr[index] = feature;
                op[index] = 'UPDATE'::naksha_op;
            ELSE
                EXECUTE insert_stmt USING feature, geo INTO feature;
                feature_arr[index] = feature;
                op[index] = 'INSERT'::naksha_op;
            END IF;
        ELSEIF op = 'DELETE' THEN
            IF expected_uuid IS NOT NULL THEN
                IF expected_uuid != existing_uuid THEN
                    RAISE SQLSTATE '22023' USING
                    MESSAGE = format('The feature %L is not in expected state %L, found: %L', id, expected_uuid, existing_uuid);
                END IF;
            END IF;

            IF existing_uuid IS NOT NULL THEN
                EXECUTE delete_stmt USING id INTO feature, geo;
                feature_arr[index] = feature;
                geometry_arr[index] = geo;
            END IF;
        END IF;

        i = i + 1;
    END LOOP;
    IF NOT return_results THEN
        RETURN;
    END IF;
    RETURN QUERY SELECT unnest(op_arr) AS "o", unnest(feature_arr) AS "f", unnest(geometry_arr) AS "g";
END
$BODY$;

-- Start the transaction by setting the application-identifier, the current author (which may be null)
-- and the returns the transaction number.
-- See: https://www.postgresql.org/docs/current/runtime-config-query.html
CREATE OR REPLACE FUNCTION naksha_tx_start(app_id text, author text, create_tx bool) RETURNS uuid LANGUAGE 'plpgsql' VOLATILE AS $$
BEGIN
    EXECUTE format('SELECT '
                       || 'SET_CONFIG(''plan_cache_mode'', ''force_generic_plan'', true)'
                       || ',SET_CONFIG(''cursor_tuple_fraction'', ''1.0'', true)'
                       || ',SET_CONFIG(''geqo'', ''false'', true)'
                       || ',SET_CONFIG(''work_mem'', ''128 MB'', true)'
                       || ',SET_CONFIG(''maintenance_work_mem'', ''1024 MB'', true)'
                       || ',SET_CONFIG(''enable_seqscan'', ''OFF'', true)'
                       || ',SET_CONFIG(''enable_bitmapscan'', ''OFF'', true)'
                       || ',SET_CONFIG(''enable_sort'', ''OFF'', true)'
                       || ',SET_CONFIG(''enable_partitionwise_join'', ''ON'', true)'
                       || ',SET_CONFIG(''enable_partitionwise_aggregate'', ''ON'', true)'
                       || ',SET_CONFIG(''jit'', ''OFF'', true)'
                       || ',SET_CONFIG(''naksha.app_id'', %L::text, true)' -- same as naksha_tx_set_app_id
                       || ',SET_CONFIG(''naksha.author'', %L::text, true)' -- same as naksha_tx_set_author
        , app_id, author);
    IF create_tx THEN
        RETURN naksha_tx_current();
    END IF;
    RETURN NULL;
END
$$;

-- Return the application-id to be added into the XYZ namespace (transaction local variable).
-- Returns NULL if no app_id is set.
CREATE OR REPLACE FUNCTION naksha_tx_get_app_id() RETURNS text LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
    value  text;
BEGIN
    value := coalesce(current_setting('naksha.app_id', true), '');
    IF value = '' THEN
        RETURN NULL;
    END IF;
    RETURN value;
END
$$;

-- Set the application-id to be added into the XYZ namespace, can be set to null.
-- Returns the previously set value that was replaced or NULL, if no value was set.
CREATE OR REPLACE FUNCTION naksha_tx_set_app_id(app_id text) RETURNS text LANGUAGE 'plpgsql' STABLE AS $$
DECLARE
    old text;
    sql text;
BEGIN
    old := coalesce(current_setting('naksha.app_id', true), '');
    app_id := coalesce(app_id, '');
    sql := format('SELECT SET_CONFIG(%L, %L::text, true)', 'naksha.app_id', app_id);
    EXECUTE sql;
    IF old = '' THEN
        RETURN NULL;
    END IF;
    RETURN old;
END
$$;

-- Return the author to be added into the XYZ namespace (transaction local variable).
-- Returns NULL if no author is set.
CREATE OR REPLACE FUNCTION naksha_tx_get_author()
    RETURNS text
    LANGUAGE 'plpgsql' STABLE
AS $$
DECLARE
    value  text;
BEGIN
    value := coalesce(current_setting('naksha.author', true), '');
    IF value = '' THEN
        RETURN NULL;
    END IF;
    RETURN value;
END
$$;

-- Return the author to be added into the XYZ namespace (transaction local variable).
-- This version only returns NULL, if neither the author is set, nor an old author is in the given
-- jsonb nor an application identifier is set (which must never happen).
CREATE OR REPLACE FUNCTION naksha_tx_get_author(old jsonb)
    RETURNS text
    LANGUAGE 'plpgsql' STABLE
AS $$
DECLARE
    value  text;
BEGIN
    value := coalesce(current_setting('naksha.author', true), '');
    IF value = '' THEN
        IF old IS NOT NULL THEN
            value := old->'properties'->'@ns:com:here:xyz'->>'author';
        END IF;
        IF value IS NULL THEN
            value := naksha_tx_get_app_id();
        END IF;
        RETURN value;
    END IF;
    RETURN value;
END
$$;


-- Set the author to be added into the XYZ namespace, can be set to null.
-- Returns the previously set value that was replaced or NULL, if no value was set.
CREATE OR REPLACE FUNCTION naksha_tx_set_author(author text)
    RETURNS text
    LANGUAGE 'plpgsql' STABLE
AS $$
DECLARE
    old text;
    sql text;
BEGIN
    old := coalesce(current_setting('naksha.author', true), '');
    author := coalesce(author, '');
    sql := format('SELECT SET_CONFIG(%L, %L::text, true)', 'naksha.author', author, true);
    EXECUTE sql;
    IF old = '' THEN
        RETURN NULL;
    END IF;
    RETURN old;
END
$$;

-- Return the unique transaction number for the current transaction. If no transaction number is
-- yet acquired, it acquire a new one.
CREATE OR REPLACE FUNCTION naksha_tx_current()
    RETURNS uuid
    LANGUAGE 'plpgsql' STABLE
AS $$
DECLARE
    value  text;
    txi    int8;
    txn    uuid;
BEGIN
    value := current_setting('naksha.txn', true);
    IF coalesce(value, '') <> '' THEN
        -- RAISE NOTICE 'found value = %', value;
        return value::uuid;
    END IF;

    txi := nextval('naksha_tx_id_seq');
    txn := naksha_txn_from_object_id_and_ts(txi, current_timestamp);
    PERFORM SET_CONFIG('naksha.txn', txn::text, true);
    RETURN txn;
END
$$;

CREATE OR REPLACE FUNCTION __naksha_tx_action_modify_features(collection text) RETURNS void
LANGUAGE 'plpgsql' STABLE AS $BODY$ BEGIN
    IF NOT naksha_tx_action_cached('TxModifyFeatures', collection) THEN
        PERFORM naksha_tx_set_action('TxModifyFeatures', collection);
    END IF;
END $BODY$;

CREATE OR REPLACE FUNCTION __naksha_tx_action_upsert_collection(collection text) RETURNS void
LANGUAGE 'plpgsql' STABLE AS $BODY$ BEGIN
    IF NOT naksha_tx_action_cached('TxUpsertCollection', collection) THEN
        PERFORM naksha_tx_set_action('TxUpsertCollection', collection);
    END IF;
END $BODY$;

CREATE OR REPLACE FUNCTION __naksha_tx_action_delete_collection(collection text) RETURNS void
LANGUAGE 'plpgsql' STABLE AS $BODY$ BEGIN
    IF NOT naksha_tx_action_cached('TxDeleteCollection', collection) THEN
        PERFORM naksha_tx_set_action('TxDeleteCollection', collection);
    END IF;
END $BODY$;

CREATE OR REPLACE FUNCTION __naksha_tx_action_purge_collection(collection text) RETURNS void
LANGUAGE 'plpgsql' STABLE AS $BODY$ BEGIN
    IF NOT naksha_tx_action_cached('TxPurgeCollection', collection) THEN
        PERFORM naksha_tx_set_action('TxPurgeCollection', collection);
    END IF;
END $BODY$;

CREATE OR REPLACE FUNCTION __naksha_tx_action_enable_history(collection text) RETURNS void
LANGUAGE 'plpgsql' STABLE AS $BODY$ BEGIN
    IF NOT naksha_tx_action_cached('TxEnableHistory', collection) THEN
        PERFORM naksha_tx_set_action('TxEnableHistory', collection);
    END IF;
END $BODY$;

CREATE OR REPLACE FUNCTION __naksha_tx_action_disable_history(collection text) RETURNS void
LANGUAGE 'plpgsql' STABLE AS $BODY$ BEGIN
    IF NOT naksha_tx_action_cached('TxDisableHistory', collection) THEN
        PERFORM naksha_tx_set_action('TxDisableHistory', collection);
    END IF;
END $BODY$;

CREATE OR REPLACE FUNCTION naksha_tx_action_cached(action text, collection text) RETURNS bool
LANGUAGE 'plpgsql' STABLE AS $BODY$ BEGIN
    RETURN coalesce(current_setting(format('naksha.tx_set_%s_%s',action,collection), true),'') <> '';
END $BODY$;

-- Set a collection action for the current transaction.
CREATE OR REPLACE FUNCTION naksha_tx_set_action(action text, collection text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    id          text;
    part_id     int;
    txn         uuid;
    app_id      text;
    author      text;
BEGIN
    id := format('naksha.tx_set_%s_%s',action,md5(collection));
    IF coalesce(current_setting(id, true), '') = '' THEN
        part_id = naksha_part_id_from_ts(current_timestamp);
        txn = naksha_tx_current();
        app_id = naksha_tx_get_app_id();
        author = naksha_tx_get_author();
        INSERT INTO naksha_tx ("part_id", "txn", "action", "id", "app_id", "author", "ts", "psql_id")
          VALUES (part_id, txn, action, collection, app_id, author, current_timestamp, txid_current())
          ON CONFLICT DO NOTHING;
        PERFORM SET_CONFIG(id, 'true', true);
    END IF;
END
$BODY$;

-- Set a transaction message.
CREATE OR REPLACE FUNCTION naksha_tx_set_msg(id text, msg text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
BEGIN
    PERFORM naksha_tx_set_msg(id, msg, null, null);
END
$BODY$;

-- Set a transaction message, optionally with a json and attachment.
CREATE OR REPLACE FUNCTION naksha_tx_set_msg(msg_id text, msg text, json jsonb, attachment bytea)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    _ACTION constant text := 'TxMessage';
    _part_id     int;
    _txn         uuid;
    _app_id      text;
    _author      text;
BEGIN
    _part_id = naksha_part_id_from_ts(current_timestamp);
    _txn = naksha_tx_current();
    _app_id = naksha_tx_get_app_id();
    _author = naksha_tx_get_author();
    INSERT INTO naksha_tx ("part_id", "txn", "action", "id", "app_id", "author", "ts", "psql_id", "msg_text", "msg_json", "msg_attachment")
      VALUES (_part_id, _txn, _ACTION, msg_id, _app_id, _author, current_timestamp, txid_current(), msg, json, attachment)
      ON CONFLICT ("part_id","txn","action","id") DO UPDATE SET "msg_text" = msg, "msg_json" = json, "msg_attachment" = attachment;
END
$BODY$;

-- Ensure that the partition for the given day exists.
CREATE OR REPLACE FUNCTION __naksha_create_tx_partition_for_day(from_ts timestamptz)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    tx_table        text;
    sql             text;
    from_day        text;
    --to_day          text;
    to_ts           timestamptz;
    from_date_id    int;
    to_date_id      int;
    hst_part_name   text;
BEGIN
    tx_table := 'naksha_tx';
    from_day := to_char(from_ts, 'YYYY_MM_DD');
    from_date_id := naksha_part_id_from_ts(from_ts);
    to_ts := from_ts + '1 day'::interval;
    --to_day := to_char(to_ts, 'YYYY_MM_DD');
    to_date_id := naksha_part_id_from_ts(to_ts);
    hst_part_name := format('%s_%s', tx_table, from_day); -- example: naksha_tx_2023_03_01

    sql := format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L);',
                   hst_part_name, tx_table, from_date_id, to_date_id);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('ALTER TABLE %I ALTER COLUMN "msg_json" SET STORAGE MAIN;'
               || 'ALTER TABLE %I ALTER COLUMN "msg_json" SET COMPRESSION lz4;'
               || 'ALTER TABLE %I ALTER COLUMN "msg_attachment" SET STORAGE EXTERNAL;'
               || 'ALTER TABLE %I SET ('
               || 'toast_tuple_target=8160'
               || ',fillfactor=100'
               -- Specifies the minimum number of updated or deleted tuples needed to trigger a VACUUM in any one table.
               || ',autovacuum_vacuum_threshold=10000, toast.autovacuum_vacuum_threshold=10000'
               -- Specifies the number of inserted tuples needed to trigger a VACUUM in any one table.
               || ',autovacuum_vacuum_insert_threshold=10000, toast.autovacuum_vacuum_insert_threshold=10000'
               -- Specifies a fraction of the table size to add to autovacuum_vacuum_threshold when deciding whether to trigger a VACUUM.
               || ',autovacuum_vacuum_scale_factor=0.1, toast.autovacuum_vacuum_scale_factor=0.1'
               -- Specifies a fraction of the table size to add to autovacuum_analyze_threshold when deciding whether to trigger an ANALYZE.
               || ',autovacuum_analyze_threshold=10000, autovacuum_analyze_scale_factor=0.1'
               || ');',
                  hst_part_name, hst_part_name, hst_part_name, hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;
END
$BODY$;

-- Note: We do not partition the transaction table, because we need a primary index about the
--       transaction identifier and we can't partition by it, so partitioning would be impractical
--       even while partitioning by month could make sense.
CREATE OR REPLACE FUNCTION __naksha_create_tx_table()
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
BEGIN
    CREATE TABLE IF NOT EXISTS naksha_tx (
        "part_id"            int NOT NULL,
        "txn"                uuid NOT NULL,
        -- TxModifyFeatures -> id is collection name
        -- TxUpsertCollection -> id is collection name
        -- TxDeleteCollection -> id is collection name
        -- TxEnableHistory -> id is collection name
        -- TxDisableHistory -> id is collection name
        -- TxMessage -> id is message identifier
        "action"             text COLLATE "C" NOT NULL,
        "id"                 text COLLATE "C" NOT NULL,
        "app_id"             text COLLATE "C" NOT NULL,
        "author"             text COLLATE "C",
        "ts"                 timestamptz NOT NULL,
        "psql_id"            int8 NOT NULL,
        "msg_text"           text COLLATE "C",
        "msg_json"           jsonb,
        "msg_attachment"     bytea,
        "publish_id"         int8,
        "publish_ts"         timestamptz
    ) PARTITION BY RANGE (part_id);

    CREATE SEQUENCE IF NOT EXISTS naksha_tx_object_id_seq AS int8;

    -- PRIMARY UNIQUE INDEX to search for transactions by transaction number.
    CREATE UNIQUE INDEX IF NOT EXISTS naksha_tx_primary_idx
    ON naksha_tx USING btree ("part_id" ASC, "txn" ASC, "action" ASC, "id" ASC);

    -- INDEX to search for transactions by time.
    CREATE INDEX IF NOT EXISTS naksha_tx_ts_idx
    ON naksha_tx USING btree ("ts" ASC);

    -- INDEX to search for transactions by application and time.
    CREATE INDEX IF NOT EXISTS naksha_tx_app_id_ts_idx
    ON naksha_tx USING btree ("app_id" ASC, "ts" ASC);

    -- INDEX to search for transactions by author and time.
    CREATE INDEX IF NOT EXISTS naksha_tx_author_ts_idx
    ON naksha_tx USING btree ("author" ASC, "ts" ASC);

    -- INDEX to search for transactions by publication id.
    CREATE INDEX IF NOT EXISTS naksha_tx_publish_id_idx
    ON naksha_tx USING btree ("publish_id" ASC);

    -- INDEX to search for transactions by publication time.
    CREATE INDEX IF NOT EXISTS naksha_tx_publish_ts_idx
    ON naksha_tx USING btree ("publish_ts" ASC);

    PERFORM __naksha_create_tx_partition_for_day(current_timestamp);
    PERFORM __naksha_create_tx_partition_for_day(current_timestamp + '1 day'::interval);
    PERFORM __naksha_create_tx_partition_for_day(current_timestamp + '2 day'::interval);
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_init()
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    orig_search_path TEXT;
BEGIN
    EXECUTE 'SHOW search_path' INTO orig_search_path;
    CREATE SCHEMA IF NOT EXISTS public;
    CREATE SCHEMA IF NOT EXISTS topology;

    -- Install extensions
    CREATE EXTENSION IF NOT EXISTS btree_gist SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS btree_gin SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS postgis_topology SCHEMA topology;
    -- revert search_path to original value (as postgis_topology installation modifies it)
    EXECUTE format('SET SESSION search_path TO %s', orig_search_path);
    --CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public;
    --CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS hstore SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS pg_hint_plan SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public;

    IF __naksha_pg_version() < 14 THEN
        -- feature_not_supported
        RAISE SQLSTATE '0A000' USING MESSAGE = format('Naksha requires PostgresQL version 14+, found %L', __naksha_pg_version());
    END IF;
    PERFORM __naksha_create_tx_table();
END
$BODY$;

-- Drop the partition table for the given date.
CREATE OR REPLACE FUNCTION __naksha_delete_hst_partition_for_day(collection text, from_ts timestamptz)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    sql text;
    from_day text;
    hst_part_name text;
BEGIN
    from_day := to_char(from_ts, 'YYYY_MM_DD');
    hst_part_name := format('%s_hst_%s', collection, from_day); -- example: foo_hst_2023_03_01

    sql := format('DROP TABLE IF EXISTS %I;', hst_part_name);
    EXECUTE sql;
END
$BODY$;

-- Drop the partition table for the given date.
CREATE OR REPLACE FUNCTION __naksha_delete_tx_partition_for_day(from_ts timestamptz)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
sql text;
    from_day text;
    tx_part_name text;
BEGIN
    from_day := to_char(from_ts, 'YYYY_MM_DD');
    tx_part_name := format('naksha_tx_%s', from_day); -- example: naksha_tx_2023_03_01

sql := format('DROP TABLE IF EXISTS %I;', tx_part_name);
EXECUTE sql;
END
$BODY$;
