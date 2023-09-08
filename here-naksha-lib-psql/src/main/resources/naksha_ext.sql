---------------------------------------------------------------------------------------------------
-- NEW Naksha extensions
---------------------------------------------------------------------------------------------------
--
-- TODO: Disable IntelliJ Auto-Formatter, it sucks unbelievable:
--       Open File->Settings->Editor->Code Style->SQL->General and check "Disable formatting"
--
-- Links about error handling:
-- https://www.postgresql.org/docs/current/plpgsql-errors-and-messages.html
-- https://www.postgresql.org/docs/current/errcodes-appendix.html
-- https://www.postgresql.org/docs/9.6/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
--
-- Links about type/string handling:
-- https://www.postgresql.org/docs/current/functions-formatting.html
--
-- Concurrency information:
-- https://www.postgresql.org/docs/current/explicit-locking.html
--
-- Other useful information about PostgesQL
-- https://www.postgresql.org/docs/current/catalog-pg-class.html
-- https://www.postgresql.org/docs/current/catalog-pg-trigger.html
--
-- NOTE: Debugging in DBeaver can be done by adding notices:
--     RAISE NOTICE 'Hello';
-- and then switch to Output tab (Ctrl+Shift+O)
--
-- The effects of SET LOCAL last only till the end of the current transaction.
--
-- To create a test collection:
-- SELECT naksha_tx_start('foo', NULL);
-- SELECT naksha_collection_upsert('foo');
--
-- To insert random data into a column do this:
-- SELECT naksha_tx_start('foo', NULL);
-- EXPLAIN (ANALYZE, BUFFERS)
-- WITH rnd AS (select md5(random()::text) as id, ST_Force3D(ST_GeneratePoints('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 10)) as g from generate_Series(1,5000))
-- INSERT INTO foo (jsondata,geo) SELECT ('{"id":"'||id||'"}')::jsonb as jsondata, g FROM rnd;
--
CREATE SCHEMA IF NOT EXISTS "${schema}";
SET SESSION search_path TO "${schema}", public, topology;

-- Returns the packed Naksha extension version: 16 bit reserved, 16 bit major, 16 bit minor, 16 bit revision.
CREATE OR REPLACE FUNCTION naksha_version() RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    --        major               minor               revision
    return (  2::int8 << 32) | (  0::int8 << 16) | (  5::int8);
END $BODY$;

-- Returns the storage-id of this storage, this is created when the Naksha extension is installed and never changes.
CREATE OR REPLACE FUNCTION naksha_storage_id() RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return ${storage_id};
END $BODY$;

-- Returns the storage-id of this storage, this is created when the Naksha extension is installed and never changes.
CREATE OR REPLACE FUNCTION naksha_schema() RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return '${schema}';
END $BODY$;

-- Returns a packed Naksha extension version from the given parameters (major, minor, revision).
CREATE OR REPLACE FUNCTION naksha_version_of(major int, minor int, revision int) RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return ((major::int8 & x'ffff'::int8) << 32)
        | ((minor::int8 & x'ffff'::int8) << 16)
        | (revision::int8 & x'ffff'::int8);
END $BODY$;

-- Unpacks the given packed Naksha version into its parts (major, minor, revision).
CREATE OR REPLACE FUNCTION naksha_version_extract(version int8)
    RETURNS TABLE
            (
                major    int,
                minor    int,
                revision int
            )
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
BEGIN
    RETURN QUERY SELECT ((version >> 32) & x'ffff'::int8)::int  as "major",
                        ((version >> 16) & x'ffff'::int8)::int  as "minor",
                        (version & x'ffff'::int8)::int          as "revision";
END;
$$;

-- Unpacks the given Naksha extension version into a human readable text.
CREATE OR REPLACE FUNCTION naksha_version_to_text(version int8) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return format('%s.%s.%s',
                  (version >> 32) & x'ffff'::int8,
                  (version >> 16) & x'ffff'::int8,
                  (version & x'ffff'::int8));
END $BODY$;

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

-- Helper to test if a text is a valid JSON.
CREATE OR REPLACE FUNCTION naksha_json_id(t TEXT) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
DECLARE
  j jsonb;
BEGIN
  j = t::json;
  IF j->>'id' IS NOT NULL THEN
    RETURN j->>'id';
  END IF;
  RETURN NULL;
EXCEPTION
  WHEN OTHERS THEN RETURN NULL;
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

    sql := format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I '
               || 'FOR VALUES FROM (%L) TO (%L);',
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

-- Returns the information about the collection.
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
CREATE OR REPLACE FUNCTION naksha_tx_start(app_id text, author text, create_tx bool)
    RETURNS uuid
    LANGUAGE 'plpgsql' VOLATILE
AS $$
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
         || ',SET_CONFIG(''naksha.appid'', %L::text, true)' -- same as naksha_tx_set_app_id
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
CREATE OR REPLACE FUNCTION naksha_tx_get_app_id()
    RETURNS text
    LANGUAGE 'plpgsql' STABLE
AS $$
DECLARE
    value  text;
BEGIN
    value := coalesce(current_setting('naksha.appid', true), '');
    IF value = '' THEN
        RETURN NULL;
    END IF;
    RETURN value;
END
$$;

-- Set the application-id to be added into the XYZ namespace, can be set to null.
-- Returns the previously set value that was replaced or NULL, if no value was set.
CREATE OR REPLACE FUNCTION naksha_tx_set_app_id(app_id text)
    RETURNS text
    LANGUAGE 'plpgsql' STABLE
AS $$
DECLARE
    old text;
    sql text;
BEGIN
    old := coalesce(current_setting('naksha.appid', true), '');
    app_id := coalesce(app_id, '');
    sql := format('SELECT SET_CONFIG(%L, %L::text, true)', 'naksha.appid', app_id);
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

    txi := nextval('naksha_tx_object_id_seq');
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
    CREATE EXTENSION IF NOT EXISTS btree_gist SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS postgis_topology SCHEMA topology;
    -- revert search_path to original value (as postgis_topology installation modifies it)
    EXECUTE format('SET SESSION search_path TO %s', orig_search_path);
    CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS "hstore" SCHEMA public;

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