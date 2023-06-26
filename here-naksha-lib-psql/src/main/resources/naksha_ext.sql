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
CREATE SCHEMA IF NOT EXISTS "${schema}";
SET SESSION search_path TO "${schema}", public, topology;

-- Returns the packed Naksha extension version: 16 bit reserved, 16 bit major, 16 bit minor, 16 bit revision.
CREATE OR REPLACE FUNCTION naksha_version() RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    --        major               minor               revision
    return (  2::int8 << 32) | (  0::int8 << 16) | (  3::int8);
END $BODY$;

-- Returns the storage-id of this storage, this is created when the Naksha extension is installed and never changes.
CREATE OR REPLACE FUNCTION naksha_storage_id() RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return ${storage_id};
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
    raw_uuid bytea;
    object_id int8;
    ts timestamptz;
    type int;
    connector_id int8;
    year int;
    month int;
    day int;
BEGIN
    raw_uuid := naksha_uuid_to_bytes(the_uuid);
    object_id := naksha_uuid_bytes_get_object_id(raw_uuid);
    ts := naksha_uuid_bytes_get_ts(raw_uuid);
    year := EXTRACT(year FROM ts);
    month := EXTRACT(month FROM ts);
    day := EXTRACT(day FROM ts);
    type := naksha_uuid_bytes_get_type(raw_uuid);
    connector_id := naksha_uuid_connector_id(raw_uuid);
    RETURN QUERY SELECT object_id    as "object_id",
                        year         as "year",
                        month        as "month",
                        day          as "day",
                        type         as "type",
                        connector_id as "connector_id";
END
$BODY$;

-- Creates a feature UUID from the given object_id and timestamp.
CREATE OR REPLACE FUNCTION naksha_feature_uuid_from_object_id_and_ts(object_id int8, ts timestamptz)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS $$
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
DECLARE
    TRANSACTION CONSTANT int := 0;
    FEATURE CONSTANT int := 1;
BEGIN
    RETURN naksha_uuid_of(object_id, ts, TRANSACTION, naksha_storage_id());
END;
$$;

-- Returns the "id" from the given feature.
CREATE OR REPLACE FUNCTION naksha_feature_get_id(f jsonb)
    RETURNS text
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
DECLARE
    id text;
BEGIN
    id = f->>'id'::text;
    IF id IS NULL THEN
        --RAISE NOTICE '%', f::text;
        -- 23502
        --RAISE EXCEPTION not_null_violation USING MESSAGE = '"id" must not be null';
    END IF;
    RETURN id;
END
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

CREATE OR REPLACE FUNCTION __naksha_trigger_fix_jsondata()
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
    author := naksha_tx_get_author();

    IF TG_OP = 'INSERT' THEN
        --RAISE NOTICE 'naksha_trigger_fix_jsondata % %', TG_OP, NEW.jsondata;
        author := naksha_tx_get_author();
        xyz := NEW.jsondata->'properties'->'@ns:com:here:xyz';
        IF xyz IS NULL THEN
            xyz := '{}'::jsonb;
        END IF;
        xyz := jsonb_set(xyz, '{"action"}', ('"CREATE"')::jsonb, true);
        xyz := jsonb_set(xyz, '{"version"}', to_jsonb(1::int8), true);
        xyz := jsonb_set(xyz, '{"collection"}', to_jsonb(TG_TABLE_NAME), true);
        xyz := jsonb_set(xyz, '{"author"}', coalesce(author::jsonb, 'null'), true);
        xyz := jsonb_set(xyz, '{"appId"}', coalesce(app_id::jsonb, 'null'), true);
        xyz := jsonb_set(xyz, '{"puuid"}', 'null'::jsonb, true);
        xyz := jsonb_set(xyz, '{"uuid"}', ('"'||((new_uuid)::text)||'"')::jsonb, true);
        xyz := jsonb_set(xyz, '{"txn"}', ('"'||((txn)::text)||'"')::jsonb, true);
        xyz := jsonb_set(xyz, '{"createdAt"}', to_jsonb(ts_millis), true);
        xyz := jsonb_set(xyz, '{"updatedAt"}', to_jsonb(ts_millis), true);
        xyz := jsonb_set(xyz, '{"rtcts"}', to_jsonb(rts_millis), true);
        xyz := jsonb_set(xyz, '{"rtuts"}', to_jsonb(rts_millis), true);
        IF NEW.jsondata->'properties' IS NULl THEN
            NEW.jsondata = jsonb_set(NEW.jsondata, '{"properties"}', '{}'::jsonb, true);
        END IF;
        NEW.jsondata = jsonb_set(NEW.jsondata, '{"properties","@ns:com:here:xyz"}', xyz, true);
        NEW.i = i;
        --RAISE NOTICE 'naksha_trigger_fix_jsondata return %', NEW.jsondata;
        return NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        --RAISE NOTICE 'naksha_trigger_fix_jsondata % %', TG_OP, NEW.jsondata;
        author := naksha_tx_get_author(OLD.jsondata);
        xyz := NEW.jsondata->'properties'->'@ns:com:here:xyz';
        IF xyz IS NULL THEN
            xyz := '{}'::jsonb;
        END IF;
        xyz := jsonb_set(xyz, '{"action"}', ('"UPDATE"')::jsonb, true);
        xyz := jsonb_set(xyz, '{"version"}', to_jsonb(naksha_feature_get_version(OLD.jsondata) + 1::int8), true);
        xyz := jsonb_set(xyz, '{"collection"}', to_jsonb(TG_TABLE_NAME), true);
        xyz := jsonb_set(xyz, '{"author"}', coalesce(author::jsonb, 'null'), true);
        xyz := jsonb_set(xyz, '{"appId"}', coalesce(app_id::jsonb, 'null'), true);
        xyz := jsonb_set(xyz, '{"puuid"}', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'uuid', true);
        xyz := jsonb_set(xyz, '{"uuid"}', ('"'||((new_uuid)::text)||'"')::jsonb, true);
        xyz := jsonb_set(xyz, '{"txn"}', ('"'||((txn)::text)||'"')::jsonb, true);
        xyz := jsonb_set(xyz, '{"createdAt"}', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'createdAt', true);
        xyz := jsonb_set(xyz, '{"updatedAt"}', to_jsonb(ts_millis), true);
        xyz := jsonb_set(xyz, '{"rtcts"}', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'rtcts', true);
        xyz := jsonb_set(xyz, '{"rtuts"}', to_jsonb(rts_millis), true);
        IF NEW.jsondata->'properties' IS NULl THEN
            NEW.jsondata = jsonb_set(NEW.jsondata, '{"properties"}', '{}'::jsonb, true);
        END IF;
        NEW.jsondata = jsonb_set(NEW.jsondata, '{"properties","@ns:com:here:xyz"}', xyz, true);
        NEW.i = i;
        --RAISE NOTICE 'naksha_trigger_fix_jsondata return %', NEW.jsondata;
        return NEW;
    END IF;

    -- DELETE
    --RAISE NOTICE 'naksha_trigger_fix_jsondata % return %', TG_OP, OLD.jsondata;
    RETURN OLD;
END
$BODY$;

CREATE OR REPLACE FUNCTION __naksha_trigger_write_tx()
    RETURNS trigger
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
BEGIN
    PERFORM __naksha_tx_set_modify_features_of(TG_TABLE_NAME);
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        RETURN NEW;
    END IF;
    RETURN OLD;
END
$BODY$;

CREATE OR REPLACE FUNCTION __naksha_trigger_write_hst()
    RETURNS trigger
    LANGUAGE 'plpgsql' VOLATILE
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
    SQL text;
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        --RAISE NOTICE 'naksha_trigger_write_hst % %', TG_OP, NEW.jsondata;

        -- purge feature.
        SQL := format('DELETE FROM %I.%I WHERE jsondata->>''id'' = %L',
                       TG_TABLE_SCHEMA, format('%s_del', TG_TABLE_NAME), NEW.jsondata->>'id');
        EXECUTE SQL;

        -- write history.
        SQL := format('INSERT INTO %I.%I (jsondata,geo,i) VALUES(%L,%L,%L)',
                       TG_TABLE_SCHEMA, format('%s_hst', TG_TABLE_NAME), NEW.jsondata, NEW.geo, NEW.i);
        EXECUTE SQL;
        RETURN NEW;
    END IF;

    --RAISE NOTICE 'naksha_trigger_write_hst % %', TG_OP, OLD.jsondata;
    rts_millis := naksha_current_millis(clock_timestamp());
    ts_millis := naksha_current_millis(current_timestamp);
    txn := naksha_tx_current();
    i = nextval('"'||TG_TABLE_SCHEMA||'"."'||TG_TABLE_NAME||'_i_seq"');
    new_uuid := naksha_feature_uuid_from_object_id_and_ts(i, current_timestamp);
    author := naksha_tx_get_author(OLD.jsondata);
    app_id := naksha_tx_get_app_id();

    -- We do these updates, because in the "after-trigger" we only write into history.
    xyz := OLD.jsondata->'properties'->'@ns:com:here:xyz';
    xyz := jsonb_set(xyz, '{"action"}', ('"DELETE"')::jsonb, true);
    xyz := jsonb_set(xyz, '{"version"}', to_jsonb(naksha_feature_get_version(OLD.jsondata) + 1::int8), true);
    xyz := jsonb_set(xyz, '{"author"}', coalesce(author::jsonb, 'null'), true);
    xyz := jsonb_set(xyz, '{"appId"}', coalesce(app_id::jsonb, 'null'), true);
    xyz := jsonb_set(xyz, '{"puuid"}', xyz->'uuid', true);
    xyz := jsonb_set(xyz, '{"uuid"}', ('"'||((new_uuid)::text)||'"')::jsonb, true);
    xyz := jsonb_set(xyz, '{"txn"}', ('"'||((txn)::text)||'"')::jsonb, true);
    -- createdAt and rtcts stay what they are
    xyz := jsonb_set(xyz, '{"updatedAt"}', to_jsonb(ts_millis), true);
    xyz := jsonb_set(xyz, '{"rtuts"}', to_jsonb(rts_millis), true);
    OLD.jsondata = jsonb_set(OLD.jsondata, '{"properties","@ns:com:here:xyz"}', xyz, true);
    OLD.i = i;
    -- write delete.
    EXECUTE format('INSERT INTO %I.%I (jsondata,geo,i) VALUES(%L,%L,%L)',
                   TG_TABLE_SCHEMA, format('%s_del', TG_TABLE_NAME), OLD.jsondata, OLD.geo, OLD.i);
    -- write history.
    EXECUTE format('INSERT INTO %I.%I (jsondata,geo,i) VALUES(%L,%L,%L)',
                   TG_TABLE_SCHEMA, format('%s_hst', TG_TABLE_NAME), OLD.jsondata, OLD.geo, OLD.i);
    -- Note: PostgresQL does not support returning modified old records.
    --       Therefore, the next trigger will see the unmodified OLD.jsondata again!
    RETURN OLD;
END
$BODY$;

-- Internal helper to create the MAIN and DELETE tables with default indices.
-- This function does not create any triggers, only the table and the default indices!
CREATE OR REPLACE FUNCTION __naksha_create_table(_table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    sql text;
BEGIN
    sql := format('CREATE TABLE IF NOT EXISTS %I ('
               || 'jsondata   JSONB'
               || ',geo       GEOMETRY(GeometryZ, 4326) '
               || ',i         int8 PRIMARY KEY NOT NULL)',
               _table);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Primary index to avoid that two features with the same "id" are created.
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I '
                || 'USING btree (jsondata->>''id'') DESC) '
                || 'WITH (fillfactor=50)',
                   format('%s_id_idx', _table), _table);

    -- Index to search for one specific feature by its state UUID.
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I '
                || 'USING btree ((jsondata->>''properties''->''@ns:com:here:xyz''->>''uuid'') ASC) '
                || 'WITH (fillfactor=50)',
                   format('%s_uuid_idx', _table), _table);

    -- Index to search for features by geometry.
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I '
                || 'USING gist (geo) '
                || 'WITH (buffering=ON,fillfactor=50)',
                   format('%s_geo_idx', _table), _table);

    -- Index to search for features that have been part of a certain transaction, using "i" for paging.
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I '
                || 'USING btree ((jsondata->>''properties''->''@ns:com:here:xyz''->>''txn'') ASC, i DESC) '
                || 'WITH (fillfactor=50)',
                   format('%s_txn_idx', _table), _table);

    -- Index to search for features that have been created within a certain time window, using "i" for paging.
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I '
                || 'USING btree ((jsondata->>''properties''->''@ns:com:here:xyz''->>''createdAt'') DESC, i DESC) '
                || 'WITH (fillfactor=50)',
                   format('%s_createdAt_idx', _table), _table);

    -- Index to search for features that have been updated within a certain time window, using "i" for paging.
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I '
                || 'USING btree ((jsondata->>''properties''->''@ns:com:here:xyz''->>''updatedAt'') DESC, i DESC) '
                || 'WITH (fillfactor=50)',
                   format('%s_updatedAt_idx', _table), _table);

    -- Index to search what a user has updated (newest first), results order descending by update-time, id and version.
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I '
                || 'USING btree ((jsondata->>''properties''->''@ns:com:here:xyz''->>''lastUpdatedBy'') DESC, '
                || '             (jsondata->>''properties''->''@ns:com:here:xyz''->>''updatedAt'') DESC) '
                || '             (jsondata->>''id'') DESC) '
                || '             (jsondata->>''properties''->''@ns:com:here:xyz''->>''version'')::int8 DESC) '
                || 'WITH (fillfactor=50)',
                   format('%s_lastUpdatedBy_idx', _table), _table);
END
$BODY$;

-- __naksha_collection_maintain(name text)
-- naksha_collection_get_all()
-- naksha_collection_get(name text)
-- naksha_collection_upsert(name text, max_age int)
-- naksha_collection_delete(name text, at timestamptz)
-- naksha_collection_history(name text)
-- naksha_collection_enable_history(name text)
-- naksha_collection_disable_history(name text)

CREATE OR REPLACE FUNCTION naksha_table_ensure(_schema text, _table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    full_name text;
    trigger_name text;
    sql text;
BEGIN
    PERFORM __naksha_create_table(_table);

    sql := format('CREATE SEQUENCE IF NOT EXISTS %I.%I AS int8 OWNED BY %I.i', _schema, format('%s_i_seq', _table), _table);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    PERFORM __naksha_create_table(_schema, format('%s_del', _table));

    -- Update the XYZ namespace in jsondata.
    trigger_name := format('%s_fix_jsondata', _table);
    full_name := format('%I.%I', _schema, _table);
    IF NOT EXISTS(SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = full_name::regclass and tgname = trigger_name) THEN
        sql := format('CREATE TRIGGER %I '
                   || 'BEFORE INSERT OR UPDATE ON %I.%I '
                   || 'FOR EACH ROW EXECUTE FUNCTION naksha_trigger_fix_jsondata();',
                      trigger_name, _schema, _table);
        --RAISE NOTICE '%', sql;
        EXECUTE sql;
    END IF;

    -- Update the transaction table.
    trigger_name := format('%s_write_tx', _table);
    full_name := format('%I.%I', _schema, _table);
    IF NOT EXISTS(SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = full_name::regclass and tgname = trigger_name) THEN
        sql := format('CREATE TRIGGER %I '
                   || 'AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
                   || 'FOR EACH ROW EXECUTE FUNCTION naksha_trigger_write_tx();',
                      trigger_name, _schema, _table);
        --RAISE NOTICE '%', sql;
        EXECUTE sql;
    END IF;
END
$BODY$;

-- Ensures that the given schema and table exist as storage location for a space, including the
-- needed history tables and partitions.
-- This method does NOT enable the history, this has to be done as an own action.
CREATE OR REPLACE FUNCTION naksha_table_ensure_with_history(_schema text, _table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    TABLE_NAME_HST text;
    ts timestamptz;
BEGIN
    PERFORM naksha_table_ensure(_schema, _table);
    TABLE_NAME_HST := format('%s_hst', _table);
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I '
                || '(jsondata jsonb, geo geometry(GeometryZ, 4326), i int8 NOT NULL) '
                || 'PARTITION BY RANGE (naksha_json_txn_ts(jsondata))',
                   _schema, TABLE_NAME_HST);

    -- We rather use the start of the transaction to ensure that every query works.
    --ts = clock_timestamp();
    ts = current_timestamp;
    PERFORM __naksha_create_history_partition_for_day(_schema, _table, ts);
    PERFORM __naksha_create_history_partition_for_day(_schema, _table, ts + '1 day'::interval);
    PERFORM __naksha_create_history_partition_for_day(_schema, _table, ts + '2 day'::interval);
END
$BODY$;

-- Ensure that the partition for the given day exists.
CREATE OR REPLACE FUNCTION __naksha_create_history_partition_for_day(_schema text, _table text, from_ts timestamptz)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    sql text;
    from_day text;
    to_day text;
    to_ts timestamptz;
    hst_name text;
    hst_part_name text;
BEGIN
    from_day := to_char(from_ts, 'YYYY_MM_DD');
    to_ts := from_ts + '1 day'::interval;
    to_day := to_char(to_ts, 'YYYY_MM_DD');
    hst_name := format('%s_hst', _table);
    hst_part_name := format('%s_hst_%s', _table, from_day); -- example: foo_hst_2023_03_01

    sql := format('CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I '
               || 'FOR VALUES FROM (%L::timestamptz) TO (%L::timestamptz);',
                  _schema, hst_part_name, _schema, hst_name,
                  from_day, to_day);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Note: The history table is updated only for one day and then never touched again, therefore
    --       we want to fill the indices as much as we can, even while this may have a bad effect
    --       when doing bulk loads, because we may have more page splits.
    -- Therefore: Disable history for bulk loads by removing the triggers!

    -- Indices with important constrains.
    sql := format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I.%I '
               || 'USING btree (naksha_json_uuid(jsondata) DESC) '
               || 'INCLUDE (i) '
               || 'WITH (fillfactor=90) ',
                  format('%s_uuid_idx', hst_part_name), _schema, hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    -- Indices that can be delayed in creation.
    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
               || 'USING btree (naksha_json_id(jsondata) ASC) '
               || 'INCLUDE (i) '
               || 'WITH (fillfactor=90) ',
                  format('%s_id_idx', hst_part_name), _schema, hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
               || 'USING gist (geo, naksha_json_id(jsondata), naksha_json_version(jsondata)) '
               || 'INCLUDE (i) '
               || 'WITH (buffering=ON,fillfactor=90) ',
                  format('%s_geo_idx', hst_part_name), _schema, hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
               || 'USING btree (naksha_json_txn(jsondata) DESC) '
               || 'INCLUDE (i) '
               || 'WITH (fillfactor=90) ',
                  format('%s_txn_idx', hst_part_name), _schema, hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
               || 'USING btree (naksha_json_createdAt(jsondata) DESC) '
               || 'INCLUDE (i) '
               || 'WITH (fillfactor=90) ',
                  format('%s_createdAt_idx', hst_part_name), _schema, hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
               || 'USING btree (naksha_json_updatedAt(jsondata) DESC) '
               || 'INCLUDE (i) '
               || 'WITH (fillfactor=90) ',
                  format('%s_updatedAt_idx', hst_part_name), _schema, hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
               || 'USING btree (naksha_json_lastUpdatedBy(jsondata) ASC, '
               ||              'naksha_json_id(jsondata) ASC, '
               ||              'naksha_json_version(jsondata) DESC) '
               || 'INCLUDE (i) '
               || 'WITH (fillfactor=90) ',
                  format('%s_lastUpdatedBy_idx', hst_part_name), _schema, hst_part_name);
    --RAISE NOTICE '%', sql;
    EXECUTE sql;
END
$BODY$;


-- Enable the history by adding the triggers to the main table.
CREATE OR REPLACE FUNCTION naksha_table_enable_history(_schema text, _table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    trigger_name text;
    full_name text;
    sql text;
BEGIN
    trigger_name := format('%s_write_hst', _table);
    full_name := format('%I.%I', _schema, _table);
    IF NOT EXISTS(SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = full_name::regclass and tgname = trigger_name) THEN
        sql := format('CREATE TRIGGER %I '
                   || 'AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
                   || 'FOR EACH ROW EXECUTE FUNCTION naksha_trigger_write_hst();',
                      trigger_name, _schema, _table);
        EXECUTE sql;
    END IF;
END
$BODY$;

-- Disable the history by dropping the triggers from the main table.
CREATE OR REPLACE FUNCTION naksha_table_disable_history(_schema text, _table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    trigger_name text;
BEGIN
    trigger_name := format('%s_write_hst', _table);
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I', trigger_name, _schema, _table);
END
$BODY$;

-- Start the transaction by setting the application-identifier, the current author (which may be null)
-- and the returns the transaction number.
--
CREATE OR REPLACE FUNCTION naksha_tx_start(app_id text, author text)
    RETURNS uuid
    LANGUAGE 'plpgsql' VOLATILE
AS $$
BEGIN
    PERFORM naksha_tx_set_app_id(app_id);
    PERFORM naksha_tx_set_author(author);
    RETURN naksha_tx_current();
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
    sql := format('SELECT SET_CONFIG(%L, %L::text, true)', 'naksha.appid', app_id, true);
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
    sql    text;
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
    sql := format('SELECT SET_CONFIG(%L, %L::text, true)', 'naksha.txn', txn, true);
    -- RAISE NOTICE 'create value via sql = %', sql;
    EXECUTE sql;
    RETURN txn;
END
$$;

CREATE OR REPLACE FUNCTION __naksha_tx_set_modify_features_of(collection text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    txn         uuid;
    app_id      text;
    author      text;
BEGIN
    txn = naksha_tx_current();
    app_id = naksha_tx_get_app_id();
    author = naksha_tx_get_author();
    INSERT INTO naksha_tx ("txn", "action", "id", "app_id", "author", "ts", "psql_id")
      VALUES (txn, 'MESSAGE', collection, app_id, author, current_timestamp, txid_current())
      ON CONFLICT DO NOTHING;
END
$BODY$;

-- Set the message with the given identifier
CREATE OR REPLACE FUNCTION naksha_tx_set_msg(id text, msg text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
BEGIN
    PERFORM naksha_tx_set_msg(id, msg, null, null);
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_tx_set_msg(id text, msg text, json jsonb, attachment jsonb)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    txn         uuid;
    app_id      text;
    author      text;
BEGIN
    txn = naksha_tx_current();
    app_id = naksha_tx_get_app_id();
    author = naksha_tx_get_author();
    INSERT INTO naksha_tx ("txn", "action", "id", "app_id", "author", "ts", "psql_id", "msg_text", "msg_json", "msg_attachment")
      VALUES (txn, 'MESSAGE', id, app_id, author, current_timestamp, txid_current(), msg, json, attachment)
      ON CONFLICT DO UPDATE SET "msg_text" = msg, "msg_json" = json, "msg_attachment" = attachment;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '%, sql = %', SQLERRM, sql;
END
$BODY$;

-- Note: We do not partition the transaction table, because we need a primary index about the
--       transaction identifier and we can't partition by it, so partitioning would be impractical
--       even while partitioning by month could make sense.
CREATE OR REPLACE FUNCTION __naksha_create_transaction_table()
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
BEGIN
    CREATE TABLE IF NOT EXISTS naksha_tx (
        "i"                  BIGSERIAL PRIMARY KEY NOT NULL,
        "txn"                uuid NOT NULL,
        "action"             text COLLATE "C" NOT NULL,
        "id"                 text COLLATE "C" NOT NULL,
        "app_id"             text COLLATE "C" NOT NULL,
        "author"             text COLLATE "C" NOT NULL,
        "ts"                 timestamptz NOT NULL,
        "psql_id"            int8 NOT NULL,
        "msg_text"           text COLLATE "C",
        "msg_json"           jsonb,
        "msg_attachment"     bytea,
        "publish_id"         int8,
        "publish_ts"         timestamptz
    );

    CREATE SEQUENCE IF NOT EXISTS naksha_tx_object_id_seq AS int8;

    -- PRIMARY UNIQUE INDEX to search for transactions by transaction number.
    CREATE UNIQUE INDEX IF NOT EXISTS naksha_tx_primary_idx
    ON naksha_tx USING btree ("txn" ASC, "action" ASC, "id" ASC);

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
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_init()
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
BEGIN
    CREATE SCHEMA IF NOT EXISTS public;
    CREATE SCHEMA IF NOT EXISTS topology;
    CREATE EXTENSION IF NOT EXISTS btree_gist SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS postgis_topology SCHEMA topology;
    CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public;
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA public;

    PERFORM __naksha_create_transaction_table();
END
$BODY$;
