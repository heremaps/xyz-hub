---------------------------------------------------------------------------------------------------
-- NEW Naksha extensions
---------------------------------------------------------------------------------------------------
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
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS btree_gist;
-- CREATE EXTENSION pldbgapi;

-- Notice:
-- When service starts, check all connectors and their schemas if they are up-to-date with ext-version
-- When a new connector is created/updated, synchronously check the database and schema and extension
--   - Should we reject connector create/update, if the database is not accessible or anything fails?
--   - I think we make it optionally like: ?failOrError=false
-- After a certain time period, e.g. every 4 hours, check connectors and especially ensure that history tables exist!
-- Clarify: What happens when the space is modified and the "tableName" is changed?
--    First solution: Do not allow tableName change after creation (review later!)
-- TODO: Add versioning!
-- TODO: Add function to create config-schema!
-- TODO: In long term, maintenance thread needs to clear history (drop too old tables!)
-- TODO: For each space, ask to TTL of history (per space!)
-- TODO: Add notifications for new transactions and then in the publisher ID updated, listen to them to avoid polling for new transactions
-- TODO: tomorrow:
--   Use Token API in Wikvaya Proxy!
--   Deploy new Wikvaya and new XYZ-Hub to E2E!
--   Create ADMIN Tokens
--   Fix that owner is set correctly in Token and used by XYZ-Hub
-- TODO: Change namespace from xyz-e2e ... to naksha-e2e ...
-- TODO: Fix space creation in XYZ-Hub to use the naksha extension
-- Ticket: https://confluence.in.here.com/pages/viewpage.action?pageId=1585083118

-- We use function to prevent typos when accessing the config schema.
-- Additionally we can change the config schema this way.
-- Note: There can be only one central config schema in every database.
CREATE OR REPLACE FUNCTION naksha_config_schema() RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return 'xyz_config';
END $BODY$;

CREATE OR REPLACE FUNCTION naksha_config_transactions() RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return 'transactions';
END $BODY$;

-- Returns the version: 16 bit reserved, 16 bit major, 16 bit minor, 16 bit revision
CREATE OR REPLACE FUNCTION naksha_version() RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return 1::int8 << 32;
END $BODY$;

CREATE OR REPLACE FUNCTION naksha_version_of(major int, minor int, revision int) RETURNS int8 LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return ((major::int8 & x'ffff'::int8) << 32)
         | ((minor::int8 & x'ffff'::int8) << 16)
         | (revision::int8 & x'ffff'::int8);
END $BODY$;

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

CREATE OR REPLACE FUNCTION naksha_version_text() RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return naksha_version_text(naksha_version());
END $BODY$;

CREATE OR REPLACE FUNCTION naksha_version_text(version int8) RETURNS text LANGUAGE 'plpgsql' IMMUTABLE AS $BODY$
BEGIN
    return format('%s.%s.%s',
                  (version >> 32) & x'ffff'::int8,
                  (version >> 16) & x'ffff'::int8,
                  (version & x'ffff'::int8));
END $BODY$;


-- Create a transaction number from the given UTC timestamp and the given transaction id.
CREATE OR REPLACE FUNCTION naksha_txn_of(ts timestamptz, txid int8)
    RETURNS int8
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    r record;
    tid int8 := (txid & x'fffffff'::int8);
BEGIN
    WITH parts AS (SELECT EXTRACT(year from ts)::int8   as "year",
                          EXTRACT(month from ts)::int8  as "month",
                          EXTRACT(day from ts)::int8    as "day",
                          EXTRACT(hour from ts)::int8   as "hour",
                          EXTRACT(minute from ts)::int8 as "minute")
    SELECT parts.* INTO r FROM parts;
    RETURN ((r.year - 2000) << 52)
               | (r.month << 48)
               | (r.day << 43)
               | (r.hour << 38)
               | (r.minute << 32)
               | ((tid >> 12) << 16)
               | (4::int8 << 12)
        | (tid & x'fff'::int8);
END;
$$;

-- Create the minimal transaction number for the given UTC timestamp.
CREATE OR REPLACE FUNCTION naksha_txn_min(ts timestamptz)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $$
BEGIN
    RETURN naksha_txn_of(ts, 0::int8);
END
$$;

-- Create the maximal transaction number for the given UTC timestamp.
CREATE OR REPLACE FUNCTION naksha_txn_max(ts timestamptz)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $$
BEGIN
    RETURN naksha_txn_of(ts, x'7fffffffffffffff'::int8);
END
$$;

-- Create the unique transaction number for the current transaction.
CREATE OR REPLACE FUNCTION naksha_txn_current()
    RETURNS int8
    LANGUAGE 'plpgsql' STABLE
AS $$
BEGIN
    RETURN naksha_txn_of(current_timestamp, txid_current());
END
$$;

-- Split the given unique transaction number into its part.
CREATE OR REPLACE FUNCTION naksha_txn_extract(txn int8)
    RETURNS TABLE
            (
                year    int8,
                month   int8,
                day     int8,
                hour    int8,
                minute  int8,
                txid    int8
            )
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
BEGIN
    RETURN QUERY SELECT 2000 + ((txn >> 52) & x'ff'::int8)                           as "year",
                        ((txn >> 48) & x'f'::int8)                                   as "month",
                        ((txn >> 43) & x'1f'::int8)                                  as "day",
                        (txn >> 38) & x'1f'::int8                                    as "hour",
                        (txn >> 32) & x'3f'::int8                                    as "minute",
                        (((txn >> 16) & x'ffff'::int8) << 12 | (txn & x'fff'::int8)) as "txid";
END;
$$;

-- Converts the UUID in a byte-array.
CREATE OR REPLACE FUNCTION naksha_uuid_to_bytes(the_uuid uuid)
    RETURNS bytea
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
BEGIN
    return DECODE(REPLACE(the_uuid::text, '-', ''), 'hex');
END
$BODY$;

-- Converts the byte-array into a UUID.
CREATE OR REPLACE FUNCTION naksha_uuid_from_bytes(raw_uuid bytea)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
BEGIN
    RETURN CAST(ENCODE(raw_uuid, 'hex') AS UUID);
END
$BODY$;

-- Create a UUID from the given transaction number and the given identifier.
CREATE OR REPLACE FUNCTION naksha_uuid_of(txn int8, i int8)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
DECLARE
    raw_uuid bytea;
BEGIN
    raw_uuid := set_byte('\x00000000000000000000000000000000'::bytea, 0, ((txn >> 56) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 1, ((txn >> 48) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 2, ((txn >> 40) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 3, ((txn >> 32) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 4, ((txn >> 24) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 5, ((txn >> 16) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 6, ((txn >> 8) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 7, (txn & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 8, (((i >> 56) & x'3f'::int8) | 128)::int);
    raw_uuid := set_byte(raw_uuid, 9, ((i >> 48) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 10, ((i >> 40) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 11, ((i >> 32) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 12, ((i >> 24) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 13, ((i >> 16) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 14, ((i >> 8) & x'ff'::int8)::int);
    raw_uuid := set_byte(raw_uuid, 15, (i & x'ff'::int8)::int);
    RETURN CAST(ENCODE(raw_uuid, 'hex') AS UUID);
END
$BODY$;

-- Fully extracts all values encoded into an UUID.
CREATE OR REPLACE FUNCTION naksha_uuid_extract(the_uuid uuid)
    RETURNS TABLE
            (
                year    int8,
                month   int8,
                day     int8,
                hour    int8,
                minute  int8,
                txid    int8,
                txn     int8,
                i       int8
            )
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
DECLARE
    i int8;
    txn int8;
    raw_uuid bytea;
BEGIN
    raw_uuid := naksha_uuid_to_bytes(the_uuid);
    txn := naksha_uuid_txn(raw_uuid);
    i := naksha_uuid_i(raw_uuid);
    RETURN QUERY SELECT 2000 + ((txn >> 52) & x'ff'::int8)                           as "year",
                        ((txn >> 48) & x'f'::int8)                                   as "month",
                        ((txn >> 43) & x'1f'::int8)                                  as "day",
                        (txn >> 38) & x'1f'::int8                                    as "hour",
                        (txn >> 32) & x'3f'::int8                                    as "minute",
                        (((txn >> 16) & x'ffff'::int8) << 12 | (txn & x'fff'::int8)) as "txid",
                        txn                                                          as "txn",
                        i                                                            as "i";
END
$BODY$;

-- Returns the transaction number encoded in a UUID, given as raw bytes.
CREATE OR REPLACE FUNCTION naksha_uuid_txn(raw_uuid bytea)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
BEGIN
    return ((get_byte(raw_uuid, 0)::int8) << 56)
               | ((get_byte(raw_uuid, 1)::int8) << 48)
               | ((get_byte(raw_uuid, 2)::int8) << 40)
               | ((get_byte(raw_uuid, 3)::int8) << 32)
               | ((get_byte(raw_uuid, 4)::int8) << 24)
               | ((get_byte(raw_uuid, 5)::int8) << 16)
               | ((get_byte(raw_uuid, 6)::int8) << 8)
               | ((get_byte(raw_uuid, 7)::int8));
END
$BODY$;

-- Returns the transaction number encoded in a UUID, given as raw bytes.
CREATE OR REPLACE FUNCTION naksha_uuid_i(raw_uuid bytea)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS
$BODY$
BEGIN
    return ((get_byte(raw_uuid, 8)::int8 & x'3f'::int8) << 56)
               | ((get_byte(raw_uuid, 9)::int8) << 48)
               | ((get_byte(raw_uuid, 10)::int8) << 40)
               | ((get_byte(raw_uuid, 11)::int8) << 32)
               | ((get_byte(raw_uuid, 12)::int8) << 24)
               | ((get_byte(raw_uuid, 13)::int8) << 16)
               | ((get_byte(raw_uuid, 14)::int8) << 8)
        | ((get_byte(raw_uuid, 15)::int8));
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_json_id(f jsonb)
    RETURNS text
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
DECLARE
    id text;
BEGIN
    id = f->>'id'::text;
    IF id IS NULL THEN
        -- 23502
        RAISE EXCEPTION not_null_violation USING MESSAGE = '"id" must not be null';
    END IF;
    RETURN id;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_json_version(f jsonb)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'version')::int8;
EXCEPTION WHEN OTHERS THEN
    RETURN 1::int8;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_json_action(f jsonb)
    RETURNS text
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN f->'properties'->'@ns:com:here:xyz'->>'action';
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_json_uuid(f jsonb)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'uuid')::uuid;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_json_txn(f jsonb)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
DECLARE
    raw_uuid bytea;
BEGIN
    raw_uuid := naksha_uuid_to_bytes((f->'properties'->'@ns:com:here:xyz'->>'uuid')::uuid);
    return naksha_uuid_txn(raw_uuid);
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_json_i(f jsonb)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
DECLARE
    raw_uuid bytea;
BEGIN
    raw_uuid := naksha_uuid_to_bytes((f->'properties'->'@ns:com:here:xyz'->>'uuid')::uuid);
    return naksha_uuid_i(raw_uuid);
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_json_puuid(f jsonb)
    RETURNS uuid
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'puuid')::uuid;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_json_createdAt(f jsonb)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'createdAt')::int8;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_json_updatedAt(f jsonb)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN (f->'properties'->'@ns:com:here:xyz'->>'updatedAt')::int8;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_json_lastUpdatedBy(f jsonb)
    RETURNS text
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    RETURN f->'properties'->'@ns:com:here:mom:meta'->>'lastUpdatedBy';
END
$BODY$;

-- Returns the current milliseconds (epoch time) form the given timestamp.
CREATE OR REPLACE FUNCTION naksha_ts_millis(ts timestamptz)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    return EXTRACT(epoch from ts)::int8 * 1000::int8 + EXTRACT(milliseconds from ts)::int8;
END
$BODY$;

-- Returns the current microseconds (epoch time) form the given timestamp.
CREATE OR REPLACE FUNCTION naksha_ts_micros(ts timestamptz)
    RETURNS int8
    LANGUAGE 'plpgsql' IMMUTABLE
AS $BODY$
BEGIN
    return EXTRACT(epoch from ts)::int8 * 1000000::int8 + EXTRACT(microsecond from ts)::int8;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_space_before_trigger()
    RETURNS trigger
    LANGUAGE 'plpgsql' STABLE
AS $BODY$
DECLARE
    new_uuid uuid;
    txid int8;
    txn int8;
    ts int8;
    i int8;
    xyz jsonb;
BEGIN
    ts := naksha_ts_millis(clock_timestamp());
    txid := txid_current();
    txn := naksha_txn_of(current_timestamp, txid);
    i = nextval('"'||TG_TABLE_SCHEMA||'"."'||TG_TABLE_NAME||'_i_seq"');
    new_uuid := naksha_uuid_of(txn, i);

    IF TG_OP = 'INSERT' THEN
       xyz := NEW.jsondata->'properties'->'@ns:com:here:xyz';
       IF xyz IS NULL THEN
         xyz := '{}'::jsonb;
       END IF;
       xyz := jsonb_set(xyz, '{"action"}', ('"CREATE"')::jsonb, true);
       xyz := jsonb_set(xyz, '{"version"}', '1'::jsonb, true);
       xyz := jsonb_set(xyz, '{"puuid"}', ('null')::jsonb, true);
       xyz := jsonb_set(xyz, '{"uuid"}', ('"'||((new_uuid)::text)||'"')::jsonb, true);
       xyz := jsonb_set(xyz, '{"createdAt"}', (ts::text)::jsonb, true);
       xyz := jsonb_set(xyz, '{"updatedAt"}', (ts::text)::jsonb, true);
	   NEW.jsondata = jsonb_set(NEW.jsondata, '{"properties","@ns:com:here:xyz"}', xyz, true);
       NEW.i = i;
  	   return NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
       xyz := NEW.jsondata->'properties'->'@ns:com:here:xyz';
       xyz := jsonb_set(xyz, '{"action"}', ('"UPDATE"')::jsonb, true);
       xyz := jsonb_set(xyz, '{"version"}', (''||(naksha_json_version(OLD.jsondata)+1::int8))::jsonb, true);
       xyz := jsonb_set(xyz, '{"puuid"}', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'uuid', true);
       xyz := jsonb_set(xyz, '{"uuid"}', ('"'||((new_uuid)::text)||'"')::jsonb, true);
       xyz := jsonb_set(xyz, '{"createdAt"}', OLD.jsondata->'properties'->'@ns:com:here:xyz'->'createdAt', true);
       xyz := jsonb_set(xyz, '{"updatedAt"}', (ts::text)::jsonb, true);
       NEW.jsondata = jsonb_set(NEW.jsondata, '{"properties","@ns:com:here:xyz"}', xyz, true);
       NEW.i = i;
       return NEW;
    END IF;

    -- DELETE
    RETURN OLD;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_space_after_trigger()
    RETURNS trigger
    LANGUAGE 'plpgsql' VOLATILE 
AS $BODY$
DECLARE
    new_uuid uuid;
    txid int8;
    txn int8;
    ts int8;
    i int8;
    xyz jsonb;
BEGIN
    PERFORM naksha_tx_insert(TG_TABLE_SCHEMA, TG_TABLE_NAME);

    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        EXECUTE format('INSERT INTO %s."%s_hst" (jsondata,geo,i) VALUES(%L,%L,%L)',
            TG_TABLE_SCHEMA, TG_TABLE_NAME,
            NEW.jsondata, NEW.geo, NEW.i);
        RETURN NEW;
    END IF;

    ts := naksha_ts_millis(clock_timestamp());
    txid := txid_current();
    txn := naksha_txn_of(current_timestamp, txid);
    i = nextval('"'||TG_TABLE_SCHEMA||'"."'||TG_TABLE_NAME||'_i_seq"');
    new_uuid := naksha_uuid_of(txn, i);

    -- We do these updates, because in the "after-trigger" we only write into history.
    xyz := OLD.jsondata->'properties'->'@ns:com:here:xyz';
    IF xyz IS NULL THEN
      xyz := '{}'::jsonb;
    END IF;
    xyz := jsonb_set(xyz, '{"action"}', ('"DELETE"')::jsonb, true);
    xyz := jsonb_set(xyz, '{"version"}', (''||(naksha_json_version(OLD.jsondata)+1::int8))::jsonb, true);
    xyz := jsonb_set(xyz, '{"puuid"}', xyz->'uuid', true);
    xyz := jsonb_set(xyz, '{"uuid"}', ('"'||((new_uuid)::text)||'"')::jsonb, true);
    -- createdAt stays what it is
    xyz := jsonb_set(xyz, '{"updatedAt"}', (ts::text)::jsonb, true);
    OLD.jsondata = jsonb_set(OLD.jsondata, '{"properties","@ns:com:here:xyz"}', xyz, true);
    OLD.i = i;
    EXECUTE format('INSERT INTO %s."%s_hst" (jsondata,geo,i) VALUES(%L,%L,%L)',
                   TG_TABLE_SCHEMA, TG_TABLE_NAME,
                   OLD.jsondata, OLD.geo, OLD.i);
    RETURN OLD;
END
$BODY$;

-- Ensures that the given schema and table exist as storage location for a space.
CREATE OR REPLACE FUNCTION naksha_space_ensure(_schema text, _table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    before_name text;
    sql text;
BEGIN
    sql := format('CREATE TABLE IF NOT EXISTS %I.%I ('
               || 'jsondata jsonb, '
               || 'geo geometry(GeometryZ, 4326), '
               || 'i int8 PRIMARY KEY NOT NULL)',
        _schema, _table);
    EXECUTE sql;

    sql := format('CREATE SEQUENCE IF NOT EXISTS %I AS int8 OWNED BY %I.i', format('%s_i_seq', _table), _table);
    EXECUTE sql;

    -- Note: The HEAD table is updated very often, therefore we should not fill the indices too
    --       much to avoid too many page splits. This is as well helpful when doing bulk loads.

    -- Indices with important constrains.
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING btree (naksha_json_id(jsondata) ASC) WITH (fillfactor=50)',
                    format('%s_id_idx', _table), _schema, _table);
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING btree (naksha_json_uuid(jsondata) DESC) WITH (fillfactor=50)',
                    format('%s_uuid_idx', _table), _schema, _table);

    -- Indices that can be delayed in creation.
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                 || 'USING gist (geo) WITH (buffering=ON,fillfactor=50)',
                    format('%s_geo_idx', _table), _schema, _table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING btree (naksha_json_txn(jsondata) DESC) WITH (fillfactor=50)',
                   format('%s_txn_idx', _table), _schema, _table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING btree (naksha_json_createdAt(jsondata) DESC) WITH (fillfactor=50)',
                   format('%s_createdAt_idx', _table), _schema, _table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING btree (naksha_json_updatedAt(jsondata) DESC) WITH (fillfactor=50)',
                   format('%s_updatedAt_idx', _table), _schema, _table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING btree (naksha_json_lastUpdatedBy(jsondata) ASC) WITH (fillfactor=50)',
                   format('%s_lastUpdatedBy_idx', _table), _schema, _table);

    -- We need the before trigger to update the XYZ namespace in jsondata.
    before_name = format('%s_before', _table);
    IF NOT EXISTS(SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = _table::regclass and tgname = before_name) THEN
        sql := format('CREATE OR REPLACE TRIGGER %I '
                   || 'BEFORE INSERT OR UPDATE ON %I.%I '
                   ||' FOR EACH ROW EXECUTE FUNCTION naksha_space_before_trigger();',
                       before_name, _schema, _table);
        EXECUTE sql;
    END IF;
END
$BODY$;

-- Ensures that the given schema and table exist as storage location for a space, including the
-- needed history tables and partitions.
CREATE OR REPLACE FUNCTION naksha_space_ensure_with_history(_schema text, _table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    name text;
    ts timestamptz;
BEGIN
    PERFORM naksha_space_ensure(_schema, _table);
    name := format('%s_hst', _table);
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I '
                || '(jsondata jsonb, geo geometry(GeometryZ, 4326), i int8 NOT NULL) '
                || 'PARTITION BY RANGE (naksha_json_txn(jsondata))',
                   _schema, name);

    -- We rather use the start of the transaction to ensure that every query works.
    --ts = clock_timestamp();
    ts = current_timestamp;
    PERFORM naksha_space_ensure_with_history__partitionForDay(_schema, _table, ts);
    PERFORM naksha_space_ensure_with_history__partitionForDay(_schema, _table, ts + '1 day'::interval);
    PERFORM naksha_space_ensure_with_history__partitionForDay(_schema, _table, ts + '2 day'::interval);
    PERFORM naksha_space_enable_history(_schema, _table);
END
$BODY$;

-- Ensure that the partition for the given day exists.
CREATE OR REPLACE FUNCTION naksha_space_ensure_with_history__partitionForDay(_schema text, _table text, from_ts timestamptz)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    day text;
    to_ts timestamptz;
    from_txn int8;
    to_txn int8;
    hst_name text;
    hst_part_name text;
BEGIN
    day := to_char(from_ts, 'YYYY_MM_DD');
    hst_name := format('%s_hst', _table);
    hst_part_name := format('%s_hst_%s', _table, day); -- example: foo_hst_2023_03_01
    from_txn := naksha_txn_min(from_ts);
    to_ts := from_ts + '1 day'::interval;
    to_txn := naksha_txn_min(to_ts);

    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I '
                || 'FOR VALUES FROM (%s) TO (%s);',
                   _schema, hst_part_name, hst_name,
                   from_txn, to_txn);

    -- Note: The history table is updated only for one day and then never touched again, therefore
    --       we want to fill the indices as much as we can, even while this may have a bad effect
    --       when doing bulk loads, because we may have more page splits.
    -- Therefore: Disable history for bulk loads by removing the triggers!

    -- Indices with important constrains.
    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING btree (naksha_json_uuid(jsondata) DESC) WITH (fillfactor=90);',
                   format('%s_uuid_idx', hst_part_name), _schema, hst_part_name);

    -- Indices that can be delayed in creation.
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                 || 'USING btree (naksha_json_id(jsondata) ASC) '
                 || 'WITH (fillfactor=90);',
                   format('%s_id_idx', hst_part_name), _schema, hst_part_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING gist (geo, naksha_json_version(jsondata), naksha_json_version(jsondata)) '
                || 'WITH (buffering=ON,fillfactor=90);',
                   format('%s_geo_idx', hst_part_name), _schema, hst_part_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING btree (naksha_json_txn(jsondata) DESC) '
                || 'WITH (fillfactor=90);',
                   format('%s_txn_idx', hst_part_name), _schema, hst_part_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING btree (naksha_json_createdAt(jsondata) DESC) '
                || 'WITH (fillfactor=90);',
                   format('%s_createdAt_idx', hst_part_name), _schema, hst_part_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                 || 'USING btree (naksha_json_updatedAt(jsondata) DESC) '
                 || 'WITH (fillfactor=90);',
                   format('%s_updatedAt_idx', hst_part_name), _schema, hst_part_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I '
                || 'USING btree (naksha_json_lastUpdatedBy(jsondata) ASC, naksha_json_version(jsondata) ASC, naksha_json_version(jsondata) DESC) '
                || 'WITH (fillfactor=90);',
                   format('%s_lastUpdatedBy_idx', hst_part_name), _schema, hst_part_name);
END
$BODY$;


-- Enable the history by adding the triggers to the main table.
CREATE OR REPLACE FUNCTION naksha_space_enable_history(_schema text, _table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    r record;
    after_name text;
    sql text;
BEGIN
    after_name = format('%s_after', _table);
    IF NOT EXISTS(SELECT tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid = _table::regclass and tgname = after_name) THEN
        sql := format('CREATE OR REPLACE TRIGGER %I '
                   || 'AFTER INSERT OR UPDATE OR DELETE ON %I.%I '
                   ||' FOR EACH ROW EXECUTE FUNCTION naksha_space_after_trigger();',
                      after_name, _schema, _table);
        EXECUTE sql;
    END IF;
END
$BODY$;

-- Disable the history by dropping the triggers from the main table.
CREATE OR REPLACE FUNCTION naksha_space_disable_history(_schema text, _table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    after_name text;
BEGIN
    after_name = format('%s_after', _table);
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I', after_name, _schema, _table);
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_tx_create_table()
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    cfg_schema       text;
    cfg_transactions text;
    sql              text;
BEGIN
    cfg_schema := naksha_config_schema();
    cfg_transactions := naksha_config_transactions();

    sql := format('CREATE TABLE IF NOT EXISTS %I.%I ('
        || 'i           BIGSERIAL PRIMARY KEY NOT NULL, '
        || 'txseqid     int8, '
        || 'txseqts     timestamptz, '
        || 'txn         int8 NOT NULL, '
        || 'txid        int8 NOT NULL, '
        || 'txts        timestamptz NOT NULL, '
        || '"schema"    text COLLATE "C" NOT NULL, '
        || '"table"     text COLLATE "C" NOT NULL, '
        || 'space       text COLLATE "C", '
        || 'commit_msg  text COLLATE "C",'
        || 'commit_json jsonb'
        || ')', cfg_schema, cfg_transactions);
    EXECUTE sql;

    sql := format('CREATE SEQUENCE IF NOT EXISTS %I.%I AS int8', cfg_schema, format('%s_txseqid', cfg_transactions));
    EXECUTE sql;

    sql := format('CREATE UNIQUE INDEX IF NOT EXISTS %I '
                || 'ON %I.%I USING btree (txseqid DESC, "schema" ASC, "table" ASC)'
                || 'INCLUDE (i)',
                   format('%s_txseqid_idx', cfg_transactions), cfg_schema, cfg_transactions);
    EXECUTE sql;

    sql := format('CREATE UNIQUE INDEX IF NOT EXISTS %I '
                      || 'ON %I.%I USING btree (txseqts DESC)'
                      || 'INCLUDE (i)',
                  format('%s_txseqts_idx', cfg_transactions), cfg_schema, cfg_transactions);
    EXECUTE sql;

    sql := format('CREATE UNIQUE INDEX IF NOT EXISTS %I '
                || 'ON %I.%I USING btree (txn DESC, "schema" ASC, "table" ASC) '
                || 'INCLUDE (i)',
                   format('%s_txn_idx', cfg_transactions), cfg_schema, cfg_transactions);
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I '
                || 'ON %I.%I USING btree (txts DESC, txn DESC) '
                || 'INCLUDE (i)',
                   format('%s_txts_idx', cfg_transactions), cfg_schema, cfg_transactions);
    EXECUTE sql;

    sql := format('CREATE INDEX IF NOT EXISTS %I '
                || 'ON %I.%I USING btree (space DESC) '
                || 'INCLUDE (i)',
                   format('%s_space_idx', cfg_transactions), cfg_schema, cfg_transactions);
    EXECUTE sql;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_tx_insert(_schema text, _table text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    cfg_schema       text;
    cfg_transactions text;
    sql              text;
BEGIN
    cfg_schema := naksha_config_schema();
    cfg_transactions := naksha_config_transactions();

    sql := format('INSERT INTO %I.%I (txid, txts, txn, "schema", "table") '
               || 'VALUES (%s, %L::timestamptz, %s, %L, %L) '
               || 'ON CONFLICT DO NOTHING',
                  cfg_schema, cfg_transactions,
                 txid_current(), current_timestamp, naksha_txn_current(), _schema, _table);
    EXECUTE sql;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '%s, sql = %', SQLERRM, sql;
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_tx_add_commit_msg(id text, msg text)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
BEGIN
    PERFORM naksha_tx_add_commit_msg(id, msg, null);
END
$BODY$;

CREATE OR REPLACE FUNCTION naksha_tx_add_commit_msg(id text, msg text, attachment jsonb)
    RETURNS void
    LANGUAGE 'plpgsql' VOLATILE
AS $BODY$
DECLARE
    cfg_schema       text;
    cfg_transactions text;
    sql              text;
BEGIN
    cfg_schema := naksha_config_schema();
    cfg_transactions := naksha_config_transactions();

    sql := format('INSERT INTO %I.%I (txid, txts, txn, "schema", "table", commit_msg, commit_json) '
                      || 'VALUES (%s, %L::timestamptz, %s, %L, %L, %L, %L) '
                      || 'ON CONFLICT (txn, "schema", "table") DO UPDATE '
                      || 'SET commit_msg = %L, commit_json = %L::jsonb',
                  cfg_schema, cfg_transactions,
                  txid_current(), current_timestamp, naksha_txn_current(), 'COMMIT_MSG', id, msg, attachment,
                  msg, attachment
        );
    EXECUTE sql;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '%, sql = %', SQLERRM, sql;
END
$BODY$;
