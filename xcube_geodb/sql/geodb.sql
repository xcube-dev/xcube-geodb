CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS geodb_user_info;

-- cleanup litter of previous versions

DROP FUNCTION IF EXISTS public.geodb_check_user_grants(text);
DROP FUNCTION IF EXISTS public.geodb_copy_collection2(text, text);
DROP FUNCTION IF EXISTS public.geodb_copy_collection3(text, text);
DROP FUNCTION IF EXISTS public.geodb_dashboard_view_query(text);
DROP FUNCTION IF EXISTS public.geodb_extract_database(text, text);
DROP FUNCTION IF EXISTS public.geodb_get_geodb_version();
DROP FUNCTION IF EXISTS public.geodb_get_mvt();
DROP FUNCTION IF EXISTS public.geodb_get_mvt_geom();
DROP FUNCTION IF EXISTS public.geodb_get_nearest(text, float8, float8, int4, text,
                                                 text, text, int4, int4);
DROP FUNCTION IF EXISTS public.geodb_grant_user_admin(text);
DROP FUNCTION IF EXISTS public.geodb_group_users(text);
DROP FUNCTION IF EXISTS public.geodb_list_users();
DROP FUNCTION IF EXISTS public.geodb_log_sizes();
DROP FUNCTION IF EXISTS public.geodb_reassign_owned(text, text);
DROP FUNCTION IF EXISTS public.geodb_remove_index(text);
DROP FUNCTION IF EXISTS public.geodb_remove_index(text, text);
DROP FUNCTION IF EXISTS public.geodb_test_exception();

-- cleanup end

-- do not remove - this function is called from PostGREST before any database query is done
CREATE OR REPLACE FUNCTION public.geodb_check_user()
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS
$$
BEGIN
    IF current_user = 'anonymous' THEN
        RAISE SQLSTATE 'PT403' USING DETAIL = 'Anonymous users do not have access.',
            HINT = 'Access denied.';
    END IF;
END
$$;


CREATE SEQUENCE IF NOT EXISTS public.geodb_user_info_id_seq
    INCREMENT 1
    START 2
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;


CREATE TABLE IF NOT EXISTS public."geodb_user_info"
(
    id           INT                    NOT NULL PRIMARY KEY DEFAULT nextval('geodb_user_info_id_seq'),
    user_name    CHARACTER VARYING(255) NOT NULL UNIQUE,
    start_date   DATE                   NOT NULL,
    subscription TEXT                   NOT NULL,
    permissions  TEXT                   NOT NULL
);

CREATE TABLE IF NOT EXISTS public."geodb_version_info"
(
    id      SERIAL PRIMARY KEY,
    version TEXT NOT NULL,
    date    DATE NOT NULL
);
GRANT SELECT ON TABLE geodb_version_info TO PUBLIC;
INSERT INTO geodb_version_info
VALUES (DEFAULT, '1.0.11dev', now());
-- if manually setting up the database, this might be necessary to clean up:
DELETE
FROM geodb_version_info
WHERE version like '%ERSION_PLACEHOLDER';

CREATE TABLE IF NOT EXISTS public."geodb_eventlog"
(
    event_type TEXT      NOT NULL,
    message    TEXT      NOT NULL,
    username   TEXT      NOT NULL,
    date       TIMESTAMP NOT NULL
);
GRANT ALL ON TABLE geodb_eventlog TO PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_log_event(event json)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    INSERT INTO public."geodb_eventlog"
    VALUES (event ->> 'event_type',
            event ->> 'message',
            event ->> 'user',
            now());
END
$BODY$;

-- todo - this function should check permissions on the collections
-- todo - and be renamed to geodb_get_eventlog
CREATE OR REPLACE FUNCTION public.get_geodb_eventlog(
    "event_type" text DEFAULT '%',
    "collection" text DEFAULT '%')
    RETURNS TABLE
            (
                events json
            )
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE format('SELECT JSON_AGG(temp) from ' ||
                                '(SELECT * from geodb_eventlog ' ||
                                'WHERE event_type like ''%s''' ||
                                'AND message like ''%%%s%%'') AS temp',
                                event_type, collection);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION get_geodb_eventlog(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_register_user_trg_func()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS
$BODY$
BEGIN
    IF NEW.user_name IS NOT NULL THEN
        EXECUTE format('SELECT geodb_register_user(''%s''::text, ''bla''::text)', NEW.user_name);
    END IF;

    RETURN NEW;
END;
$BODY$;


DROP TRIGGER IF EXISTS geodb_register_user_trg ON "geodb_user_info";

CREATE TRIGGER geodb_register_user_trg
    AFTER INSERT
    ON "geodb_user_info"
    FOR EACH ROW
EXECUTE PROCEDURE geodb_register_user_trg_func();


CREATE SEQUENCE IF NOT EXISTS public.geodb_user_databases_seq
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;


CREATE TABLE IF NOT EXISTS public.geodb_user_databases
(
    id    bigint                                         NOT NULL DEFAULT nextval('geodb_user_databases_seq'::regclass),
    name  character varying COLLATE pg_catalog."default" NOT NULL,
    owner character varying COLLATE pg_catalog."default" NOT NULL,
    iss   character varying COLLATE pg_catalog."default",
    CONSTRAINT geodb_user_databases_pkey PRIMARY KEY (id),
    CONSTRAINT unique_db_name_owner UNIQUE (name, owner)
)
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

GRANT SELECT, INSERT, UPDATE ON geodb_user_databases TO PUBLIC;
GRANT SELECT, UPDATE, USAGE ON geodb_user_databases_seq TO PUBLIC;

-- ensures that database of collection belongs to user or group
CREATE
    OR REPLACE FUNCTION public.geodb_user_allowed(
    collection text,
    usr text)
    RETURNS INT
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS
$BODY$
DECLARE
    ct        INT;
    groupname varchar;
BEGIN
    -- noinspection SqlAggregates
    SELECT COUNT(*) as ct
    FROM geodb_user_databases
    WHERE collection LIKE name || '_%'
      AND owner = usr
    INTO ct;
    if ct > 0 then
        return 1;
    end if;

    -- User is also allowed if any of the groups he is in is allowed

    FOR groupname IN
        (SELECT rolname
         FROM pg_roles
         WHERE pg_has_role(usr, oid, 'member'))
        LOOP
            if groupname != usr then
                SELECT geodb_user_allowed(collection, groupname)
                INTO ct;
                if ct > 0 then
                    return 1;
                end if;
            end if;
        END LOOP;

    return 0;

END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_user_allowed(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_create_database(
    database text)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS
$BODY$
DECLARE
    usr TEXT;
    ct  INT;
BEGIN
    usr := (SELECT geodb_whoami());

    -- noinspection SqlAggregates

    SELECT COUNT(*) as ct FROM geodb_user_databases WHERE name = database INTO ct;

    IF ct > 0 THEN
        RAISE EXCEPTION 'Database % exists already.', database;
    END IF;

    EXECUTE format('INSERT INTO geodb_user_databases(name, owner, iss) VALUES(''%s'', ''%s'', '''')', "database", usr);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_create_database(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_truncate_database(
    database text)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS
$BODY$
DECLARE
    usr text;
BEGIN
    usr := (SELECT geodb_whoami());

    EXECUTE format('DELETE FROM geodb_user_databases WHERE name=''%s'' and owner=''%s''', "database", usr);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_truncate_database(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_add_properties(IN collection text, IN properties json)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    props_row record;
BEGIN
    FOR props_row IN SELECT "key", "value" FROM json_each_text(properties)
        LOOP
            EXECUTE format('ALTER TABLE %I ADD COLUMN %I %s', collection, lower(props_row.key), props_row.value);
        END LOOP;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_add_properties(text, json) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_drop_properties(IN collection text, IN properties json)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    props_row record;
    usr       text;
    tab       text;
BEGIN
    usr := (SELECT geodb_whoami());
    tab := usr || '_' || collection;

    FOR props_row IN SELECT property FROM json_array_elements(properties::json) AS property
        LOOP
            EXECUTE format('ALTER TABLE %I DROP COLUMN %s', collection, props_row.property);
        END LOOP;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_drop_properties(text, json) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_get_properties(collection text)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as js ' ||
                                'FROM (SELECT
                                             name as database,
                                             regexp_replace(table_name, name || ''_'','''') as "collection",
                                             column_name,
                                             data_type
                                        FROM information_schema.columns
										LEFT JOIN geodb_user_databases
											ON table_name LIKE name || ''_%%''
                                        WHERE
                                           table_schema = ''public''
                                            AND table_name = ''%s'') AS src',
                                collection);

END
$BODY$;

CREATE OR REPLACE FUNCTION update_modified_column()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.modified_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';


CREATE OR REPLACE FUNCTION public.geodb_create_collection(collection text, properties json, crs text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    usr   text;
    trigg text;
    owns  INT;
BEGIN
    usr := (SELECT geodb_whoami());

    SELECT geodb_user_allowed(collection, usr) INTO owns;

    if owns = 0 then
        RAISE EXCEPTION 'No access for user %.', usr;
    end if;

    EXECUTE format('CREATE TABLE %I(
								    id SERIAL PRIMARY KEY,
									created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
									modified_at TIMESTAMP WITH TIME ZONE,
									geometry geometry(Geometry,' || crs || ') NOT NULL
							   )', collection);

    PERFORM geodb_add_properties(collection, properties);

    trigg := 'update_' || collection || '_modtime';

    EXECUTE format('CREATE TRIGGER %I
                    BEFORE UPDATE ON %I
                    FOR EACH ROW EXECUTE PROCEDURE update_modified_column()', trigg, collection);

    EXECUTE 'GRANT ALL ON TABLE "' || collection || '" TO postgres';

    EXECUTE format('ALTER TABLE %I OWNER to %I;', collection, usr);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_create_collection(text, json, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_create_collections("collections" json)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    "collection_row" record;
    properties       json;
    crs              text;
BEGIN
    FOR collection_row IN SELECT "key"::text, "value" FROM json_each(collections)
        LOOP
            properties := (SELECT "value"::json FROM json_each(collection_row.value) WHERE "key" = 'properties');
            crs := (SELECT "value"::text FROM json_each(collection_row.value) WHERE "key" = 'crs');
            EXECUTE format('SELECT geodb_create_collection(''%s'', ''%s''::json, ''%s'')',
                           collection_row.key,
                           properties,
                           crs);
        END LOOP;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_create_collections(json) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_drop_collections(collections json, "cascade" bool DEFAULT TRUE)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    "collection_row" record;
BEGIN
    FOR collection_row IN SELECT collection FROM json_array_elements_text(collections) as collection
        LOOP
            IF "cascade" THEN
                EXECUTE format('DROP TABLE %I CASCADE', collection_row.collection);
            ELSE
                EXECUTE format('DROP TABLE %I', collection_row.collection);
            END IF;
        END LOOP;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_drop_collections(json, bool) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_grant_access_to_collection(
    collection text,
    usr text)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS
$BODY$
DECLARE
    seq_name text;
BEGIN
    select pg_get_serial_sequence('"' || collection || '"', 'id') into seq_name;
    IF seq_name IS NULL
    THEN
        raise exception 'No sequence for collection %', collection;
    END IF;
    EXECUTE format('GRANT SELECT ON TABLE %I TO %I;', collection, usr);
    EXECUTE format('GRANT USAGE, SELECT ON SEQUENCE %s TO %I', seq_name, usr);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_grant_access_to_collection(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_revoke_access_from_collection(
    collection text,
    usr text)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS
$BODY$
DECLARE
    seq_name TEXT;
BEGIN
    select replace(pg_get_serial_sequence('"' || collection || '"', 'id'), 'public.', '') into seq_name;
    EXECUTE format('REVOKE SELECT ON TABLE %I FROM %I;', collection, usr);
    EXECUTE format('REVOKE USAGE, SELECT ON SEQUENCE %s FROM %I', seq_name, usr);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_revoke_access_from_collection(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_get_collection_bbox(collection text)
    RETURNS text
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    qry  TEXT;
    bbox TEXT;
BEGIN
    qry := format('SELECT text(ST_Extent(geometry)) from %I AS src',
                  collection);
    EXECUTE qry INTO bbox;

    RETURN bbox;
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_estimate_collection_bbox(collection text)
    RETURNS text
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    qry  TEXT;
    bbox TEXT;
BEGIN
    qry := format('SELECT ST_EstimatedExtent(%L, ''geometry'') AS src',
                  collection);
    EXECUTE qry INTO bbox;

    RETURN bbox;
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_geometry_types(collection text, aggregate boolean DEFAULT true)
    RETURNS TABLE
            (
                types json
            )
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    qry TEXT;
BEGIN
    IF aggregate THEN
        qry := format('SELECT JSON_AGG(temp) AS types FROM (
                        SELECT DISTINCT GeometryType(geometry) FROM %I) AS temp',
                      collection);
    ELSE
        qry := format('SELECT JSON_AGG(temp) AS types FROM (
                        SELECT GeometryType(geometry) FROM %I) AS temp',
                      collection);
    END IF;
    RETURN QUERY EXECUTE qry;
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_get_my_collections(
    database text DEFAULT NULL::text)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS
$BODY$
DECLARE
    usr           text;
    database_cond text;
    qry           text;
BEGIN
    usr := (SELECT geodb_whoami());
    IF database IS NULL
    THEN
        database_cond := '';
    ELSE
        database_cond := format(' WHERE db = ''%s''', "database");
    END IF;

    qry := format('SELECT JSON_AGG(src) as js ' ||
                  '
                  FROM (
                    SELECT
                        owner,
                        db as database,
                        regexp_replace(table_name, db || ''_'', '''') as "collection"
                    FROM
                    (
                        SELECT table_name,
                           (SELECT name FROM geodb_user_databases WHERE table_name LIKE name || ''_%%'' ORDER BY name DESC LIMIT 1) AS db,
                           (SELECT owner FROM geodb_user_databases WHERE table_name LIKE name || ''_%%'' ORDER BY name DESC LIMIT 1) AS owner
                        FROM information_schema."tables"
                        WHERE table_schema = ''public''
                            AND table_name NOT LIKE ''geodb_user%%''
                            AND table_name NOT LIKE ''geodb_functions%%''
                            AND table_name NOT LIKE ''geodb_size_log%%''
                            AND table_name != ''spatial_ref_sys''
                            AND table_name != ''geography_columns''
                            AND table_name != ''geometry_columns''
                            AND table_name != ''raster_columns''
                            AND table_name != ''raster_overviews''
                            AND table_name NOT LIKE ''pg_%%''
                        ORDER BY table_name
                    ) as inf
                    %s
                 ) AS src',
                  database_cond);

    RETURN QUERY EXECUTE qry;
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_list_grants(database TEXT)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS
$BODY$
DECLARE
    usr text;
BEGIN
    usr := (SELECT geodb_whoami());

    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as js
                                FROM (SELECT
								regexp_replace(table_name, ''%s_'', '''') as "collection",
								grantee
                                FROM information_schema.role_table_grants
                                WHERE grantor = ''%s'' AND grantee != ''%s'') AS src',
                                database, usr, usr);

END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_list_grants()
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS
$BODY$
DECLARE
    usr text;
BEGIN
    usr := (SELECT geodb_whoami());

    RETURN QUERY SELECT geodb_list_grants(usr);

END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_list_databases()
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS
$$
DECLARE
    usr text;
BEGIN
    usr := (SELECT geodb_whoami());

    RETURN QUERY EXECUTE format(
            'SELECT JSON_AGG(src) as js FROM(' ||
            'SELECT * FROM geodb_user_databases WHERE owner = ''%s'') as src', usr);
END
$$;

REVOKE EXECUTE ON FUNCTION geodb_list_databases() FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_rename_collection(
    collection text,
    new_name text)
    RETURNS text
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS
$BODY$
DECLARE
    allowed INT;
    usr     TEXT;
    qry     TEXT;
BEGIN
    usr := (SELECT geodb_whoami());

    qry := 'SELECT geodb_user_allowed(''' || new_name || ''',''' || usr || ''')';

    EXECUTE qry INTO allowed;

    IF allowed = 1 THEN
        EXECUTE format('ALTER TABLE %I RENAME TO %I', collection, new_name);
        RETURN 'success';
    END IF;

    raise exception '% has not access to that table or database. You might have to create the database first.', usr;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_rename_collection(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_publish_collection(
    collection text)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS
$BODY$
BEGIN
    EXECUTE format('GRANT SELECT ON TABLE %I TO PUBLIC;', collection);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_publish_collection(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_unpublish_collection(
    collection text)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
AS
$BODY$
BEGIN
    EXECUTE format('REVOKE SELECT ON TABLE %I FROM PUBLIC;', collection);

END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_unpublish_collection(text) FROM PUBLIC;

DO
$do$
    BEGIN
        IF NOT EXISTS(SELECT
                      FROM pg_catalog.pg_roles -- SELECT list can be empty for this
                      WHERE rolname = 'authenticator') THEN
            CREATE ROLE authenticator NOINHERIT;
            ALTER ROLE authenticator SET search_path = public;
        END IF;
    END
$do$;


DO
$do$
    BEGIN
        IF NOT EXISTS(SELECT
                      FROM pg_catalog.pg_roles -- SELECT list can be empty for this
                      WHERE rolname = 'geodb_admin') THEN
            CREATE ROLE geodb_admin NOINHERIT;
            ALTER ROLE geodb_admin SET search_path = public;
        END IF;
    END
$do$;

CREATE OR REPLACE FUNCTION public.geodb_whoami()
    RETURNS text
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    RETURN current_user;
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_get_geodb_sql_version()
    RETURNS text
    LANGUAGE SQL
AS
$$
SELECT version
from geodb_version_info
$$;

CREATE OR REPLACE FUNCTION public.geodb_register_user(
    user_name text,
    password text)
    RETURNS character varying
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS
$BODY$
DECLARE
    usr TEXT;
    r   RECORD;
BEGIN
    usr := current_setting('request.jwt.claim.clientId', true);
    BEGIN
        EXECUTE format('CREATE ROLE %I LOGIN ROLE %I', user_name, current_user);
    EXCEPTION
        WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    END;
    EXECUTE format('ALTER ROLE %I PASSWORD ''%s''; ALTER ROLE %I SET search_path = public;' ||
                   'GRANT %I TO authenticator;', user_name, password, user_name, user_name);
    FOR r IN
        SELECT p.proname,
               pgn.nspname,
               pg_catalog.pg_get_function_identity_arguments(p.oid) AS arg_types
        FROM pg_proc p
                 JOIN pg_namespace pgn ON p.pronamespace = pgn.oid
        WHERE p.proname LIKE '%geodb_%'
          AND pgn.nspname NOT IN ('pg_catalog', 'information_schema')
          AND p.proname NOT IN ('geodb_get_user_roles', 'geodb_get_user_usage')
        LOOP
            EXECUTE format('GRANT EXECUTE ON FUNCTION ' || r.nspname || '.' || r.proname || '(' || r.arg_types ||
                           ') TO %I ;',
                           user_name);
        END LOOP;
    EXECUTE format('GRANT CREATE ON SCHEMA public TO %I ;', user_name);
    EXECUTE format(
            'INSERT INTO geodb_user_databases(name, owner, iss) VALUES(''%s'',''%s'', ''%s'') ON CONFLICT ON CONSTRAINT unique_db_name_owner DO NOTHING;',
            user_name, user_name, usr);
    RETURN 'success';
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_register_user(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION geodb_register_user(text, text) TO geodb_admin;

CREATE OR REPLACE FUNCTION public.geodb_user_exists(user_name text)
    RETURNS TABLE
            (
                exts boolean
            )
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE format('SELECT EXISTS (SELECT true FROM pg_roles WHERE rolname=''%s'')', user_name);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_user_exists(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION geodb_user_exists(text) TO geodb_admin;


-- DROP FUNCTION IF EXISTS public.geodb_drop_user(text);

CREATE OR REPLACE FUNCTION public.geodb_drop_user(user_name text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    EXECUTE format('DROP ROLE IF EXISTS %I', user_name);
    RETURN true;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_drop_user(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION geodb_drop_user(text) TO geodb_admin;

CREATE OR REPLACE FUNCTION public.geodb_get_user_usage(user_name text)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS
$BODY$
BEGIN
    RETURN QUERY SELECT JSON_AGG(vals) as src
                 FROM (SELECT SUM(pg_total_relation_size(quote_ident(table_name))) AS "usage"
                       FROM information_schema.tables
                       WHERE table_schema = 'public'
                         AND table_name LIKE user_name || '%') AS vals;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_get_user_usage(text) FROM PUBLIC;

-- noinspection SqlUnused

CREATE OR REPLACE FUNCTION public.geodb_get_user_usage(
    user_name text,
    pretty BOOLEAN)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS
$BODY$
BEGIN
    RETURN QUERY SELECT JSON_AGG(vals) as src
                 FROM (SELECT pg_size_pretty(SUM(pg_total_relation_size(quote_ident(table_name)))) AS "usage"
                       FROM information_schema.tables
                       WHERE table_schema = 'public'
                         AND table_name LIKE user_name || '%') AS vals;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_get_user_usage(text, bool) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_get_my_usage()
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS
$BODY$
DECLARE
    me TEXT;
BEGIN
    SELECT geodb_whoami() INTO me;
    RETURN QUERY EXECUTE format('SELECT geodb_get_user_usage(''%I'')', me);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_get_my_usage() FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_get_my_usage(pretty boolean)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS
$BODY$
DECLARE
    me TEXT;
BEGIN
    SELECT geodb_whoami() INTO me;
    RETURN QUERY EXECUTE format('SELECT geodb_get_user_usage(''%I'', ''%s'')', me, pretty);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_get_my_usage(bool) FROM PUBLIC;


CREATE OR REPLACE FUNCTION public.geodb_get_pg(
    collection text,
    "select" text DEFAULT '*',
    "where" text DEFAULT NULL,
    "group" text DEFAULT NULL,
    "order" text DEFAULT NULL,
    "limit" integer DEFAULT NULL,
    "offset" integer DEFAULT NULL
)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS
$BODY$
DECLARE
    row_ct int;
    qry    text;
BEGIN
    qry := format('SELECT %s FROM %I ', "select", "collection");

    IF "where" IS NOT NULL THEN
        qry := qry || format('WHERE %s ', "where");
    END IF;

    IF "group" IS NOT NULL THEN
        qry := qry || format('GROUP BY %s ', "group");
    END IF;

    IF "order" IS NOT NULL THEN
        qry := qry || format('ORDER BY %s ', "order");
    END IF;

    IF "limit" IS NOT NULL THEN
        qry := qry || format('LIMIT %s  ', "limit");
    END IF;

    IF "limit" IS NOT NULL AND "offset" IS NOT NULL THEN
        qry := qry || format('OFFSET %s ', "offset");
    END IF;

    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as src FROM (%s) as src ', qry);

    GET DIAGNOSTICS row_ct = ROW_COUNT;

    IF row_ct < 1 THEN
        RAISE EXCEPTION 'Empty result';
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_get_by_bbox(collection text,
                                                    minx double precision,
                                                    miny double precision,
                                                    maxx double precision,
                                                    maxy double precision,
                                                    comparison_mode VARCHAR(255) DEFAULT 'within',
                                                    bbox_crs int DEFAULT 4326,
                                                    "where" text DEFAULT 'id > 0'::text,
                                                    op text DEFAULT 'AND'::text,
                                                    "limit" int DEFAULT 0,
                                                    "offset" int DEFAULT 0)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

AS
$BODY$
DECLARE
    bbox_func VARCHAR;
    row_ct    int;
    lmt_str   text;
    qry       text;
BEGIN
    CASE comparison_mode
        WHEN 'within' THEN bbox_func := 'ST_Within';
        WHEN 'contains' THEN bbox_func := 'ST_Contains';
        WHEN 'intersects' THEN bbox_func := 'ST_Intersects';
        WHEN 'touches' THEN bbox_func := 'ST_Touches';
        WHEN 'overlaps' THEN bbox_func := 'ST_Overlaps';
        WHEN 'crosses' THEN bbox_func := 'ST_Crosses';
        WHEN 'disjoint' THEN bbox_func := 'ST_Disjoint';
        WHEN 'equals' THEN bbox_func := 'ST_Equals';
        ELSE RAISE EXCEPTION 'comparison mode % does not exist. Use ''within'' | ''contains''', comparison_mode USING ERRCODE = 'data_exception';
        END CASE;

    lmt_str := '';

    IF "limit" > 0 THEN
        lmt_str := ' LIMIT ' || "limit";
    END IF;

    IF "offset" > 0 THEN
        lmt_str := lmt_str || ' OFFSET ' || "offset";
    END IF;

    qry := format('SELECT JSON_AGG(src) as js
                     FROM (SELECT * FROM %I
                     WHERE (%s) %s %s(''SRID=%s;POLYGON((' ||
                  minx
                      || ' ' || miny
                      || ', ' || maxx
                      || ' ' || miny
                      || ', ' || maxx
                      || ' ' || maxy
                      || ', ' || minx
                      || ' ' || maxy
                      || ', ' || minx
                      || ' ' || miny
                      || '))'', geometry) '
                      || ' ORDER BY id '
                      || lmt_str || ') as src',
                  "collection",
                  "where",
                  "op",
                  "bbox_func",
                  "bbox_crs"
           );

    RETURN QUERY EXECUTE qry;

    GET DIAGNOSTICS row_ct = ROW_COUNT;

    IF row_ct < 1 THEN
        RAISE EXCEPTION 'Only % rows!', row_ct;
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_count_collection(collection text)
    RETURNS BIGINT
    LANGUAGE 'plpgsql'
    STRICT
AS
$$
DECLARE
    row_ct int;
    qry    text;
BEGIN
    qry := format('SELECT COUNT(*) from %I', collection);
    EXECUTE qry INTO row_ct;
    RETURN row_ct;
END
$$;

-- see https://stackoverflow.com/a/7945274/2043113
CREATE OR REPLACE FUNCTION public.geodb_estimate_collection_count(collection text)
    RETURNS BIGINT
    LANGUAGE 'plpgsql'
    STRICT
AS
$$
DECLARE
    pages_size int;
    row_ct     int;
    qry        text;
BEGIN
    qry := format('SELECT relpages FROM pg_class WHERE oid = (SELECT oid FROM pg_class WHERE relname = ''%s'');',
                  collection);
    EXECUTE qry into pages_size;
    IF pages_size > 0 then
        qry := format('SELECT (reltuples / relpages * (pg_relation_size(oid) / 8192))::bigint
                     FROM pg_class
                     WHERE oid = (SELECT oid FROM pg_class WHERE relname = ''%s'')', collection);
        EXECUTE qry INTO row_ct;
        RETURN row_ct;
    ELSE
        RAISE NOTICE 'Preferred way of estimation unsupported on table (run VACUUM on table %s to support); performing alternative estimation.', collection;
        qry := format('SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = ''%s'';', collection);
        EXECUTE qry into row_ct;
        RETURN row_ct;
    END IF;
END
$$;

-- TODO Merge with get_by_bbox
CREATE OR REPLACE FUNCTION public.geodb_count_by_bbox(collection text,
                                                      minx double precision,
                                                      miny double precision,
                                                      maxx double precision,
                                                      maxy double precision,
                                                      comparison_mode VARCHAR(255) DEFAULT 'within',
                                                      bbox_crs int DEFAULT 4326,
                                                      "where" text DEFAULT 'id > 0'::text,
                                                      op text DEFAULT 'AND'::text)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

AS
$BODY$
DECLARE
    bbox_func VARCHAR;
    row_ct    int;
    qry       text;
BEGIN
    CASE comparison_mode
        WHEN 'within' THEN bbox_func := 'ST_Within';
        WHEN 'contains' THEN bbox_func := 'ST_Contains';
        WHEN 'intersects' THEN bbox_func := 'ST_Intersects';
        WHEN 'touches' THEN bbox_func := 'ST_Touches';
        WHEN 'overlaps' THEN bbox_func := 'ST_Overlaps';
        WHEN 'crosses' THEN bbox_func := 'ST_Crosses';
        WHEN 'disjoint' THEN bbox_func := 'ST_Disjoint';
        WHEN 'equals' THEN bbox_func := 'ST_Equals';
        ELSE RAISE EXCEPTION 'comparison mode % does not exist. Use ''within'' | ''contains''', comparison_mode USING ERRCODE = 'data_exception';
        END CASE;

    qry := format('SELECT JSON_AGG(src) as js
                     FROM (SELECT COUNT(*) as ct FROM %I
                     WHERE (%s) %s %s(''SRID=%s;POLYGON((' ||
                  minx
                      || ' ' || miny
                      || ', ' || maxx
                      || ' ' || miny
                      || ', ' || maxx
                      || ' ' || maxy
                      || ', ' || minx
                      || ' ' || maxy
                      || ', ' || minx
                      || ' ' || miny
                      || '))'', geometry) '
                      || ') as src',
                  "collection",
                  "where",
                  "op",
                  "bbox_func",
                  "bbox_crs"
           );

    RETURN QUERY EXECUTE qry;

    GET DIAGNOSTICS row_ct = ROW_COUNT;

    IF row_ct < 1 THEN
        RAISE EXCEPTION 'Only % rows!', row_ct;
    END IF;
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_get_collection_srid(
    collection text)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS
$BODY$
DECLARE
    res INTEGER;
BEGIN
    SELECT Find_SRID('public', collection, 'geometry') INTO res;
    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as js ' ||
                                'FROM (SELECT %s AS srid
                               ) AS src',
                                res);
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_copy_collection(
    old_collection text,
    new_collection text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS
$$
DECLARE
    usr     text;
    allowed int;
    qry     text;
BEGIN
    usr := (SELECT geodb_whoami());

    qry := 'SELECT geodb_user_allowed(''' || new_collection || ''',''' || usr || ''')';
    raise notice '%, %, %', new_collection, usr, qry;

    EXECUTE qry INTO allowed;

    IF allowed = 1 THEN
        EXECUTE 'CREATE TABLE "' || new_collection || '"(LIKE "' || old_collection || '" INCLUDING ALL)';

        EXECUTE 'GRANT ALL ON TABLE "' || new_collection || '" TO postgres';

        EXECUTE 'CREATE TRIGGER "update_' || new_collection || '_modtime"
                BEFORE UPDATE
                    ON "' || new_collection || '"
                FOR EACH ROW
                    EXECUTE PROCEDURE public.update_modified_column()';

        EXECUTE 'INSERT INTO "' || new_collection || '" (SELECT * FROM  "' || old_collection || '")';
        RETURN 'SUCCESS';
    END IF;

    raise exception '% has not access to that table or database.', usr;
END
$$;

REVOKE EXECUTE ON FUNCTION geodb_copy_collection(text, text) FROM PUBLIC;


CREATE OR REPLACE FUNCTION public.geodb_show_indexes(collection text)
    RETURNS TABLE
            (
                indexname name
            )
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE format('SELECT indexname FROM pg_indexes WHERE tablename = ''%s''', collection);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_show_indexes(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_create_index(collection text, property text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    idx_name text;
BEGIN
    idx_name := geodb_get_index_name(collection, property);
    IF property = 'geometry' then
        EXECUTE format('CREATE INDEX %I ON %I USING GIST(%I)', idx_name, collection, property);
    ELSE
        EXECUTE format('CREATE INDEX %I ON %I (%I)', idx_name, collection, property);
    END IF;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_create_index(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_drop_index(collection text, property text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    idx_name text;
BEGIN
    idx_name := geodb_get_index_name(collection, property);
    EXECUTE format('DROP INDEX %I', idx_name);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_drop_index(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_get_index_name(collection text, property text)
    RETURNS text
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    idx_name             text;
    collection_shortened text;
BEGIN
    idx_name := format('idx_%s_%s', property, collection);
    collection_shortened := collection;
    WHILE LENGTH(idx_name) > 63
        LOOP
            collection_shortened := SUBSTR(collection_shortened, 2, LENGTH(collection_shortened));
            idx_name := format('idx_%s_%s', property, collection_shortened);
        END LOOP;
    RETURN idx_name;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_get_index_name(text, text) FROM PUBLIC;

-- group functions

CREATE OR REPLACE FUNCTION public.geodb_create_role(user_name text, user_group text)
    RETURNS void
    LANGUAGE 'plpgsql'
    SECURITY DEFINER
    -- see https://www.cybertec-postgresql.com/en/abusing-security-definer-functions/
    SET search_path = public,pg_temp
AS
$BODY$
DECLARE
    manage integer;
BEGIN
    EXECUTE format('SELECT COUNT(*) FROM geodb_user_info WHERE user_name = ''%s'' AND subscription LIKE ''%%manage%%''',
                   user_name) INTO manage;
    IF NOT current_setting('role') = 'geodb_admin' AND manage = 0 THEN
        RAISE EXCEPTION 'Insufficient subscription for user %', user_name;
    END IF;

    EXECUTE format('CREATE ROLE %I NOLOGIN
                                   NOSUPERUSER
                                   NOCREATEDB
                                   NOCREATEROLE
                                   NOREPLICATION', user_group);
    EXECUTE format('GRANT %I TO %I WITH ADMIN OPTION;', user_group, user_name);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_create_role(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_group_publish_collection(collection text, user_group text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    EXECUTE format('GRANT ALL ON TABLE %I TO %I', collection, user_group);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_group_publish_collection(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_group_unpublish_collection(collection text, user_group text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    EXECUTE format('REVOKE ALL ON TABLE %I FROM %I', collection, user_group);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_group_unpublish_collection(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_group_publish_database(database text, user_group text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    EXECUTE format('INSERT INTO geodb_user_databases(name, owner) VALUES(''%s'', ''%s'')', database, user_group);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_group_publish_database(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_group_unpublish_database(database text, user_group text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    EXECUTE format('DELETE FROM geodb_user_databases WHERE name = ''%s'' AND owner = ''%s''', database, user_group);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_group_unpublish_database(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_get_user_roles(user_name text)
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE format(
            'SELECT JSON_AGG(src) FROM
                (SELECT rolname FROM pg_roles
                    WHERE pg_has_role(''%s'', oid, ''MEMBER'')
                ) as src', user_name);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_get_user_roles(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_get_group_users(group_name text)
    RETURNS TABLE
            (
                res json
            )
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE format(
            'SELECT JSON_AGG(res) FROM
                (SELECT rolname from pg_catalog.pg_roles
                    WHERE oid in
                        (SELECT member FROM pg_catalog.pg_roles AS roles JOIN pg_catalog.pg_auth_members m ON m.roleid = roles.oid
                            WHERE roles.rolname = ''%s'')) as res;', group_name);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_get_group_users(text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_group_grant(user_group text, user_name text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    EXECUTE format('GRANT %I TO %I;', user_group, user_name);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_group_grant(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_group_revoke(user_group text, user_name text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    EXECUTE format('REVOKE %I FROM %I;', user_group, user_name);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_group_revoke(text, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_get_grants(collection text)
    RETURNS TABLE
            (
                res json
            )
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE format(
            'SELECT JSON_AGG(res) FROM
                (SELECT grantee::varchar, privilege_type::varchar
                    FROM information_schema.role_table_grants
                    WHERE table_name=''%s''
                    AND (grantee = current_user
                        OR grantee in
                            (SELECT rolname FROM pg_roles WHERE pg_has_role(current_user, oid, ''member''))
                        )
                ) as res', collection);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_get_grants(text) FROM PUBLIC;

-- below: metadata support

CREATE SCHEMA IF NOT EXISTS geodb_collection_metadata;
GRANT USAGE ON SCHEMA geodb_collection_metadata to PUBLIC;

DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1
                       FROM pg_type t
                                JOIN pg_namespace n ON n.oid = t.typnamespace
                       WHERE t.typname = 'provider_role'
                         AND n.nspname = 'geodb_collection_metadata') THEN
            CREATE TYPE geodb_collection_metadata.provider_role AS ENUM ('licensor', 'producer', 'processor', 'host');
            CREATE TYPE geodb_collection_metadata.relation AS ENUM ('self', 'root', 'parent', 'child', 'collection', 'item');
        END IF;
    END
$$;

CREATE TABLE IF NOT EXISTS geodb_collection_metadata.basic
(
    collection_name TEXT            NOT NULL,
    database        TEXT            NOT NULL,
    title           CHARACTER VARYING(255),
    description     TEXT            NOT NULL DEFAULT 'No description available',
    license         TEXT            NOT NULL DEFAULT 'proprietary',
    spatial_extent  GEOMETRY[]      NULL,
    temporal_extent TIMESTAMPTZ[][] NOT NULL DEFAULT ARRAY [ ARRAY [NULL::TIMESTAMPTZ, NULL::TIMESTAMPTZ]],
    stac_extensions TEXT[]          NULL     DEFAULT ARRAY []::TEXT[],
    keywords        TEXT[]          NULL     DEFAULT ARRAY []::TEXT[],
    summaries       JSONB           NULL,
    CONSTRAINT basic_unique_collection_name_database UNIQUE ("collection_name", database)
);

CREATE TABLE IF NOT EXISTS geodb_collection_metadata.link
(
    id              SERIAL PRIMARY KEY,
    href            TEXT                               NOT NULL,
    rel             geodb_collection_metadata.relation NOT NULL,
    type            TEXT,
    title           TEXT,
    method          TEXT,
    headers         JSONB,
    body            JSONB,
    collection_name TEXT                               NOT NULL,
    database        TEXT                               NOT NULL,
    FOREIGN KEY (collection_name, database) REFERENCES geodb_collection_metadata.basic (collection_name, database)
);

CREATE TABLE IF NOT EXISTS geodb_collection_metadata.provider
(
    name            TEXT PRIMARY KEY,
    description     TEXT                                      NULL,
    roles           geodb_collection_metadata.provider_role[] NULL DEFAULT ARRAY []::geodb_collection_metadata.provider_role[],
    url             TEXT                                      NULL,
    collection_name TEXT                                      NOT NULL,
    database        TEXT                                      NOT NULL,
    FOREIGN KEY (collection_name, database) REFERENCES geodb_collection_metadata.basic (collection_name, database)
);

CREATE TABLE IF NOT EXISTS geodb_collection_metadata.asset
(
    id              SERIAL PRIMARY KEY,
    href            TEXT   NOT NULL,
    title           TEXT   NULL,
    description     TEXT   NULL,
    type            TEXT   NULL,
    roles           TEXT[] NULL DEFAULT ARRAY []::TEXT[],
    collection_name TEXT   NOT NULL,
    database        TEXT   NOT NULL,
    FOREIGN KEY (collection_name, database) REFERENCES geodb_collection_metadata.basic (collection_name, database)
);

CREATE TABLE IF NOT EXISTS geodb_collection_metadata."item_asset"
(
    id              SERIAL PRIMARY KEY,
    title           TEXT   NULL,
    description     TEXT   NULL,
    type            TEXT   NULL,
    roles           TEXT[] NULL DEFAULT ARRAY []::TEXT[],
    collection_name TEXT   NOT NULL,
    database        TEXT   NOT NULL,
    FOREIGN KEY (collection_name, database) REFERENCES geodb_collection_metadata.basic (collection_name, database)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA geodb_collection_metadata TO PUBLIC;

CREATE OR REPLACE FUNCTION public.geodb_get_metadata(collection text, db text)
    RETURNS JSONB
    LANGUAGE 'plpgsql'
AS
$$
DECLARE
    mp              RECORD;
    result          JSONB;
    link_data       JSONB := '[]';
    provider_data   JSONB := '[]';
    asset_data      JSONB := '[]';
    item_asset_data JSONB := '[]';
    usr             TEXT;
    ct              INT;
BEGIN
    usr := (SELECT geodb_whoami());
    SELECT geodb_user_allowed(db || '_' || collection, usr) INTO ct;

    IF ct = 0 then
        RAISE EXCEPTION 'No access to collection % for user %.', db || '_' || collection, usr;
    end if;

    SELECT md.*,
           ARRAY(
                   SELECT jsonb_build_object(
                                  'minx', ST_XMin(g),
                                  'miny', ST_YMin(g),
                                  'maxx', ST_XMax(g),
                                  'maxy', ST_YMax(g)
                          )
                   FROM unnest(md.spatial_extent) AS g
           ) AS spatial_extent
    INTO mp
    FROM geodb_collection_metadata.basic md
    WHERE md.collection_name = collection
      AND md.database = db;

    SELECT COALESCE(jsonb_agg(to_jsonb(li)), '[]'::jsonb)
    INTO link_data
    FROM geodb_collection_metadata.link li
    WHERE li.collection_name = mp.collection_name
      AND li.database = mp.database;

    SELECT COALESCE(jsonb_agg(to_jsonb(pr)), '[]'::jsonb)
    INTO provider_data
    FROM geodb_collection_metadata.provider pr
    WHERE pr.collection_name = mp.collection_name
      AND pr.database = mp.database;

    SELECT COALESCE(jsonb_agg(to_jsonb(a)), '[]'::jsonb)
    INTO asset_data
    FROM geodb_collection_metadata.asset a
    WHERE a.collection_name = mp.collection_name
      AND a.database = mp.database;

    SELECT COALESCE(jsonb_agg(to_jsonb(ia)), '[]'::jsonb)
    INTO item_asset_data
    FROM geodb_collection_metadata.item_asset ia
    WHERE ia.collection_name = mp.collection_name
      AND ia.database = mp.database;

    -- Construct final JSON
    result := jsonb_build_object(
            'basic', to_jsonb(mp),
            'links', link_data,
            'providers', provider_data,
            'assets', asset_data,
            'item_assets', item_asset_data
              );
    return result;
END
$$;

CREATE OR REPLACE FUNCTION public.geodb_set_spatial_extent(collection text, db text, se float8[][], srid int)
    RETURNS SETOF jsonb
    LANGUAGE 'plpgsql'
AS
$$
DECLARE
    bbox   float8[];
    result geometry[];
BEGIN

    FOREACH bbox SLICE 1 IN ARRAY se
        LOOP
            IF array_length(bbox, 1) = 4 THEN
                result := result || ST_Transform(ST_MakeEnvelope(
                                                         bbox[1], -- minx
                                                         bbox[2], -- miny
                                                         bbox[3], -- maxx
                                                         bbox[4], -- maxy
                                                         srid), 4326);
            END IF;
        END LOOP;

    UPDATE geodb_collection_metadata.basic md
    SET spatial_extent = result
    WHERE md.collection_name = collection
      and md.database = db;
END
$$;

CREATE OR REPLACE FUNCTION public.geodb_set_metadata_field(field text, value json, collection text, db text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS
$$
DECLARE
    link       JSON;
    asset      JSON;
    roles      TEXT[];
    item_asset JSON;
    ts_array   TIMESTAMPTZ[][];
    usr        TEXT;
    ct         INT;
BEGIN

    usr := (SELECT geodb_whoami());
    SELECT geodb_user_allowed(db || '_' || collection, usr) INTO ct;

    IF ct = 0 then
        RAISE EXCEPTION 'No access to collection % for user %.', db || '_' || collection, usr;
    end if;

    RAISE NOTICE 'Setting metadata field % to % for %_%', field, value, collection, db;
    IF field IN ('title', 'description', 'license') THEN
        SELECT COUNT(*)
        FROM geodb_collection_metadata.basic
        WHERE collection_name = collection
          and database = db
        INTO ct;
        IF ct = 0 THEN
            EXECUTE format('
            INSERT INTO geodb_collection_metadata.basic (%s, collection_name, database)
            VALUES (''%s'', ''%s'', ''%s'');', field, replace(value #>> '{}', '"', ''), collection, db);
        ELSE
            EXECUTE format('
            UPDATE geodb_collection_metadata.basic md
            SET %s = ''%s''
            WHERE md.collection_name = ''%s''
              and md.database = ''%s'';'
                , field, replace(value #>> '{}', '"', ''), collection, db);
        END IF;
    ELSIF field IN ('keywords', 'stac_extensions') THEN
        EXECUTE format('
            UPDATE geodb_collection_metadata.basic md
            SET %s = ''%s''
            WHERE md.collection_name = ''%I''
              and md.database = ''%I'';'
            , field, ARRAY(SELECT json_array_elements_text(value)), collection, db);
    ELSIF field = 'links' THEN
        DELETE
        FROM geodb_collection_metadata.link li
        WHERE li.collection_name = collection
          AND li.database = db;
        FOR link in SELECT json_array_elements(value)
            LOOP
                EXECUTE format('
                INSERT INTO geodb_collection_metadata.link (href, rel, type, title, method, headers, body, collection_name, database)
                VALUES (''%s'', ''%s'', ''%s'', ''%s'', ''%s'', %L, %L, ''%I'', ''%I'');',
                               link ->> 'href',
                               link ->> 'rel',
                               link ->> 'type',
                               link ->> 'title',
                               link ->> 'method',
                               (link ->> 'headers')::JSONB,
                               (link ->> 'body')::JSONB,
                               collection,
                               db);
            END LOOP;
    ELSIF field = 'providers' THEN
        DELETE
        FROM geodb_collection_metadata.provider pr
        WHERE pr.collection_name = collection
          AND pr.database = db;
        FOR asset in SELECT json_array_elements(value)
            LOOP
                IF asset -> 'roles' IS NOT NULL THEN
                    SELECT ARRAY(SELECT json_array_elements_text(asset -> 'roles'))
                    INTO roles;
                ELSE
                    roles := '{}'::TEXT[];
                END IF;
                EXECUTE format('
                INSERT INTO geodb_collection_metadata.provider (name, description, roles, url, collection_name, database)
                VALUES (''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'');',
                               asset ->> 'name',
                               asset ->> 'description',
                               roles,
                               asset ->> 'url',
                               collection,
                               db);
            END LOOP;
    ELSIF field = 'assets' THEN
        DELETE
        FROM geodb_collection_metadata.asset a
        WHERE a.collection_name = collection
          AND a.database = db;
        FOR asset in SELECT json_array_elements(value)
            LOOP
                IF asset -> 'roles' IS NOT NULL THEN
                    SELECT ARRAY(SELECT json_array_elements_text(asset -> 'roles'))
                    INTO roles;
                ELSE
                    roles := '{}'::TEXT[];
                END IF;
                EXECUTE format('
                INSERT INTO geodb_collection_metadata.asset (href, title, description, type, roles, collection_name, database)
                VALUES (''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'');',
                               asset ->> 'href',
                               asset ->> 'title',
                               asset ->> 'description',
                               asset ->> 'type',
                               roles,
                               collection,
                               db);
            END LOOP;
    ELSIF field = 'item_assets' THEN
        DELETE
        FROM geodb_collection_metadata.item_asset a
        WHERE a.collection_name = collection
          AND a.database = db;
        FOR item_asset in SELECT json_array_elements(value)
            LOOP
                IF item_asset -> 'roles' IS NOT NULL THEN
                    SELECT ARRAY(SELECT json_array_elements_text(item_asset -> 'roles'))
                    INTO roles;
                ELSE
                    roles := '{}'::TEXT[];
                END IF;
                EXECUTE format('
                INSERT INTO geodb_collection_metadata."item_asset" (title, description, type, roles, collection_name, database)
                VALUES (''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'');',
                               item_asset ->> 'title',
                               item_asset ->> 'description',
                               item_asset ->> 'type',
                               roles,
                               collection,
                               db);
            END LOOP;
    ELSIF field = 'temporal_extent' THEN
        SELECT ARRAY(
                       SELECT ARRAY(
                                      SELECT CASE
                                                 WHEN elem IS NULL OR elem::text = 'null' THEN NULL
                                                 ELSE elem::text::TIMESTAMPTZ
                                                 END
                                      FROM json_array_elements(outer_elem::json) AS elem
                              )
                       FROM json_array_elements(value::json) AS outer_elem
               )
        INTO ts_array;
        EXECUTE format('
            UPDATE geodb_collection_metadata.basic md
            SET %s = ''%s''
            WHERE md.collection_name = ''%I''
              and md.database = ''%I'';'
            , field, ts_array, collection, db);
    ELSIF field = 'summaries' THEN
        EXECUTE format('
            UPDATE geodb_collection_metadata.basic md
            SET %s = ''%s''
            WHERE md.collection_name = ''%I''
              and md.database = ''%I'';'
            , field, value, collection, db);
    ELSE
        RAISE EXCEPTION 'Invalid field; must be one of "title", "description", "license", "keywords", "stac_extensions", "links", "providers", "assets", "item_assets", "temporal_extent", "summaries"';
    END IF;
END
$$;

-- Below: watching PostGREST schema cache changes to the database, and trigger a
-- reload.
-- Code copied from https://postgrest.org/en/stable/references/schema_cache.html.
-- BEGIN of copied code
-- watch CREATE and ALTER
CREATE OR REPLACE FUNCTION pgrst_ddl_watch() RETURNS event_trigger AS
$$
DECLARE
    cmd record;
BEGIN
    FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            IF cmd.command_tag IN (
                                   'CREATE SCHEMA', 'ALTER SCHEMA', 'CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO',
                                   'ALTER TABLE', 'CREATE FOREIGN TABLE', 'ALTER FOREIGN TABLE', 'CREATE VIEW',
                                   'ALTER VIEW', 'CREATE MATERIALIZED VIEW', 'ALTER MATERIALIZED VIEW',
                                   'CREATE FUNCTION', 'ALTER FUNCTION', 'CREATE TRIGGER', 'CREATE TYPE', 'ALTER TYPE',
                                   'CREATE RULE', 'COMMENT'
                )
                -- don't notify in case of CREATE TEMP table or other objects created on pg_temp
                AND cmd.schema_name is distinct from 'pg_temp'
            THEN
                NOTIFY pgrst, 'reload schema';
            END IF;
        END LOOP;
END;
$$ LANGUAGE plpgsql;

-- watch DROP
CREATE OR REPLACE FUNCTION pgrst_drop_watch() RETURNS event_trigger AS
$$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
        LOOP
            IF obj.object_type IN (
                                   'schema', 'table', 'foreign table', 'view', 'materialized view', 'function',
                                   'trigger', 'type', 'rule'
                )
                AND obj.is_temporary IS false -- no pg_temp objects
            THEN
                NOTIFY pgrst, 'reload schema';
            END IF;
        END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP EVENT TRIGGER IF EXISTS pgrst_ddl_watch;
CREATE EVENT TRIGGER pgrst_ddl_watch
    ON ddl_command_end
EXECUTE PROCEDURE pgrst_ddl_watch();

DROP EVENT TRIGGER IF EXISTS pgrst_drop_watch;
CREATE EVENT TRIGGER pgrst_drop_watch
    ON sql_drop
EXECUTE PROCEDURE pgrst_drop_watch();
-- END of copied code