CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS geodb_user_info;


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
    id           SERIAL              PRIMARY KEY,
    version      TEXT                   NOT NULL,
    date         DATE                   NOT NULL
);
GRANT SELECT ON TABLE geodb_version_info TO PUBLIC;
INSERT INTO geodb_version_info VALUES (DEFAULT, 'VERSION_PLACEHOLDER', now());
-- if manually setting up the database, this might be necessary to clean up:
DELETE FROM geodb_version_info WHERE version like '%ERSION_PLACEHOLDER';

CREATE TABLE IF NOT EXISTS public."geodb_eventlog"
(
    event_type   TEXT                NOT NULL,
    message      TEXT                NOT NULL,
    username     TEXT                NOT NULL,
    date         TIMESTAMP           NOT NULL
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


CREATE OR REPLACE FUNCTION public.notify_ddl_postgrest()
    RETURNS event_trigger
    LANGUAGE plpgsql
AS
$$
BEGIN
    NOTIFY ddl_command_end;
END;
$$;

DROP EVENT TRIGGER IF EXISTS ddl_postgrest;
CREATE EVENT TRIGGER ddl_postgrest ON ddl_command_end
EXECUTE PROCEDURE public.notify_ddl_postgrest();

-- FUNCTION: public.geodb_create_collection(text, json, text)

-- DROP FUNCTION public.geodb_create_collection(text, json, text);

CREATE OR REPLACE FUNCTION public.geodb_user_allowed(
    collection text,
    usr text)
    RETURNS INT
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS
$BODY$
DECLARE
    ct INT;
BEGIN
    -- noinspection SqlAggregates

    SELECT COUNT(*) as ct
    FROM geodb_user_databases
    WHERE collection LIKE name || '_%'
      AND owner = usr
    INTO ct;

    if ct > 0 then
        return 1;
    else
        return 0;
    end if;
END
$BODY$;

-- FUNCTION: public.geodb_create_database(text)

-- DROP FUNCTION public.geodb_create_database(text);

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

-- FUNCTION: public.geodb_add_properties(text, json)

-- DROP FUNCTION public.geodb_add_properties(text, json);

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


-- noinspection SqlNoDataSourceInspectionForFile

-- noinspection SqlResolveForFile


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

-- geodb_drop_collections(json) has been replace by public.geodb_drop_collections(json, bool)
DROP FUNCTION IF EXISTS public.geodb_drop_collections(json);

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

CREATE OR REPLACE FUNCTION public.geodb_get_collection_bbox(collection text)
    RETURNS text
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    qry TEXT;
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
    qry TEXT;
    bbox TEXT;
BEGIN
    qry := format('SELECT ST_EstimatedExtent(%L, ''geometry'') AS src',
                  collection);
    EXECUTE qry INTO bbox;

    RETURN bbox;
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


-- FUNCTION: public.geodb_list_databases()

-- DROP FUNCTION public.geodb_list_databases();

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

ALTER FUNCTION public.geodb_list_databases()
    OWNER TO postgres;



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


-- noinspection SqlSignatureForFile

DO
$do$
    BEGIN
        IF NOT EXISTS(
                SELECT
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
        IF NOT EXISTS(
                SELECT
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
    AS $$ SELECT version from geodb_version_info $$;


-- FUNCTION: public.geodb_log_sizes()

-- DROP FUNCTION public.geodb_log_sizes();

CREATE OR REPLACE FUNCTION public.geodb_log_sizes()
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS
$BODY$
BEGIN
    INSERT INTO geodb_size_log
    SELECT now()
         , *
         , pg_size_pretty(total_bytes) AS total
         , pg_size_pretty(index_bytes) AS "index"
         , pg_size_pretty(toast_bytes) AS "toast"
         , pg_size_pretty(table_bytes) AS "table"
    FROM (
             SELECT *, total_bytes - index_bytes - COALESCE(toast_bytes, 0) AS table_bytes
             FROM (
                      SELECT c.oid
                           , nspname                               AS table_schema
                           , relname                               AS "collection"
                           , c.reltuples                           AS row_estimate
                           , pg_total_relation_size(c.oid)         AS total_bytes
                           , pg_indexes_size(c.oid)                AS index_bytes
                           , pg_total_relation_size(reltoastrelid) AS toast_bytes
                      FROM pg_class c
                               LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE relkind = 'r'
                  ) a
             WHERE table_schema = 'public'
         ) a;
END
$BODY$;


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
BEGIN
    usr := current_setting('request.jwt.claim.clientId', true);
    BEGIN
        EXECUTE format('CREATE ROLE %I LOGIN', user_name);
    EXCEPTION
        WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    END;
    EXECUTE format('ALTER ROLE %I PASSWORD ''%s''; ALTER ROLE %I SET search_path = public;' ||
                   'GRANT %I TO authenticator;', user_name, password, user_name, user_name);
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

CREATE OR REPLACE FUNCTION public.geodb_grant_user_admin(user_name text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    EXECUTE format('GRANT geodb_admin TO %I', user_name);
    RETURN true;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_grant_user_admin(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION geodb_grant_user_admin(text) TO geodb_admin;


-- FUNCTION: public.geodb_check_user()

-- DROP FUNCTION public.geodb_check_user();

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


-- FUNCTION: public.geodb_check_user_grants(text)

-- DROP FUNCTION public.geodb_check_user_grants(text);

CREATE OR REPLACE FUNCTION public.geodb_check_user_grants(grt text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS
$$
DECLARE
    permissions text;
    ct          integer;
BEGIN
    permissions := current_setting('request.jwt.claim.scope', TRUE)::text;

    EXECUTE format('SELECT strpos(''%s'', ''%s'')', permissions, grt) INTO ct;

    IF ct = 0 THEN
        raise 'Not enough access rights to perform this operation: %', grt;
    END IF;

    RETURN TRUE;
END
$$;


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
                 FROM (
                          SELECT SUM(pg_total_relation_size(quote_ident(table_name))) AS "usage"
                          FROM information_schema.tables
                          WHERE table_schema = 'public'
                            AND table_name LIKE user_name || '%'
                      ) AS vals;
END
$BODY$;


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
                 FROM (
                          SELECT pg_size_pretty(SUM(pg_total_relation_size(quote_ident(table_name)))) AS "usage"
                          FROM information_schema.tables
                          WHERE table_schema = 'public'
                            AND table_name LIKE user_name || '%'
                      ) AS vals;
END
$BODY$;

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
        WHEN 'within' THEN
            bbox_func := 'ST_Within';
        WHEN 'contains' THEN
            bbox_func := 'ST_Contains';
        WHEN 'intersects' THEN
            bbox_func := 'ST_Intersects';
        WHEN 'touches' THEN
            bbox_func := 'ST_Touches';
        WHEN 'overlaps' THEN
            bbox_func := 'ST_Overlaps';
        WHEN 'crosses' THEN
            bbox_func := 'ST_Crosses';
        WHEN 'disjoint' THEN
            bbox_func := 'ST_Disjoint';
        WHEN 'equals' THEN
            bbox_func := 'ST_Equals';
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
    LANGUAGE 'plpgsql' STRICT
AS
$$
DECLARE
    row_ct    int;
    qry       text;
BEGIN
    qry := format('SELECT COUNT(*) from %I', collection);
    EXECUTE qry INTO row_ct;
    RETURN row_ct;
END
$$;

-- see https://stackoverflow.com/a/7945274/2043113
CREATE OR REPLACE FUNCTION public.geodb_estimate_collection_count(collection text)
    RETURNS BIGINT
    LANGUAGE 'plpgsql' STRICT
AS
$$
DECLARE
    pages_size int;
    row_ct     int;
    qry        text;
BEGIN
    qry := format('SELECT relpages FROM pg_class WHERE oid = (SELECT oid FROM pg_class WHERE relname = ''%s'');', collection);
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
        WHEN 'within' THEN
            bbox_func := 'ST_Within';
        WHEN 'contains' THEN
            bbox_func := 'ST_Contains';
        WHEN 'intersects' THEN
            bbox_func := 'ST_Intersects';
        WHEN 'touches' THEN
            bbox_func := 'ST_Touches';
        WHEN 'overlaps' THEN
            bbox_func := 'ST_Overlaps';
        WHEN 'crosses' THEN
            bbox_func := 'ST_Crosses';
        WHEN 'disjoint' THEN
            bbox_func := 'ST_Disjoint';
        WHEN 'equals' THEN
            bbox_func := 'ST_Equals';
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


CREATE OR REPLACE FUNCTION public.geodb_get_nearest(
    collection text,
    x double precision,
    y double precision,
    point_crs integer DEFAULT 4326,
    "select" text DEFAULT '*'::text,
    "where" text DEFAULT NULL::text,
    "group" text DEFAULT NULL::text,
    "limit" integer DEFAULT NULL::integer,
    "offset" integer DEFAULT NULL::integer)
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
    qry            text;
    DECLARE row_ct int;
BEGIN
    qry := format(
            'SELECT
                %s,
                ST_AsText(geometry) as aoi,
                ST_Distance(geometry,''SRID=%s;POINT(%s %s)''::geometry) AS distance
            FROM
                %I
            ',
            "select", point_crs, x, y, collection
        );

    IF "where" IS NOT NULL THEN
        qry := qry || format('WHERE %s ', "where");
    END IF;

    IF "group" IS NOT NULL THEN
        qry := qry || format('GROUP BY %s ', "group");
    END IF;

    qry := qry || format(' ORDER BY geometry <#> ''SRID=%s;POINT(%s %s)''::geometry ', point_crs, x, y);

    IF "limit" IS NOT NULL THEN
        qry := qry || format('LIMIT %s  ', "limit");
    END IF;

    IF "limit" IS NOT NULL AND "offset" IS NOT NULL THEN
        qry := qry || format('OFFSET %s ', "offset");
    END IF;

    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as src FROM (%s) as src ', qry);

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


-- FUNCTION: public.geodb_copy_collection(text, text)

-- DROP FUNCTION public.geodb_copy_collection(text, text);

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


-- FUNCTION: public.geodb_dashboard_view_query(text)

-- DROP FUNCTION public.geodb_dashboard_view_query(text);

CREATE OR REPLACE FUNCTION public.geodb_dashboard_view_query(tab text)
    RETURNS TABLE
            (
                aoi_id            character varying,
                country           character varying,
                site_name         character varying,
                indicator_code    character varying,
                sub_aoi           character varying,
                indicator_value   character varying,
                color_code        character varying,
                measurement_value character varying,
                max_time          timestamp without time zone,
                geometry          geometry
            )
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000
AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE format('SELECT i.aoi_id,
                                        i.country,
                                        i.site_name,
                                        i.indicator_code,
                                        i.sub_aoi,
                                        i.indicator_value,
                                        i.color_code,
                                        i.measurement_value,
                                        i.time,
                                        i.geometry
                                    FROM %I i
                                    INNER JOIN
                                    (SELECT aoi_id, indicator_code, max(time) as time FROM %I
                                    GROUP BY aoi_id, indicator_code) tmp
                                        ON i.aoi_id = tmp.aoi_id
                                        AND i.indicator_code = tmp.indicator_code
                                        AND i.time = tmp.time
                                    ORDER BY i.aoi_id, i.indicator_code',
                                tab, tab);
END
$BODY$;

-- FUNCTION: public.geodb_list_users(json)

-- DROP FUNCTION public.geodb_list_users(json);

CREATE OR REPLACE FUNCTION public.geodb_list_users()
    RETURNS TABLE
            (
                src json
            )
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    RETURN QUERY SELECT JSON_AGG(vals) as src
                 FROM (
                          SELECT usename AS role_name,
                                 CASE
                                     WHEN usesuper AND usecreatedb THEN
                                         CAST('superuser, create database' AS pg_catalog.text)
                                     WHEN usesuper THEN
                                         CAST('superuser' AS pg_catalog.text)
                                     WHEN usecreatedb THEN
                                         CAST('create database' AS pg_catalog.text)
                                     ELSE
                                         CAST('' AS pg_catalog.text)
                                     END    role_attributes
                          FROM pg_catalog.pg_user
                          WHERE usename LIKE 'geodb_%'
                          ORDER BY role_name desc
                      ) as vals;
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_show_indexes(collection text)
    RETURNS TABLE(indexname name)
    LANGUAGE 'plpgsql'
AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE format('SELECT indexname FROM pg_indexes WHERE tablename = ''%s''', collection);
END
$BODY$;

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

CREATE OR REPLACE FUNCTION public.geodb_get_index_name(collection text, property text)
    RETURNS text
    LANGUAGE 'plpgsql'
AS
$BODY$
DECLARE
    idx_name text;
    collection_shortened text;
BEGIN
    idx_name := format('idx_%s_%s', property, collection);
    collection_shortened := collection;
    WHILE LENGTH(idx_name) > 63 LOOP
        collection_shortened := SUBSTR(collection_shortened, 2, LENGTH(collection_shortened));
        idx_name := format('idx_%s_%s', property, collection_shortened);
    END LOOP;
    RETURN idx_name;
END
$BODY$;
