-- CREATE EXTENSION postgis;

CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';


CREATE OR REPLACE FUNCTION public.geodb_create_collection(IN collection text, IN properties json, IN crs text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    usr text;
    tab text;
BEGIN
    usr := (SELECT geodb_whoami());

    tab := usr || '_' ||  collection;

    EXECUTE format('CREATE TABLE %I(
								    id SERIAL PRIMARY KEY,
									created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
									modified_at TIMESTAMP WITH TIME ZONE,
									geometry geometry(Geometry,' || crs || ') NOT NULL
							   )', tab);

    PERFORM geodb_add_properties(collection, properties);

    EXECUTE format('CREATE TRIGGER update_%s_modtime
                    BEFORE UPDATE ON %I_%I
                    FOR EACH ROW EXECUTE PROCEDURE update_modified_column()', tab, usr, collection);

    EXECUTE format('ALTER TABLE %I OWNER to %I;', tab, usr);
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_create_collections(IN "collections" json)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    "collection_row" record;
    properties json;
    crs text;
BEGIN
    FOR collection_row IN SELECT "key"::text,"value" FROM json_each(collections) LOOP
            properties := (SELECT "value"::json FROM collection_row.value WHERE "key" = 'properties');
            crs := (SELECT "value"::text FROM collection_row.value WHERE "key" = 'crs');
            EXECUTE format('SELECT geodb_create_collection(''%s'', ''%s''::json, ''%s'')',
            collection_row.key,
            properties,
            crs);
    END LOOP;
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_drop_collections(IN collections json)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    "collection_row" record;
    usr text;
    tab text;
BEGIN
    usr := (SELECT geodb_whoami());

    FOR collection_row IN SELECT collection FROM json_array_elements_text(collections) as collection LOOP
            tab := usr || '_' ||  collection_row.collection;
            EXECUTE format('DROP TABLE %I', tab);
    END LOOP;
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_grant_access_to_collection(collection text, usr text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('GRANT SELECT ON TABLE %I TO %I;', collection, usr);

END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_revoke_access_to_collection(collection text, usr text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('REVOKE SELECT ON TABLE %I FROM %I;', collection, usr);

END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_list_collections()
    RETURNS TABLE(src json)
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE usr text;
BEGIN
    usr := (SELECT geodb_whoami());

    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as js ' ||
                                'FROM (SELECT
                                           regexp_replace(tablename, ''%s_'', '''') as table_name
                                        FROM
                                           pg_catalog.pg_tables
                                        WHERE
                                           schemaname = ''public''
                                            AND tableowner = ''%s'') AS src',
                                usr, usr);

END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_list_grants()
    RETURNS TABLE(src json)
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE usr text;
BEGIN
    usr := (SELECT geodb_whoami());
    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as js ' ||
                                'FROM (SELECT table_name, grantee ' ||
                                'FROM information_schema.role_table_grants ' ||
                                'WHERE grantor = ''%s'' AND grantee != ''%s'') AS src',
                                usr, usr);

END
$BODY$;
