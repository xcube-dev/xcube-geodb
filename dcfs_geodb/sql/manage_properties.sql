
DROP FUNCTION IF EXISTS public.geodb_add_properties(text, json);
CREATE OR REPLACE FUNCTION public.geodb_add_properties(IN collection text, IN properties json)
    RETURNS void
    LANGUAGE 'plpgsql'
    AS $BODY$
    DECLARE
        props_row record;
        usr text;
        tab text;
    BEGIN
        usr := (SELECT geodb_whoami());

        tab := usr || '_' ||  collection;
        FOR props_row IN SELECT lower("key"), "value" FROM json_each_text(properties) LOOP
            EXECUTE format('ALTER TABLE %I ADD COLUMN %I %s', tab, props_row.key, props_row.value);
        END LOOP;
    END
$BODY$;


DROP FUNCTION IF EXISTS public.geodb_drop_properties(text, json);
CREATE OR REPLACE FUNCTION public.geodb_drop_properties(IN collection text, IN properties json)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
    DECLARE
        props_row record;
        usr text;
        tab text;
    BEGIN
        usr := (SELECT geodb_whoami());
        tab := usr || '_' ||  collection;

        FOR props_row IN SELECT property FROM json_array_elements(properties::json) AS property LOOP
        EXECUTE format('ALTER TABLE %I DROP COLUMN %s', tab, props_row.property);
    END LOOP;
END
$BODY$;


DROP FUNCTION IF EXISTS public.geodb_get_properties(text);
CREATE OR REPLACE FUNCTION public.geodb_get_properties(collection text)
    RETURNS TABLE(src json)
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE usr text;
BEGIN
    usr := (SELECT geodb_whoami());
    collection := usr || '_' || collection;

    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as js ' ||
                                'FROM (SELECT
                                           regexp_replace(table_name, ''%s_'', '''') as table_name, ' ||
                                '           column_name, ' ||
                                '           data_type
                                        FROM information_schema.columns
                                        WHERE
                                           table_schema = ''public''
                                            AND table_name = ''%s'') AS src',
                                usr, collection);

END
$BODY$;


