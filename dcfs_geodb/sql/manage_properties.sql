CREATE OR REPLACE FUNCTION public.geodb_get_column_info_from_json(IN properties json)
    RETURNS TABLE("name" VARCHAR(255), "type"  VARCHAR(255))
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE 'SELECT lower(("column"->>''name''))::VARCHAR(255) , ("column"->>''type'')::VARCHAR(255) ' ||
                         'FROM json_array_elements(''' || properties || '''::json) AS "column"';
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_add_properties(IN dataset text, IN properties json)
    RETURNS void
    LANGUAGE 'plpgsql'
    AS $BODY$
    DECLARE
        props_row record;
        usr text;
        tab text;
    BEGIN
        usr := (SELECT geodb_whoami());

        tab := usr || '_' ||  dataset;
        FOR props_row IN SELECT * FROM geodb_get_column_info_from_json(properties) LOOP
            EXECUTE format('ALTER TABLE %I ADD COLUMN "%s" %s', tab, props_row.name, props_row.type);
        END LOOP;
    END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_drop_properties(IN dataset text, IN properties json)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
    DECLARE
        props_row record;
        usr text;
        tab text;
    BEGIN
        usr := (SELECT geodb_whoami());
        tab := usr || '_' ||  dataset;

        FOR props_row IN SELECT property FROM json_array_elements(properties::json) AS property LOOP
        EXECUTE format('ALTER TABLE %I DROP COLUMN %s', tab, props_row.property);
    END LOOP;
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_get_properties(dataset text)
    RETURNS TABLE(src json)
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE usr text;
BEGIN
    usr := (SELECT geodb_whoami());
    dataset := usr || '_' || dataset;

    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as js ' ||
                                'FROM (SELECT
                                           regexp_replace(table_name, ''%s_'', '''') as table_name, ' ||
                                '           column_name, ' ||
                                '           data_type
                                        FROM information_schema.columns
                                        WHERE
                                           table_schema = ''public''
                                            AND table_name = ''%s'') AS src',
                                usr, dataset);

END
$BODY$;


