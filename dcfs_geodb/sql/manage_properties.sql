CREATE OR REPLACE FUNCTION public.geodb_get_column_info_from_json(IN properties json)
    RETURNS TABLE("name" VARCHAR(255), "type"  VARCHAR(255))
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE 'SELECT ("column"->>''name'')::VARCHAR(255) , ("column"->>''type'')::VARCHAR(255) ' ||
                         'FROM json_array_elements(''' || properties || '''::json) AS "column"';
END
$BODY$;

DROP FUNCTION geodb_add_properties(text, json);

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

DROP FUNCTION geodb_drop_properties(text, json);

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


