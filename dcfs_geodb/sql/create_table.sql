CREATE OR REPLACE FUNCTION public.geodb_create_dataset(IN "table_name" text, IN properties json, IN crs int)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('CREATE TABLE %s(
								    id integer PRIMARY KEY,
									created_date date NOT NULL,
									last_updated_date date NOT NULL,
									geometry geometry(Geometry,' || crs || ') NOT NULL
							   )', "table_name");

    SELECT public.geodb_add_properties("table_name", "properties");
    RETURN 'success';
END
$BODY$;



CREATE OR REPLACE FUNCTION public.geodb_drop_table(IN "table_name" text)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('DROP TABLE %s', "table_name");

    RETURN 'success';
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_add_properties(IN "table_name" text, IN "columns" json)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'

    AS $BODY$
    DECLARE
        "cols" TABLE("name" VARCHAR(255), "type"  VARCHAR(255));
        "cols_row" "cols"%rowtype;
    BEGIN
        "cols" := (SELECT * FROM geodb_get_column_info_from_json("columns"));
        FOR cols_row IN cols LOOP
            EXECUTE format('ALTER TABLE %s ADD COLUMN %s(%s)', "table_name", cols_row.name, cols_row.type);
        END LOOP;

        RETURN 'success';
    END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_drop_properties(IN "table_name" text, IN columns json)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    "cols" TABLE("name" VARCHAR(255));
    "cols_row" "cols"%rowtype;
BEGIN
    "cols" := (SELECT * FROM geodb_get_columns_from_json("columns"));
    FOR cols_row IN cols LOOP
        EXECUTE format('ALTER TABLE %s DROP COLUMN %s', "table_name", cols_row.name);
    END LOOP;
    RETURN 'success';
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_drop_property(IN "table_name" character varying, IN "column_name" character varying)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('ALTER TABLE %s DROP COLUMN %s', "table_name", "column_name");

    RETURN 'success';
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_get_column_info_from_json(IN columns json)
    RETURNS TABLE("name" VARCHAR(255), "type"  VARCHAR(255))
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE 'SELECT ("column"->>''name'')::VARCHAR(255) , ("column"->>''type'')::VARCHAR(255) ' ||
                         'FROM json_array_elements(''' || "columns" || '''::json->''columns'' ) AS "column"';
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_get_columns_from_json(IN columns json)
    RETURNS TABLE("name" VARCHAR(255))
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE 'SELECT ("column"->>''name'')::VARCHAR(255) ' ||
                         'FROM json_array_elements(''' || "columns" || '''::json->''columns'' ) AS "column"';
END
$BODY$;
