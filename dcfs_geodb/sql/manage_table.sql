CREATE OR REPLACE FUNCTION public.geodb_get_table_infos_from_json(IN datasets json)
    RETURNS TABLE("name" VARCHAR(255), "properties"  json, "crs" text)
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE 'SELECT ("dataset"->>''name'')::VARCHAR(255) , ' ||
                         '("dataset"->>''properties'')::json, ' ||
                         '("dataset"->>''crs'')' ||
                         'FROM json_array_elements(''' || "datasets" || '''::json) AS "dataset"';
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_create_dataset(IN dataset text, IN properties json, IN crs text)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('CREATE TABLE %s(
								    id SERIAL PRIMARY KEY,
									created_date date NOT NULL,
									last_updated_date date NOT NULL,
									geometry geometry(Geometry,' || crs || ') NOT NULL
							   )', dataset);
    SELECT geodb_add_properties(dataset, properties);
    RETURN 'success';
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_create_datasets(IN "datasets" json)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    "dataset_row" record;
BEGIN
    FOR dataset_row IN SELECT * FROM geodb_get_table_infos_from_json("datasets") LOOP
        EXECUTE format('SELECT geodb_create_dataset(''%s'', ''%s''::json, ''%s'')',
            dataset_row.name,
            dataset_row.properties,
            dataset_row.crs);
    END LOOP;

    RETURN 'success';
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_drop_datasets(IN datasets json)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    "dataset_row" record;
BEGIN
    FOR dataset_row IN SELECT dataset FROM json_array_elements("datasets"::json) AS dataset LOOP
        EXECUTE format('DROP TABLE %s', dataset_row.dataset);
    END LOOP;

    RETURN 'success';END
$BODY$;
