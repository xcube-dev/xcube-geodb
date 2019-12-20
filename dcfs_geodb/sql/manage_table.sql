CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';


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
									created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
									modified_at TIMESTAMP WITH TIME ZONE,
									geometry geometry(Geometry,' || crs || ') NOT NULL
							   )', dataset);

    PERFORM geodb_add_properties(dataset, properties);

    EXECUTE format('CREATE TRIGGER update_%s_modtime
                    BEFORE UPDATE ON %s
                    FOR EACH ROW EXECUTE PROCEDURE update_modified_column()', dataset, dataset);

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

    RETURN 'success';
END
$BODY$;
