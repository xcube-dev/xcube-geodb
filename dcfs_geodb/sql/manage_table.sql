-- CREATE EXTENSION postgis;

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
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    usr text;
    tab text;
BEGIN
    usr := (SELECT geodb_whoami());

    tab := usr || '_' ||  dataset;

    EXECUTE format('CREATE TABLE %I(
								    id SERIAL PRIMARY KEY,
									created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
									modified_at TIMESTAMP WITH TIME ZONE,
									geometry geometry(Geometry,' || crs || ') NOT NULL
							   )', tab);

    PERFORM geodb_add_properties(dataset, properties);

    EXECUTE format('CREATE TRIGGER update_%s_modtime
                    BEFORE UPDATE ON %I_%I
                    FOR EACH ROW EXECUTE PROCEDURE update_modified_column()', tab, usr, dataset);

    EXECUTE format('ALTER TABLE %I OWNER to %I;', tab, usr);
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_create_datasets(IN "datasets" json)
    RETURNS void
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
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_drop_datasets(IN datasets json)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    "dataset_row" record;
    usr text;
    tab text;
BEGIN
    usr := (SELECT geodb_whoami());

    FOR dataset_row IN SELECT dataset FROM json_array_elements_text(datasets) as dataset LOOP
            tab := usr || '_' ||  dataset_row.dataset;
            EXECUTE format('DROP TABLE %I', tab);
    END LOOP;
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_grant_access_to_dataset(dataset text, usr text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('GRANT SELECT ON TABLE %I TO %I;', dataset, usr);

END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_revoke_access_to_dataset(dataset text, usr text)
    RETURNS void
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('REVOKE SELECT ON TABLE %I FROM %I;', dataset, usr);

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
