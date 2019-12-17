CREATE OR REPLACE FUNCTION public.geodb_create_dataset(IN "table_name" text, IN crs int)
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

    RETURN 'success';
END
$BODY$;



CREATE OR REPLACE FUNCTION public.geodb_drop_dataset(IN "table_name" text)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('DROP TABLE %s', "table_name");

    RETURN 'success';
END
$BODY$;

