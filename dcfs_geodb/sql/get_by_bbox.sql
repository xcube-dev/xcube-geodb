CREATE OR REPLACE FUNCTION public.geodb_get_by_bbox(IN "table_name" text,
											  IN minx double precision,
											  IN miny double precision,
											  IN maxx double precision,
											  IN maxy double precision,
											  IN bbox_mode VARCHAR(255) DEFAULT 'within',
											  IN bbox_crs int DEFAULT 4326,
											  IN "limit" int DEFAULT 0,
											  IN "offset" int DEFAULT 0)
    RETURNS TABLE(src json)
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
	bbox_func VARCHAR;
    row_ct int;
	lmt_str text;
BEGIN
	CASE bbox_mode
		WHEN 'within' THEN
			bbox_func := 'ST_Within';
		WHEN 'contains' THEN
			bbox_func := 'ST_Contains';
		ELSE
			RAISE EXCEPTION 'bbox mode % does not exist. Use ''within'' | ''contains''', bbox_mode USING ERRCODE = 'data_exception';
	END CASE;

	lmt_str := '';

	IF "limit" > 0 THEN
		lmt_str := ' GROUP BY id ORDER BY id LIMIT ' || "limit";
	END IF;

	IF "offset" > 0 THEN
		lmt_str := lmt_str || ' OFFSET ' || "offset";
	END IF;

	RETURN QUERY EXECUTE format(
		'SELECT JSON_AGG(src) as js
		 FROM (SELECT * FROM "%s") as src
		 WHERE %s(''SRID=%s;POLYGON((' || minx
		                                           || ' ' || miny
		                                           || ', ' || minx
		                                           || ' ' || maxy
		                                           || ', ' || maxx
		                                           || ' ' || maxy
		                                           || ', ' || maxx
		                                           || ' ' ||  miny
		                                           || ', ' || minx
		                                           || ' ' || miny
		                                           || '))'', geometry)'
		                                           || ' ' || lmt_str, table_name, bbox_func, bbox_crs
	);

	GET DIAGNOSTICS row_ct = ROW_COUNT;

    IF row_ct < 1 THEN
	   RAISE EXCEPTION 'Only % rows! Requested minimum was %.', row_ct, min_ct;
    END IF;
END
$BODY$;
