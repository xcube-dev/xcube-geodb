CREATE OR REPLACE FUNCTION public.geodb_filter_raw(IN collection text, IN qry text)
    RETURNS TABLE(src json)
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE
    row_ct int;
BEGIN
    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as src FROM (SELECT * FROM %I WHERE %s) as src ',
        collection, qry);

    GET DIAGNOSTICS row_ct = ROW_COUNT;

    IF row_ct < 1 THEN
        RAISE EXCEPTION 'Empty result';
    END IF;
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_filter_by_bbox(IN collection text,
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
    qry text;
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
		lmt_str := ' LIMIT ' || "limit";
	END IF;

	IF "offset" > 0 THEN
		lmt_str := lmt_str || ' OFFSET ' || "offset";
	END IF;

	qry := format(
		'SELECT JSON_AGG(src) as js
		 FROM (SELECT * FROM %I
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
                                       || '))'', geometry) '
                                       || 'ORDER BY id '
                                       || lmt_str || ') as src',
        collection, bbox_func, bbox_crs
	);

    RETURN QUERY EXECUTE qry;

	GET DIAGNOSTICS row_ct = ROW_COUNT;

    IF row_ct < 1 THEN
	   RAISE EXCEPTION 'Only % rows!', row_ct;
    END IF;
END
$BODY$;
