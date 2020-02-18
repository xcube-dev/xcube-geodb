
DROP FUNCTION IF EXISTS public.geodb_get_raw(text, text);
CREATE OR REPLACE FUNCTION public.geodb_get_raw(IN collection text, IN qry text)
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


CREATE OR REPLACE FUNCTION public.geodb_get_pg(
    collection text,
    IN "select" text DEFAULT '*',
    IN "where" text DEFAULT NULL,
    IN "group" text DEFAULT NULL,
    IN "order" text DEFAULT NULL,
    IN "limit" integer DEFAULT NULL,
    IN "offset" integer DEFAULT NULL
)
    RETURNS TABLE(src json)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
DECLARE
    row_ct int;
    qry text;
BEGIN
    qry := format('SELECT %s FROM %I ', "select", "collection");

    IF "where" IS NOT NULL THEN
        qry := qry || format('WHERE %s ', "where");
    END IF;

    IF "group" IS NOT NULL THEN
        qry := qry || format('GROUP BY %s ', "group");
    END IF;

    IF "order" IS NOT NULL THEN
        qry := qry || format('ORDER BY %s ', "order");
    END IF;

    IF "limit" IS NOT NULL THEN
        qry := qry || format('LIMIT %s  ', "limit");
    END IF;

    IF "limit"  IS NOT NULL AND "offset"  IS NOT NULL THEN
        qry := qry || format('OFFSET %s ', "offset");
    END IF;

    RETURN QUERY EXECUTE format('SELECT JSON_AGG(src) as src FROM (%s) as src ', qry);

    GET DIAGNOSTICS row_ct = ROW_COUNT;

    IF row_ct < 1 THEN
        RAISE EXCEPTION 'Empty result';
    END IF;
END
$BODY$;



DROP FUNCTION IF EXISTS public.geodb_get_by_bbox(text, double precision, double precision, double precision, double precision, VARCHAR, int, int, int);
CREATE OR REPLACE FUNCTION public.geodb_get_by_bbox(IN collection text,
											  IN minx double precision,
											  IN miny double precision,
											  IN maxx double precision,
											  IN maxy double precision,
											  IN bbox_mode VARCHAR(255) DEFAULT 'within',
											  IN bbox_crs int DEFAULT 4326,
                                              IN "where" text DEFAULT 'id > 0'::text,
                                              IN op text DEFAULT 'AND'::text,
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
		 WHERE (%s) %s %s(''SRID=%s;POLYGON((' || minx
                                       || ' ' || miny
                                       || ', ' || maxx
                                       || ' ' || miny
                                       || ', ' || maxx
                                       || ' ' || maxy
                                       || ', ' || minx
                                       || ' ' ||  maxy
                                       || ', ' || minx
                                       || ' ' || miny
                                       || '))'', geometry) '
                                       || 'ORDER BY id '
                                       || lmt_str || ') as src',
        collection, "where", op, bbox_func, bbox_crs
	);

    RETURN QUERY EXECUTE qry;

	GET DIAGNOSTICS row_ct = ROW_COUNT;

    IF row_ct < 1 THEN
	   RAISE EXCEPTION 'Only % rows!', row_ct;
    END IF;
END
$BODY$;
