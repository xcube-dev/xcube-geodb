-- noinspection SqlSignatureForFile

CREATE OR REPLACE FUNCTION public.geodb_whoami()
    RETURNS text
    LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    usr text;
    scp text;
BEGIN
    scp := current_setting('request.jwt.claim.scope', true);
    usr := (SELECT src[ 2 ] FROM string_to_array(scp, ':') as src);
    RETURN usr;
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_register_user(IN user_name text)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('CREATE ROLE %s NOLOGIN; GRANT %s TO authenticator;', user_name, user_name);
    RETURN 'success';
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_user_exists(IN user_name text)
    RETURNS TABLE(exts boolean)
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE format('SELECT EXISTS (SELECT true FROM pg_roles WHERE rolname=''%s'')', user_name);
END
$BODY$;


CREATE OR REPLACE FUNCTION public.geodb_drop_user(IN user_name text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('DROP ROLE IF EXISTS %s', user_name);
    RETURN true;
END
$BODY$;


