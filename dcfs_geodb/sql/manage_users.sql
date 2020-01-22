-- noinspection SqlSignatureForFile

CREATE OR REPLACE FUNCTION public.geodb_whoami()
    RETURNS text
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN current_user;
END
$BODY$;

CREATE OR REPLACE FUNCTION public.geodb_register_user(IN user_name text, IN password text)
    RETURNS character varying(255)
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('CREATE ROLE %s LOGIN; ALTER ROLE %s PASSWORD ''%s'';ALTER ROLE %s SET search_path = public;' ||
                   'GRANT %s TO authenticator;', user_name, user_name, password, user_name, user_name);
    RETURN 'success';
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_register_user(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION geodb_register_user(text, text) TO geodb_admin;


CREATE OR REPLACE FUNCTION public.geodb_user_exists(IN user_name text)
    RETURNS TABLE(exts boolean)
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    RETURN QUERY EXECUTE format('SELECT EXISTS (SELECT true FROM pg_roles WHERE rolname=''%s'')', user_name);
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_user_exists(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION geodb_user_exists(text) TO geodb_admin;


CREATE OR REPLACE FUNCTION public.geodb_drop_user(IN user_name text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('DROP ROLE IF EXISTS %s', user_name);
    RETURN true;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_drop_user(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION geodb_drop_user(text) TO geodb_admin;

CREATE OR REPLACE FUNCTION public.geodb_grant_user_admin(IN user_name text)
    RETURNS boolean
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    EXECUTE format('GRANT geodb_admin TO %I', user_name);
    RETURN true;
END
$BODY$;

REVOKE EXECUTE ON FUNCTION geodb_grant_user_admin(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION geodb_grant_user_admin(text) TO geodb_admin;


CREATE OR REPLACE FUNCTION geodb_check_user() RETURNS void AS $$
BEGIN
    IF current_user = 'anonymous' THEN
        RAISE EXCEPTION 'Anonymous users do not have access to dev'
            USING HINT = 'Please ask Brockmann Consult for access. (geodb@brockmann-consult.de)';
    END IF;
END
$$ LANGUAGE plpgsql;
