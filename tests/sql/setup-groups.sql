CREATE ROLE "test_group" WITH
    NOLOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION;

CREATE ROLE "test_admin" WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION;

CREATE ROLE "test_noadmin" WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION;

CREATE ROLE "test_member" WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION;

CREATE ROLE "test_member_2" WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION;

CREATE ROLE "test_nomember" WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOREPLICATION;

GRANT "test_admin" TO postgres;
GRANT "test_member" TO postgres;
GRANT "test_member_2" TO postgres;
GRANT "test_nomember" TO postgres;

GRANT "test_admin" TO authenticator;
GRANT "test_member" TO authenticator;
GRANT "test_member_2" TO authenticator;
GRANT "test_nomember" TO authenticator;

GRANT ALL ON SCHEMA public TO "test_admin";
GRANT ALL ON SCHEMA public TO "test_member";
GRANT ALL ON SCHEMA public TO "test_member_2";
GRANT ALL ON SCHEMA public TO "test_nomember";

GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.geodb_user_databases TO "test_admin";
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.geodb_user_databases TO "test_member";
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.geodb_user_databases TO "test_member_2";
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE public.geodb_user_databases TO "test_nomember";
GRANT SELECT, UPDATE, USAGE ON SEQUENCE public.geodb_user_databases_seq TO "test_admin";
GRANT SELECT, UPDATE, USAGE ON SEQUENCE public.geodb_user_databases_seq TO "test_member";
GRANT SELECT, UPDATE, USAGE ON SEQUENCE public.geodb_user_databases_seq TO "test_member_2";
GRANT SELECT, UPDATE, USAGE ON SEQUENCE public.geodb_user_databases_seq TO "test_nomember";

GRANT "test_group" TO "test_admin" WITH ADMIN OPTION;

GRANT EXECUTE ON FUNCTION geodb_create_role TO "test_admin";

GRANT EXECUTE ON FUNCTION geodb_create_database TO "test_member";
GRANT EXECUTE ON FUNCTION geodb_create_collection TO "test_member";
GRANT EXECUTE ON FUNCTION geodb_get_grants TO "test_member";
GRANT EXECUTE ON FUNCTION geodb_user_allowed TO "test_member";
GRANT EXECUTE ON FUNCTION geodb_add_properties TO "test_member";
GRANT EXECUTE ON FUNCTION geodb_group_publish_collection TO "test_member";
GRANT EXECUTE ON FUNCTION geodb_group_publish_database TO "test_member";
GRANT EXECUTE ON FUNCTION geodb_group_unpublish_database TO "test_member";
GRANT EXECUTE ON FUNCTION geodb_group_unpublish_collection TO "test_member";

GRANT EXECUTE ON FUNCTION geodb_create_collection TO "test_member_2";
GRANT EXECUTE ON FUNCTION geodb_user_allowed TO "test_member_2";
GRANT EXECUTE ON FUNCTION geodb_group_revoke TO "test_member_2";
GRANT EXECUTE ON FUNCTION geodb_group_grant TO "test_member_2";
GRANT EXECUTE ON FUNCTION geodb_add_properties TO "test_member_2";


INSERT INTO geodb_user_info
VALUES (100, 'test_admin', '2020-12-08', 'geodb-manage', '');
INSERT INTO geodb_user_info
VALUES (101, 'test_noadmin', '2020-12-08', 'freetrial', '');