import os

PG_DEFAULT_CONNECTION_PARAMETERS = {
    'host': 'db-dcfs-geodb.cbfjgqxk302m.eu-central-1.rds.amazonaws.com',
    'database': 'postgres',
    'user': "postgres",
    'password': os.environ.get("PG_GEODB_PASSWORD")
}

DEFAULT_TABLE_EXISTS_SQL = """SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables
            WHERE  table_schema = 'public'
            AND    table_name = '{table}')
            """

DEFAULT_LOAD_SQL = "SELECT id, raba_pid, raba_id, d_od, geometry FROM land_use WHERE {where}"

DEFAULT_LOAD_AGG_SQL = """
            SELECT land_use.id, mean, std, raba_pid, raba_id, d_od, geometry  
            FROM land_use
            RIGHT JOIN land_use_agg_{variable}
            ON land_use_agg_{variable}.fk_land_use_id = land_use.id
            WHERE {where}
            """

DEFAULT_DROP_AGG_SQL = "DROP TABLE land_use_agg_{variable}"

DEFAULT_DELETE_SQL = "DELETE FROM {table} WHERE {where}"

DEFAULT_WRITE_SQL = """INSERT INTO {table} (RABA_PID, RABA_ID, D_OD, GEOMETRY)
                        VALUES ({raba_pid}, {raba_id}, '{d_od}', ST_GeometryFromText('{wkt}', 4326))
                    """

DEFAULT_WRITE_SQL_BATCH = """INSERT INTO {table} (RABA_PID, RABA_ID, D_OD, GEOMETRY)
                        VALUES (%s, %s, %s, ST_GeometryFromText('%s', 4326))
                    """


DEFAULT_WRITE_AGG_SQL = """INSERT INTO {table} (fk_land_use_id, mean, std)
                           VALUES({land_use_id}, {mean}, {std})
                        """

DEFAULT_WRITE_AGG_SQL_BATCH = """INSERT INTO {table} (fk_land_use_id, mean, std)
                           VALUES(%s, %s, %s)
                        """


DEFAULT_QUERY_BBOX_SQL = """SELECT * FROM {table}
                            WHERE {func}('SRID=4326;{bbox}', geometry)
                        """

DEFAULT_QUERY_BBOX_SQL_AGG = """SELECT * 
                                FROM land_use
                                RIGHT JOIN land_use_agg_{variable}
                                ON land_use_agg_{variable}.fk_land_use_id = land_use.id
                                WHERE {func}('SRID=4326;{bbox}', land_use.geometry)
                            """


DEFAULT_CREATE_AGG_SQL = """
            -- Table: public.land_use_agg_{variable}
            
            -- DROP TABLE public.land_use_agg_{variable};
            
            CREATE TABLE public.land_use_agg_{variable}
            (
                -- Inherited from table public.land_use_agg_master: id integer NOT NULL DEFAULT nextval('land_user_id_seq'::regclass),
                -- Inherited from table public.land_use_agg_master: mean double precision,
                -- Inherited from table public.land_use_agg_master: fk_land_use_id bigint NOT NULL,
                -- Inherited from table public.land_use_agg_master: std double precision
            )
                INHERITS (public.land_use_agg_master)
            WITH (
                OIDS = FALSE
            )
            TABLESPACE pg_default;
            
            ALTER TABLE public.land_use_agg_{variable}
                OWNER to postgres;
            """

DEFAULT_QUERY_SQL = "SELECT COUNT(*) FROM land_use"
