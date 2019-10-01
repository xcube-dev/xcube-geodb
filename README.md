# dcfs-geodb

Geo DB (incubator) for DCFS

## GeoDB Instance

The PostGreSQL GeoDB is an Amazon AWS RDS instance. 

- PostgreSQl 11
- Region: eu-central-1
- AMI: db.m4.large


## Schema

```sql
    CREATE TABLE "public"."land_use" (
    "id" integer DEFAULT nextval('land_user_id_seq') NOT NULL,
    "raba_id" integer NOT NULL,
    "d_od" date NOT NULL,
    "geometry" geometry NOT NULL,
    "raba_pid" double precision NOT NULL
) WITH (oids = false);
```
