INSERT INTO geodb_collection_metadata.basic ("collection_name", "database", "title", "description", "spatial_extent",
                                             "keywords", "summaries")
VALUES ('land_use', 'geodb_user', 'Land Use',
        'Sample table',
        ARRAY [ST_GeomFromText('POLYGON((-170 -80, -170 80, 170 80, -170 -80, -170 -80))'), ST_GeomFromText('POLYGON((-169 -79, -169 79, 169 79, -169 -79, -169 -79))')],
        ARRAY ['land', 'use'],
        '{
          "columns": [
            "id",
            "geometry"
          ],
          "x_range": {
            "min": "-170",
            "max": "170"
          },
          "y_range": {
            "min": "-80",
            "max": "80"
          },
          "schema": "this is a complex schema stored in a string"
        }'::JSONB);

INSERT INTO geodb_collection_metadata.link("href", "rel", "title", "collection_name", "database")
VALUES ('https://something.sth', 'self', 'some_link', 'land_use', 'geodb_user');

INSERT INTO geodb_collection_metadata.link("href", "rel", "title", "collection_name", "database")
VALUES ('https://something.else', 'root', 'some_other_link', 'land_use', 'geodb_user');

INSERT INTO geodb_collection_metadata.provider("name", "description", "url", "collection_name", "database")
VALUES ('some_provider', 'i am the best provider!', 'https://best-provider.com', 'land_use', 'geodb_user');

INSERT INTO geodb_collection_metadata.provider("name", "description", "url", "collection_name", "database")
VALUES ('some_other_provider', 'i am the worst provider!', 'https://worst-provider.com', 'land_use', 'geodb_user');

INSERT INTO geodb_collection_metadata.provider("name", "description", "roles", "url", "collection_name", "database")
VALUES ('another_provider', 'i am an ok provider!', ARRAY ['producer'::geodb_collection_metadata.provider_role,
    'host'::geodb_collection_metadata.provider_role],
        'https://ok-provider.com',
        'land_use', 'geodb_user');

INSERT INTO geodb_collection_metadata.asset("href", "collection_name", "database")
VALUES ('https://best-assets.bc', 'land_use', 'geodb_user');

INSERT INTO geodb_collection_metadata."item_asset"("type", "collection_name", "database")
VALUES ('I have a type', 'land_use', 'geodb_user');

INSERT INTO geodb_collection_metadata.basic ("collection_name", "database", "title")
VALUES ('some_other_collection', 'some_database', 'I am some other collection');