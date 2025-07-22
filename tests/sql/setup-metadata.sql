INSERT INTO geodb_collection_metadata.metadata ("collection_name", "title", "description", "spatial_extent",
                                                "keywords", "summaries")
VALUES ('geodb_user_land_use', 'Land Use',
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

INSERT INTO geodb_collection_metadata.link("href", "rel", "title", "collection_name")
VALUES ('https://something.sth', 'self', 'some_link', 'geodb_user_land_use');

INSERT INTO geodb_collection_metadata.link("href", "rel", "title", "collection_name")
VALUES ('https://something.else', 'root', 'some_other_link', 'geodb_user_land_use');

INSERT INTO geodb_collection_metadata.provider("name", "description", "url", "collection_name")
VALUES ('some_provider', 'i am the best provider!', 'https://best-provider.com', 'geodb_user_land_use');

INSERT INTO geodb_collection_metadata.provider("name", "description", "url", "collection_name")
VALUES ('some_other_provider', 'i am the worst provider!', 'https://worst-provider.com', 'geodb_user_land_use');

INSERT INTO geodb_collection_metadata.provider("name", "description", "roles", "url", "collection_name")
VALUES ('another_provider', 'i am an ok provider!', ARRAY ['producer'::geodb_collection_metadata.provider_role,
    'host'::geodb_collection_metadata.provider_role],
        'https://ok-provider.com',
        'geodb_user_land_use');

INSERT INTO geodb_collection_metadata.asset("name", "href", "collection_name")
VALUES ('some_asset', 'https://best-assets.bc', 'geodb_user_land_use');

INSERT INTO geodb_collection_metadata."item_asset"("name", "type", "collection_name")
VALUES ('some_item_asset', 'I have a type', 'geodb_user_land_use');

INSERT INTO geodb_collection_metadata.metadata ("collection_name", "title")
VALUES ('some_other_collection', 'I am some other collection');