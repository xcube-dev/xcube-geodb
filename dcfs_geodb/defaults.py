GEODB_API_DEFAULT_PARAMETERS = {
    'server_url': "http://ec2-3-120-53-215.eu-central-1.compute.amazonaws.com",
    'server_port': 3000,
    'auth_domain': "https://xcube.eu.auth0.com",
    'auth_aud': "geodb",
    'auth_pub_client_id': 'QoZa2Vmg36x9NOEqyy4yRVueeITaeZ81',
    'auth_pub_client_secret': 'd4POTIHcKOor3qbMbs08Ow1NzHCR6UzvT31CkGapMVok-FZ7ScNffYyPBHlH3cPz',
    'auth0_config_file': 'ipyauth-auth0-demo.env'
}


GEOSERVER_DEFAULT_PARAMETERS = {
    'server_url': "http://ec2-3-120-53-215.eu-central-1.compute.amazonaws.com",
    'server_port': 3000,
    'auth_domain': "https://xcube.eu.auth0.com",
    'user_name': 'bla',
    'password': 'bla',
    'auth_aud': "geodb",
    'auth_pub_client_id': 'QoZa2Vmg36x9NOEqyy4yRVueeITaeZ81',
    'auth_pub_client_secret': 'd4POTIHcKOor3qbMbs08Ow1NzHCR6UzvT31CkGapMVok-FZ7ScNffYyPBHlH3cPz'
}


JSON_VALIDATIONS = {
    'validation': {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "type": {"type": "string", "format": "valid_types"}
        }
    },
    'formats': {
        "valid_types": lambda value: value in ("int", "float", "string", "date", "datetime", "bool"),
    }
}

