GEODB_API_DEFAULT_PARAMETERS = {
    'server_url': "http://ec2-3-120-53-215.eu-central-1.compute.amazonaws.com",
    'server_port': 3000,
    'auth_domain': "https://xcube.eu.auth0.com",
    'auth_aud': "https://geodb.brockmann-consult.de",
    'auth0_config_file': 'ipyauth-auth0-demo.env'
}


GEOSERVER_DEFAULT_PARAMETERS = {
    'server_url': "http://ec2-3-120-53-215.eu-central-1.compute.amazonaws.com",
    'server_port': 3000,
    'auth_domain': "https://xcube.eu.auth0.com",
    'user_name': 'bla',
    'password': 'bla',
    'auth_aud': "https://geodb.brockmann-consult.de",
}


GEODB_DEFAULTS = {
    "server_url": "https://xcube-geodb.brockmann-consult.de",
    "server_port": 443,
    "auth_client_id": None,
    "auth_client_secret": None,
    "auth_username": None,
    "auth_password": None,
    "auth_access_token": None,
    "auth0_config_file": None,
    "auth0_config_folder": '.',
    "auth_domain": "https://edc.eu.auth0.com",
    "auth_mode": "client_credentials",
    "auth_aud": "https://geodb.brockmann-consult.de",
    "auth_access_token_uri": "/oauth/token"
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

