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

