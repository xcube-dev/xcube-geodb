GEODB_API_DEFAULT_CONNECTION_PARAMETERS = {
    'server_url': "http://ec2-3-120-53-215.eu-central-1.compute.amazonaws.com",
    'server_port': 3000
}

JSON_API_VALIDATIONS_CREATE_DATASET = {
    'validation': {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "type": {"type": "string", "format": "properties"}
        }
    },
    'formats': {
        "properties": lambda value: value in ("int", "float", "string", "date", "datetime", "bool"),
    }
}
