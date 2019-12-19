GEODB_API_DEFAULT_CONNECTION_PARAMETERS = {
    # 'server_url': "http://ec2-3-120-53-215.eu-central-1.compute.amazonaws.com",
    'server_url': "http://10.3.0.63",
    'server_port': 3000
}

JSON_API_VALIDATIONS_CREATE_DATASET = {
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


TT = {
    'type': 'array',
    'properties': {
        'name': {"type": "string"},
        'type': {"type": "string"}

    }
}
