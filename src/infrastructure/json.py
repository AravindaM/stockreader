import inflection

def json_keys_to_lower_and_snake_case(json):
    if isinstance(json, list):
        return [json_keys_to_lower_and_snake_case(element) for element in json]
    elif isinstance(json, dict):
        return dict((inflection.underscore(key), json_keys_to_lower_and_snake_case(value)) for key, value in json.items())
    else:
        return json

def remove_single_quotes_from_json(json):
    if isinstance(json, list):
        return [remove_single_quotes_from_json(element) for element in json]
    elif isinstance(json, dict):
        return dict((key, remove_single_quotes_from_json(value)) for key, value in json.items())
    else:
        return json.replace("'", "")
