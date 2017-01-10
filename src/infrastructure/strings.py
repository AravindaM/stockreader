
def remove_single_and_double_quotes(string):
    string = string.replace("'", "")
    string = string.replace('"', "")
    return string


def remove_single_quotes(string):
    return string.replace("'", "")
