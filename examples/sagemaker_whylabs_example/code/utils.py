

def message_response(message, status_code=200, reason=None):
    standard_response = {"data": None, "message": message, "reason": reason}
    return standard_response, status_code


def object_response(object, status_code=200):
    standard_response = {"data": object, "message": "Success"}
    return standard_response, status_code


class MessageException(Exception):
    def __init__(self, message, status_code, reason=None):
        Exception.__init__(self)
        self.message = message
        self.status_code = status_code
        self.reason = reason

    def to_dict(self):
        rv = dict(())
        rv["message"] = self.message
        rv["status_code"] = self.status_code
        rv["reason"] = self.reason
        return rv


def message_exception_handler(err):
    return message_response(err.message, err.status_code, err.reason)
