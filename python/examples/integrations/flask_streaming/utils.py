from typing import Optional, Tuple, Dict


def message_response(message: str, status_code: int = 200, reason: Optional[str] = None) -> tuple[
    dict[str, Optional[str]], int]:
    standard_response = {"data": None, "message": message, "reason": reason}
    return standard_response, status_code


def object_response(object: str, status_code: int = 200) -> tuple[dict[str, str], int]:
    standard_response = {"data": object, "message": "Success"}
    return standard_response, status_code


class MessageException(Exception):
    def __init__(self, message: str, status_code: int, reason: Optional[str] = None) -> None:
        Exception.__init__(self)
        self.message = message
        self.status_code = status_code
        self.reason = reason

    def to_dict(self) -> dict:
        rv = dict(())
        rv["message"] = self.message
        rv["status_code"] = str(self.status_code)
        rv["reason"] = str(self.reason)
        return rv


def message_exception_handler(err: MessageException) -> tuple[dict[str, Optional[str]], int]:
    return message_response(err.message, err.status_code, err.reason)
