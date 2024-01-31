import getpass
import sys
from typing import List, Optional

from whylogs.api.whylabs.session.session_types import ApiKey
from whylogs.api.whylabs.session.session_types import InteractiveLogger as il
from whylogs.api.whylabs.session.session_types import (
    SessionType,
    parse_api_key,
    validate_org_id,
)


def _get_user_choice(prompt: str, options: List[str]) -> int:
    il.question(prompt, ignore_suppress=True)
    for i, option in enumerate(options, 1):
        il.option(f"{i}. {option}", ignore_suppress=True)

    while True:
        try:
            sys.stdout.flush()
            il.message(ignore_suppress=True)
            choice = int(input("Enter a number from the list: "))
            if 1 <= choice <= len(options):
                return choice
        except ValueError:
            pass


def prompt_session_type(allow_anonymous: bool = True, allow_local: bool = False) -> SessionType:
    options = ["WhyLabs. Use an api key to upload to WhyLabs."]

    if allow_anonymous:
        options.append("WhyLabs Anonymous. Upload data anonymously to WhyLabs and get a viewing url.")

    if allow_local:
        options.append("Local. Don't upload data anywhere.")

    if len(options) == 1:
        return SessionType.WHYLABS

    choice = _get_user_choice("What kind of session do you want to use?", options)
    return [SessionType.WHYLABS, SessionType.WHYLABS_ANONYMOUS, SessionType.LOCAL][choice - 1]


def prompt_default_dataset_id() -> Optional[str]:
    try:
        sys.stdout.flush()
        il.message(ignore_suppress=True)
        default_dataset_id = input("[OPTIONAL] Enter a default dataset id to upload to: ").strip()
        return default_dataset_id
    except Exception:
        return None


def prompt_api_key() -> ApiKey:
    while True:
        try:
            sys.stdout.flush()
            il.message(ignore_suppress=True)
            api_key = getpass.getpass(
                "Enter your WhyLabs api key. You can find it at https://hub.whylabsapp.com/settings/access-tokens: "
            )
            return parse_api_key(api_key)
        except Exception:
            il.warning(
                f"Couldn't parse the api key. Expected a key with the format 'key_id.key:org_id'. Got: {api_key}",
                ignore_suppress=True,
            )


def prompt_org_id() -> str:
    while True:
        try:
            sys.stdout.flush()
            il.message(ignore_suppress=True)
            org_id = input("Enter your org id. You can find it at https://hub.whylabsapp.com/settings/access-tokens: ")
            validate_org_id(org_id)
            return org_id
        except Exception:
            il.warning(
                f"Couldn't parse the org id. Expected an id that starts with 'org-'. Got: {org_id}",
                ignore_suppress=True,
            )
