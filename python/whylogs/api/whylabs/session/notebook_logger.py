from typing import Any, Dict, List, Optional, Union

from whylogs.api.logger.result_set import ResultSet
from whylogs.api.whylabs.session.session_manager import (
    NotSupported,
    get_current_session,
)
from whylogs.api.whylabs.session.session_types import log_if_notebook
from whylogs.core.stubs import pd

_start_session_tip = "Start a session with why.init(session_type='whylabs_anonymous')"


def notebook_session_log_comparison(
    data: Dict[str, Union["pd.DataFrame", List[Dict[str, Any]]]], result_sets: Dict[str, ResultSet]
) -> None:
    session = get_current_session()

    # This api does show som
    if session is None:
        log_if_notebook(f"😭 No active session found. Skipping reference profile upload. {_start_session_tip}")
        return
    elif session.get_type().value != "whylabs_anonymous":
        # Only supported for anonymous session right now
        return

    profile_names_zipped_with_loggable_len = zip(result_sets.keys(), [len(v) for v in data.values()])
    joined_profile_names_and_loggable_len = ", ".join(
        [f"{v} lines into profile '{k}'" for k, v in profile_names_zipped_with_loggable_len]
    )
    log_if_notebook(f"✅ Aggregated {joined_profile_names_and_loggable_len}")
    log_if_notebook()

    try:
        result = session.upload_reference_profiles(result_sets)
        if not isinstance(result, NotSupported):
            log_if_notebook("Visualize and explore the profiles with one-click")
            log_if_notebook(f"🔍 {result.viewing_url}")

            log_if_notebook()
            log_if_notebook("Or view each profile individually")
            for item in result.whylabs_response.references:
                print(f" ⤷ {item.observatory_url}")

    except Exception as e:
        # Don't throw, just don't upload
        log_if_notebook(f"Failed to upload reference profile: {e}")


def _get_loggable_length(loggable: Optional[Union["pd.DataFrame", Dict[str, Any]]]) -> Optional[int]:
    if isinstance(loggable, pd.DataFrame):
        return len(loggable)
    elif isinstance(loggable, dict):
        return 1
    else:
        return None


def notebook_session_log(
    result_set: ResultSet,
    obj: Any = None,
    *,
    pandas: Optional[pd.DataFrame] = None,
    row: Optional[Dict[str, Any]] = None,
    name: Optional[str] = None,
) -> None:
    # This is only for reference profiles atm
    if name is None:
        log_if_notebook("Skipping uploading profile to WhyLabs because no name was given with name=")
        return

    session = get_current_session()

    # Dont' do anything if there is no session created with why.init
    # For now, only include reference profile uploads (things that have a name)
    if session is None:
        log_if_notebook(f"😭 No active session found. Skipping reference profile upload. {_start_session_tip}")
        return
    elif session.get_type().value != "whylabs_anonymous":
        # Only supported for anonymous session right now
        return

    # Get the length of whatever was just logged
    rows = _get_loggable_length(pandas) or _get_loggable_length(obj) or _get_loggable_length(row)

    if rows is not None:
        log_if_notebook(f"✅ Aggregated {rows} rows into profile '{name}'")
    else:
        log_if_notebook(f"✅ Aggregated data into profile '{name}'")

    log_if_notebook()

    profiles: Dict[str, ResultSet] = {}
    profiles[name] = result_set

    try:
        result = session.upload_reference_profiles(profiles)
        if not isinstance(result, NotSupported):
            log_if_notebook("Visualize and explore this profile with one-click")
            log_if_notebook(f"🔍 {result.viewing_url}")
    except Exception as e:
        # Don't throw, just don't upload
        log_if_notebook(f"Failed to upload reference profile: {e}")
