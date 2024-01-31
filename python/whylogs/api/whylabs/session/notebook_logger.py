import traceback
from typing import Any, Dict, List, Optional, Union

from whylogs.api.logger.result_set import ResultSet
from whylogs.api.whylabs.session.session import NotSupported
from whylogs.api.whylabs.session.session_manager import get_current_session
from whylogs.api.whylabs.session.session_types import InteractiveLogger as il
from whylogs.api.whylabs.session.session_types import SessionType
from whylogs.core.stubs import pd


def notebook_session_log_comparison(
    data: Dict[str, Union["pd.DataFrame", List[Dict[str, Any]]]], result_sets: Dict[str, ResultSet]
) -> None:
    session = get_current_session()

    # This api does show som
    if session is None:
        return
    elif session.get_type() == SessionType.LOCAL:
        return

    profile_names_zipped_with_loggable_len = zip(result_sets.keys(), [len(v) for v in data.values()])
    joined_profile_names_and_loggable_len = ", ".join(
        [f"{v} lines into profile '{k}'" for k, v in profile_names_zipped_with_loggable_len]
    )
    il.message()
    il.success(f"Aggregated {joined_profile_names_and_loggable_len}")
    il.message()

    try:
        result = session.upload_reference_profiles(result_sets)
        if not isinstance(result, NotSupported):
            il.message("Visualize and explore the profiles with one-click")
            il.inspect(result.viewing_url)

            urls = result.individual_viewing_urls
            if urls is not None:
                il.message()
                il.message("Or view each profile individually")
                for url in urls:
                    il.option(url)

    except Exception as e:
        # Don't throw, just don't upload
        il.failure(f"Failed to upload reference profile: {e}")
        traceback.print_exc()


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
    session = get_current_session()

    # Dont' do anything if there is no session created with why.init
    # For now, only include reference profile uploads (things that have a name)
    if session is None:
        return
    elif session.get_type() == SessionType.LOCAL:
        return

    # Get the length of whatever was just logged
    rows = _get_loggable_length(pandas) or _get_loggable_length(obj) or _get_loggable_length(row)

    il.message()
    if rows is not None:
        name_message = name or ""
        il.success(f"Aggregated {rows} rows into profile {name_message}")
    else:
        il.success(f"Aggregated data into profile {name_message}")

    il.message()

    if name is None:
        try:
            result = session.upload_batch_profile(result_set)
            if not isinstance(result, NotSupported):
                il.message("Visualize and explore this profile with one-click")
                il.inspect(result.viewing_url)
        except Exception as e:
            # Don't throw, just don't upload
            il.failure(f"Failed to upload profile: {e}")
            traceback.print_exc()

    else:
        profiles: Dict[str, ResultSet] = {}
        profiles[name] = result_set

        try:
            result = session.upload_reference_profiles(profiles)
            if not isinstance(result, NotSupported):
                il.message("Visualize and explore this profile with one-click")
                il.inspect(result.viewing_url)
        except Exception as e:
            # Don't throw, just don't upload
            il.failure(f"Failed to upload reference profile: {e}")
            traceback.print_exc()
