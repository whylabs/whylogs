from datetime import datetime, timedelta

from whylogs.api.store.profile_store import ProfileStore
from whylogs.api.store.query import DateQuery
from whylogs.core.constraints import ConstraintsBuilder
from whylogs.core.constraints.factories import greater_than_number

# 1. Configure Store
store = ProfileStore(base_name="test_constraints")
now_config = DateQuery(start_date=datetime.now())
reference_config = DateQuery(start_date=datetime.now() - timedelta(days=7), end_date=datetime.now())

# 2. Get the profile
profile_view = store.get(date_config=now_config)
reference_profile = store.get(date_config=reference_config)

# 3. Run constraints
builder = ConstraintsBuilder(dataset_profile_view=profile_view)

reference_mean = reference_profile.get_column("a").get_metric("distribution").avg
builder.add_constraint(greater_than_number(column_name="a", number=reference_mean))
constraints = builder.build()
print(constraints.report())
