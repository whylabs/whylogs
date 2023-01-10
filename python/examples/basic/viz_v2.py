from whylogs.viz import NotebookProfileVisualizer
import whylogs as why
import pandas as pd
from whylogs.core.datatypes import Fractional
from whylogs.drift.column_drift_algorithms import KS, Hellinger, ChiSquare
from whylogs.drift.configs import KSTestConfig, HellingerConfig, ChiSquareConfig
from whylogs.drift.column_drift_algorithms import calculate_drift_scores

data = {
    "animal": ["cat", "hawk", "snake", "cat"],
    "legs": [4, 2, 0, 4],
    "weight": [4.3, 1.8, None, 4.1],
}

df = pd.DataFrame(data)

target_view = why.log(df).profile().view()
ref_view = why.log(df).profile().view()

# visualization = NotebookProfileVisualizer()
# visualization.set_profiles(target_profile_view=target_view, reference_profile_view=ref_view)

# ks_config = KSTestConfig(
#     thresholds=[0.05, 0.15]
# )  # drift report will consider drift/possible-drift/no-drift based on the tresholds

chisquareconfig = ChiSquareConfig(thresholds=[0.01, 0.04])

hellingerconfig = HellingerConfig(max_hist_buckets=30)

# visualization.add_drift_config(column_names=["legs"], algorithm=ChiSquare(chisquareconfig))

# visualization.summary_drift_report()

# print("hello")

drift_map = {"animal": Hellinger(hellingerconfig), "weight": Hellinger(hellingerconfig)}
drift_scores = calculate_drift_scores(
    target_view=target_view, reference_view=ref_view, drift_map=drift_map, with_thresholds=True
)
print("hello")
