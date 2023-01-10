import warnings
from typing import Dict, List, Optional, Union, Callable, Any
from abc import ABC
import numpy as np
from scipy import stats  # type: ignore
from scipy.spatial.distance import euclidean
from typing_extensions import TypedDict
from whylogs_sketching import kll_doubles_sketch  # type: ignore
from dataclasses import dataclass
from enum import Enum
from whylogs.core.view.column_profile_view import ColumnProfileView  # type: ignore
from whylogs.core.view.dataset_profile_view import DatasetProfileView  # type: ignore
from whylogs.drift.configs import HellingerConfig, KSTestConfig, ChiSquareConfig
from whylogs.viz.utils import _calculate_bins
from whylogs.viz.utils.frequent_items_calculations import (
    FrequentStats,
    get_frequent_stats,
    zero_padding_frequent_items,
)


@dataclass
class DriftAlgorithmScore:
    algorithm: str
    pvalue: Optional[float] = None
    statistic: Optional[float] = None
    thresholds: Optional[List[float]] = None

    def to_dict(self):
        score_dict = {
            "algorithm": self.algorithm,
            "pvalue": self.pvalue,
            "statistic": self.statistic,
            "thresholds": self.thresholds,
        }
        return score_dict


class ColumnDriftAlgorithm(ABC):
    def __init__(self, parameter_config: Optional[Any] = None):
        self._parameter_config = parameter_config

    def calculate(
        self, target_column_view: ColumnProfileView, reference_column_view: ColumnProfileView
    ) -> DriftAlgorithmScore:
        raise NotImplementedError

    def set_parameters(self, parameter_config: Any):
        raise NotImplementedError


class Hellinger(ColumnDriftAlgorithm):
    def __init__(self, parameter_config: Optional[KSTestConfig] = None):
        self.name = "hellinger"
        if parameter_config is None:
            parameter_config = HellingerConfig()
        super().__init__(parameter_config)

    def _calculate_hellinger_score(self, target_pmf: List[float], reference_pmf: List[float]) -> float:
        """Calculates hellinger distance between two discrete probability distributions.

        Parameters
        ----------
        target_pmf : List[float]
            Target discrete probability distribution.
        reference_pmf : List[float]
            Reference discrete probability distribution.

        Returns
        -------
        float
            The hellinger distance between the two discrete probability distributions.
            Between 0 (identical distributions) and 1 (completely different distributions).
        """
        # https://en.wikipedia.org/wiki/Hellinger_distance
        distance = euclidean(np.sqrt(target_pmf), np.sqrt(reference_pmf)) / np.sqrt(2)
        return distance

    def calculate(
        self, target_column_view: ColumnProfileView, reference_column_view: ColumnProfileView, with_thresholds=False
    ) -> DriftAlgorithmScore:
        MAX_HIST_BUCKETS = self._parameter_config.max_hist_buckets
        HIST_AVG_NUMBER_PER_BUCKET = self._parameter_config.hist_avg_number_per_bucket
        MIN_N_BUCKETS = self._parameter_config.min_n_buckets
        if MIN_N_BUCKETS < 2:
            warnings.warn(
                "MIN_N_BUCKETS < 2 might lead to erroneous results for low-sized samples. Consider setting it to >=2."
            )
        nbins = MAX_HIST_BUCKETS
        target_dist_metric = target_column_view.get_metric("distribution")
        ref_dist_metric = reference_column_view.get_metric("distribution")

        if target_dist_metric is None or ref_dist_metric is None:
            warnings.warn("Column must have a Distribution Metric assigned to it.")
            return None

        target_kll_sketch = target_dist_metric.kll.value
        ref_kll_sketch = ref_dist_metric.kll.value

        if target_kll_sketch.is_empty() or ref_kll_sketch.is_empty():
            warnings.warn("Distribution sketch must not be empty.")
            return None

        start = min([target_kll_sketch.get_min_value(), ref_kll_sketch.get_min_value()])
        end = max([target_kll_sketch.get_max_value(), ref_kll_sketch.get_max_value()])
        n = target_kll_sketch.get_n() + ref_kll_sketch.get_n()
        bins, end = _calculate_bins(
            end=end,
            start=start,
            n=n,
            avg_per_bucket=HIST_AVG_NUMBER_PER_BUCKET,
            max_buckets=nbins,
            min_n_buckets=MIN_N_BUCKETS,
        )

        target_pmf = target_kll_sketch.get_pmf(bins)
        ref_pmf = ref_kll_sketch.get_pmf(bins)
        distance = self._calculate_hellinger_score(target_pmf=target_pmf, reference_pmf=ref_pmf)
        if with_thresholds:
            return DriftAlgorithmScore(
                algorithm=self.name, statistic=distance, thresholds=self._parameter_config.thresholds
            )
        else:
            return DriftAlgorithmScore(algorithm=self.name, statistic=distance)


class ChiSquare(ColumnDriftAlgorithm):
    def __init__(self, parameter_config: Optional[KSTestConfig] = None):
        if parameter_config is None:
            parameter_config = ChiSquareConfig()
        self.name = "chi-square"
        super().__init__(parameter_config)

    def _compute_chi_squared_score(
        self, target_distribution: FrequentStats, reference_distribution: FrequentStats, with_thresholds=False
    ) -> Optional[DriftAlgorithmScore]:
        """
        Calculate the Chi-Squared test p-value for two discrete distributions.

        Uses the top frequent items summary, unique count estimate and total count estimate for each feature,
        to calculate the estimated Chi-Squared statistic.
        Applicable only to discrete distributions.

        Parameters
        ----------
        target_distribution : ReferenceDistributionDiscreteMessage
            The summary message of the target feature's distribution.
            Should be a ReferenceDistributionDiscreteMessage containing the frequent items,
            unique, and total count summaries.
        reference_distribution : ReferenceDistributionDiscreteMessage
            The summary message of the reference feature's distribution.
            Should be a ReferenceDistributionDiscreteMessage containing the frequent items,
            unique, and total count summaries.

        Returns
        -------
            p_value : ColumnDriftValue or None. ColumnDriftValue has fields `p-value` with the test's result,
            and the name of the test applied in the `test` field (chi-squared).
            The estimated p-value from the Chi-Squared test, applied on the target and reference distributions'
            frequent and unique items summaries
        """
        target_freq_items = target_distribution["frequent_items"]
        ref_freq_items = reference_distribution["frequent_items"]
        target_total_count = target_distribution["total_count"]
        target_unique_count = target_distribution["unique_count"]
        ref_total_count = reference_distribution["total_count"]

        if ref_total_count <= 0 or target_total_count <= 0:
            return None

        target_freq_items, ref_freq_items = zero_padding_frequent_items(target_freq_items, ref_freq_items)

        ref_dist_items = dict()
        for item in reference_distribution["frequent_items"]:
            ref_dist_items[item["value"]] = item["estimate"]
        proportion_ref_dist_items = {k: v / ref_total_count for k, v in ref_dist_items.items()}

        chi_sq = 0.0
        for item in target_freq_items:
            target_frequency = item["estimate"]
            if item["value"] in ref_dist_items:
                ref_frequency = int(proportion_ref_dist_items[item["value"]] * target_total_count)
            else:
                ref_frequency = 0

            if ref_frequency == 0:
                chi_sq = np.inf
                break
            chi_sq += (target_frequency - ref_frequency) ** 2 / ref_frequency

        degrees_of_freedom = target_unique_count - 1
        degrees_of_freedom = degrees_of_freedom if degrees_of_freedom > 0 else 1
        p_value = stats.chi2.sf(chi_sq, degrees_of_freedom)
        if with_thresholds:
            drift_score = DriftAlgorithmScore(
                algorithm=self.name, pvalue=p_value, statistic=chi_sq, thresholds=self._parameter_config.thresholds
            )
        else:
            drift_score = DriftAlgorithmScore(algorithm=self.name, pvalue=p_value, statistic=chi_sq)

        return drift_score

    def calculate(
        self, target_column_view: ColumnProfileView, reference_column_view: ColumnProfileView, with_thresholds=False
    ) -> DriftAlgorithmScore:
        target_frequent_stats: FrequentStats = get_frequent_stats(target_column_view, config=None)
        ref_frequent_stats: FrequentStats = get_frequent_stats(reference_column_view, config=None)

        if not target_frequent_stats or not ref_frequent_stats:
            return None

        chi2_p_value = self._compute_chi_squared_score(
            target_distribution=target_frequent_stats,
            reference_distribution=ref_frequent_stats,
            with_thresholds=with_thresholds,
        )
        return chi2_p_value


class KS(ColumnDriftAlgorithm):
    def __init__(self, parameter_config: Optional[KSTestConfig] = None):
        self.name = "ks"
        if parameter_config is None:
            parameter_config = KSTestConfig()
        super().__init__(parameter_config)

    def _compute_ks_score(self, target_distribution, reference_distribution, with_thresholds=False):
        QUANTILES = self._parameter_config.quantiles
        if reference_distribution.is_empty() or target_distribution.is_empty():
            return None

        D_max = 0
        target_quantile_values = target_distribution.get_quantiles(QUANTILES)
        ref_quantile_values = reference_distribution.get_quantiles(QUANTILES)

        num_quantiles = len(QUANTILES)
        i, j = 0, 0
        while i < num_quantiles and j < num_quantiles:
            if target_quantile_values[i] < ref_quantile_values[j]:
                current_quantile = target_quantile_values[i]
                i += 1
            else:
                current_quantile = ref_quantile_values[j]
                j += 1

            cdf_target = target_distribution.get_cdf([current_quantile])[0]
            cdf_ref = reference_distribution.get_cdf([current_quantile])[0]
            D = abs(cdf_target - cdf_ref)
            if D > D_max:
                D_max = D

        while i < num_quantiles:
            cdf_target = target_distribution.get_cdf([target_quantile_values[i]])[0]
            cdf_ref = reference_distribution.get_cdf([target_quantile_values[i]])[0]
            D = abs(cdf_target - cdf_ref)
            if D > D_max:
                D_max = D
            i += 1

        while j < num_quantiles:
            cdf_target = target_distribution.get_cdf([ref_quantile_values[j]])[0]
            cdf_ref = reference_distribution.get_cdf([ref_quantile_values[j]])[0]
            D = abs(cdf_target - cdf_ref)
            if D > D_max:
                D_max = D
            j += 1

        m, n = sorted([target_distribution.get_n(), reference_distribution.get_n()], reverse=True)
        en = m * n / (m + n)
        p_value = stats.distributions.kstwo.sf(D_max, np.round(en))
        if with_thresholds:
            drift_score = DriftAlgorithmScore(
                algorithm=self.name, pvalue=p_value, statistic=D_max, thresholds=self._parameter_config.thresholds
            )
        else:
            drift_score = DriftAlgorithmScore(algorithm=self.name, pvalue=p_value, statistic=D_max)

        return drift_score

    def calculate(
        self, target_column_view: ColumnProfileView, reference_column_view: ColumnProfileView, with_thresholds=False
    ) -> DriftAlgorithmScore:
        target_dist_metric = target_column_view.get_metric("distribution")
        ref_dist_metric = reference_column_view.get_metric("distribution")

        if target_dist_metric is None or ref_dist_metric is None:
            return None

        target_kll_sketch = target_dist_metric.kll.value
        ref_kll_sketch = ref_dist_metric.kll.value

        ks_score: DriftAlgorithmScore = self._compute_ks_score(
            target_kll_sketch,
            ref_kll_sketch,
            with_thresholds=with_thresholds,
        )
        return ks_score

    def set_parameters(self, parameter_config: Any):
        if not isinstance(parameter_config, KSTestConfig):
            raise ValueError("Parameter config should be of type KSTestConfig")
        self._parameter_config = parameter_config


def _get_drift_score(
    column_name: str,
    target_column_view: ColumnProfileView,
    reference_column_view: ColumnProfileView,
    with_thresholds: bool,
    drift_map: Optional[Dict[str, ColumnDriftAlgorithm]] = None,
):
    if drift_map and column_name in drift_map:
        drift_algorithm = drift_map[column_name]
        drift_score: DriftAlgorithmScore = drift_algorithm.calculate(
            reference_column_view=reference_column_view,
            target_column_view=target_column_view,
            with_thresholds=with_thresholds,
        )
        return drift_score
    else:  # apply default drift algorithm selection logic
        ks_score = KS().calculate(
            reference_column_view=reference_column_view,
            target_column_view=target_column_view,
            with_thresholds=with_thresholds,
        )
        chisquare_score = ChiSquare().calculate(
            reference_column_view=reference_column_view,
            target_column_view=target_column_view,
            with_thresholds=with_thresholds,
        )
        return ks_score or chisquare_score


def calculate_drift_scores(
    target_view: DatasetProfileView,
    reference_view: DatasetProfileView,
    drift_map: Optional[Dict[str, ColumnDriftAlgorithm]] = None,
    with_thresholds=False,
) -> Dict[str, Optional[DriftAlgorithmScore]]:
    """Calculate drift scores for all columns in the target dataset profile.

    If a drift map is provided, the drift algorithm for each column in the map is determined by the map.
    Columns not in the map (or if map is not provided) will use the default drift algorithm selection logic.
    If the column does not have the required metrics to apply the selected algorithm, None is returned. For example, if KS or Hellinger is selected for a column with string values, None will be returned.

    If with_thresholds is True, the configured algorithm's thresholds is returned in the DriftAlgorithmScore.

    Returns a dictionary of column names to drift scores.



    Examples
    --------
    .. code-block:: python
        from whylogs.drift.column_drift_algorithms import Hellinger
        from whylogs.drift.configs import HellingerConfig

        hellingerconfig = HellingerConfig(max_hist_buckets=30)

        drift_map = {"legs": Hellinger(hellingerconfig), "weight": Hellinger(hellingerconfig)}
        drift_scores = calculate_drift_scores(
            target_view=target_view, reference_view=ref_view, drift_map=drift_map, with_thresholds=True
        )
    """
    drift_scores: Dict[str, Optional[DriftAlgorithmScore]] = {}
    target_view_columns = target_view.get_columns()
    reference_view_columns = reference_view.get_columns()

    for column_name in drift_map.keys():
        if column_name not in target_view_columns.keys():
            warnings.warn(f"Column {column_name} not found in target profile.")
        if column_name not in reference_view_columns.keys():
            warnings.warn(f"Column {column_name} not found in reference profile.")

    for target_column_name in target_view_columns:
        if target_column_name in reference_view_columns:
            target_view_column = target_view_columns[target_column_name]
            reference_view_column = reference_view_columns[target_column_name]
            drift_scores[target_column_name] = _get_drift_score(
                column_name=target_column_name,
                target_column_view=target_view_column,
                reference_column_view=reference_view_column,
                drift_map=drift_map,
                with_thresholds=with_thresholds,
            )
    return drift_scores
