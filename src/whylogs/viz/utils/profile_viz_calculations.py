import json

from whylogs.core.summaryconverters import (
    compute_chi_squared_test_p_value,
    ks_test_compute_p_value,
    single_quantile_from_sketch,
)
from whylogs.proto import InferredType, ReferenceDistributionDiscreteMessage

categorical_types = (InferredType.Type.STRING, InferredType.Type.BOOLEAN)


def __calculate_variance(profile_jsons, feature_name):
    """
    Calculates variance for single feature

    Parameters
    ----------
    profile_jsons: Profile summary serialized json
    feature_name: Name of feature

    Returns
    -------
    variance : Calculated variance for feature
    """
    feature = profile_jsons.get("columns").get(feature_name)
    variance = feature.get("numberSummary").get("stddev") ** 2 if feature.get("numberSummary") is not None else 0
    return variance


def __calculate_coefficient_of_variation(profile_jsons, feature_name):
    """
    Calculates coefficient of variation for single feature

    Parameters
    ----------
    profile_jsons: Profile summary serialized json
    feature_name: Name of feature

    Returns
    -------
    coefficient_of_variation : Calculated coefficient of variation for feature
    """
    feature = profile_jsons.get("columns").get(feature_name)
    coefficient_of_variation = (
        feature.get("numberSummary").get("stddev") / feature.get("numberSummary").get("mean") if feature.get("numberSummary") is not None else 0
    )
    return coefficient_of_variation


def __calculate_sum(profile_jsons, feature_name):
    """
    Calculates sum for single feature

    Parameters
    ----------
    profile_jsons: Profile summary serialized json
    feature_name: Name of feature

    Returns
    -------
    coefficient_of_variation : Calculated sum for feature
    """
    feature = profile_jsons.get("columns").get(feature_name)
    feature_number_summary = feature.get("numberSummary")
    if feature_number_summary:
        sum = feature_number_summary.get("mean") * int(feature.get("counters").get("count"))
    else:
        sum = 0
    return sum


def __calculate_quantile_statistics(feature, profile_jsons, feature_name):
    """
    Calculates sum for single feature

    Parameters
    ----------
    profile_jsons: Profile summary serialized json
    feature_name: Name of feature

    Returns
    -------
    coefficient_of_variation : Calculated sum for feature
    """
    quantile_statistics = {}
    feature_number_summary = profile_jsons.get("columns").get(feature_name).get("numberSummary")
    if feature.number_tracker and feature.number_tracker.histogram.get_n() > 0:
        kll_sketch = feature.number_tracker.histogram
        quantile_statistics["fifth_percentile"] = single_quantile_from_sketch(kll_sketch, quantile=0.05).quantile
        quantile_statistics["q1"] = single_quantile_from_sketch(kll_sketch, quantile=0.25).quantile
        quantile_statistics["median"] = single_quantile_from_sketch(kll_sketch, quantile=0.5).quantile
        quantile_statistics["q3"] = single_quantile_from_sketch(kll_sketch, quantile=0.75).quantile
        quantile_statistics["ninety_fifth_percentile"] = single_quantile_from_sketch(kll_sketch, quantile=0.95).quantile
        quantile_statistics["range"] = feature_number_summary.get("max") - feature_number_summary.get("min")
        quantile_statistics["iqr"] = quantile_statistics["q3"] - quantile_statistics["q1"]
    return quantile_statistics


def add_drift_val_to_ref_profile_json(target_profile, reference_profile, reference_profile_json):
    """
    Calculates drift value for reference profile based on profile type and inserts that data into reference profile

    Parameters
    ----------
    target_profile: Target profile
    reference_profile: Reference profile
    reference_profile_json: Reference profile summary serialized json

    Returns
    -------
    reference_profile_json : Reference profile summary serialized json with drift value for every feature
    """
    observations = 0
    missing_cells = 0
    total_count = 0
    for target_col_name in target_profile.columns.keys():
        target_col = target_profile.columns[target_col_name]
        observations += target_col.counters.to_protobuf().count
        null_count = target_col.to_summary().counters.null_count.value
        missing_cells += null_count if null_count else 0
        total_count += target_col.to_summary().counters.count

        if target_col_name in reference_profile.columns:
            ref_col = reference_profile.columns[target_col_name]
            target_type = target_col.schema_tracker.to_summary().inferred_type.type
            unique_count = target_col.to_summary().unique_count
            ref_type = ref_col.schema_tracker.to_summary().inferred_type.type
            if all([type == InferredType.FRACTIONAL or type == InferredType.INTEGRAL for type in (ref_type, target_type)]):
                target_kll_sketch = target_col.number_tracker.histogram
                reference_kll_sketch = ref_col.number_tracker.histogram
                ks_p_value = ks_test_compute_p_value(target_kll_sketch, reference_kll_sketch)
                reference_profile_json["columns"][target_col_name]["drift_from_ref"] = ks_p_value.ks_test
            elif all([type in categorical_types for type in (ref_type, target_type)]) and ref_type == target_type:
                target_frequent_items_sketch = target_col.frequent_items
                reference_frequent_items_sketch = ref_col.frequent_items
                if any([msg.to_summary() is None for msg in (target_frequent_items_sketch, reference_frequent_items_sketch)]):
                    continue
                target_total_count = target_col.counters.count
                target_message = ReferenceDistributionDiscreteMessage(
                    frequent_items=target_frequent_items_sketch.to_summary(), unique_count=unique_count, total_count=target_total_count
                )
                ref_total_count = ref_col.counters.count

                reference_message = ReferenceDistributionDiscreteMessage(
                    frequent_items=reference_frequent_items_sketch.to_summary(), total_count=ref_total_count
                )
                chi_squared_p_value = compute_chi_squared_test_p_value(target_message, reference_message)
                if chi_squared_p_value.chi_squared_test is not None:
                    reference_profile_json["columns"][target_col_name]["drift_from_ref"] = chi_squared_p_value.chi_squared_test
                else:
                    reference_profile_json["columns"][target_col_name]["drift_from_ref"] = None
    reference_profile_json["properties"]["observations"] = observations
    reference_profile_json["properties"]["missing_cells"] = missing_cells
    reference_profile_json["properties"]["total_count"] = total_count
    reference_profile_json["properties"]["missing_percentage"] = (missing_cells / total_count) * 100 if total_count else 0

    return reference_profile_json


def add_feature_statistics(feature, profile_json, feature_name):
    """
    Calculates different values for feature statistics

    Parameters
    ----------
    feature:
    profile_json: Profile summary serialized json
    feature_name: Name of feature

    Returns
    -------
    feature: Feature data with appended values for statistics report
    """
    profile_features = json.loads(profile_json)
    feature_with_statistics = {}
    feature_with_statistics["properties"] = profile_features.get("properties")
    feature_with_statistics[feature_name] = profile_features.get("columns").get(feature_name)
    feature_with_statistics[feature_name]["sum"] = __calculate_sum(profile_features, feature_name)
    feature_with_statistics[feature_name]["variance"] = __calculate_variance(profile_features, feature_name)
    feature_with_statistics[feature_name]["coefficient_of_variation"] = __calculate_coefficient_of_variation(profile_features, feature_name)
    feature_with_statistics[feature_name]["quantile_statistics"] = __calculate_quantile_statistics(feature, profile_features, feature_name)
    return feature_with_statistics
