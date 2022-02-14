from whylogs.core.summaryconverters import ks_test_compute_p_value, compute_chi_squared_test_p_value

from whylogs.proto import ReferenceDistributionDiscreteMessage, InferredType

TYPES = InferredType.Type
categorical_types = (TYPES.INTEGRAL, TYPES.STRING, TYPES.BOOLEAN)


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
    # QUESTION: Should this function need to change behaviour to add drift into target profile?
    for target_col_name in target_profile.columns.keys():
        target_col = target_profile.columns[target_col_name]
        if target_col_name in reference_profile.columns:
            ref_col = reference_profile.columns[target_col_name]
            target_type = target_col.schema_tracker.to_summary().inferred_type.type
            ref_type = ref_col.schema_tracker.to_summary().inferred_type.type
            if all([type == InferredType.FRACTIONAL for type in (ref_type, target_type)]):
                target_kll_sketch = target_col.number_tracker.histogram
                reference_kll_sketch = ref_col.number_tracker.histogram
                ks_p_value = ks_test_compute_p_value(target_kll_sketch, reference_kll_sketch)
                reference_profile_json['columns'][target_col_name]['drift_from_ref'] = ks_p_value.ks_test
            elif all([type in categorical_types for type in (ref_type, target_type)]) and ref_type == target_type:
                target_frequent_items_sketch = target_col.frequent_items
                reference_frequent_items_sketch = ref_col.frequent_items
                if any([msg.to_summary() is None for msg in (target_frequent_items_sketch, reference_frequent_items_sketch)]):
                    continue
                target_total_count = target_col.counters.count
                target_message = ReferenceDistributionDiscreteMessage(
                    frequent_items=target_frequent_items_sketch.to_summary(),
                    total_count=target_total_count)
                ref_total_count = ref_col.counters.count

                reference_message = ReferenceDistributionDiscreteMessage(
                    frequent_items=reference_frequent_items_sketch.to_summary(),
                    total_count=ref_total_count
                )
                chi_squared_p_value = compute_chi_squared_test_p_value(
                    target_message, reference_message)
                if chi_squared_p_value.chi_squared_test:
                    reference_profile_json['columns'][target_col_name]['drift_from_ref'] = chi_squared_p_value.chi_squared_test
                else:
                    reference_profile_json['columns'][target_col_name]['drift_from_ref'] = None
    return reference_profile_json


def calculate_variance(profile_jsons, feature_name):
    feature = profile_jsons.get(
        'columns').get(feature_name)
    variance = feature.get('numberSummary').get('stddev')**2
    return variance


def calculate_coefficient_of_variation(profile_jsons, feature_name):
    feature = profile_jsons.get(
        'columns').get(feature_name)
    coefficient_of_variation = feature.get(
        'numberSummary').get('stddev')/feature.get('numberSummary').get('mean')
    return coefficient_of_variation
