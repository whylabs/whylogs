def _quantile_strings(quantiles: list):
    return ["quantile_{:.4f}".format(q) for q in quantiles]


def flatten_dataset_quantiles(dataset_summary: DatasetSummary):
    """
    Flatten quantiles from a dataset summary
    """
    quants = {}
    for col_name, col in dataset_summary.columns.items():
        try:
            quant = getter(getter(col, "number_summary"), "quantiles")
            x = OrderedDict()
            for q, qval in zip(_quantile_strings(quant.quantiles), quant.quantile_values):
                x[q] = qval
            quants[col_name] = x
        except KeyError:
            pass

    return quants


def flatten_dataset_string_quantiles(dataset_summary: DatasetSummary):
    """
    Flatten quantiles from a dataset summary
    """
    quants = {}
    for col_name, col in dataset_summary.columns.items():
        try:
            quant = getter(getter(col, "number_summary"), "quantiles")
            x = OrderedDict()
            for q, qval in zip(_quantile_strings(quant.quantiles), quant.quantile_values):
                x[q] = qval
            quants[col_name] = x
        except KeyError:
            pass

    return quants


def flatten_dataset_histograms(dataset_summary: DatasetSummary):
    """
    Flatten histograms from a dataset summary
    """
    histograms = {}

    for col_name, col in dataset_summary.columns.items():
        try:
            hist = getter(getter(col, "number_summary"), "histogram")
            if len(hist.bins) > 1:
                histograms[col_name] = {
                    "bin_edges": list(hist.bins),
                    "counts": list(hist.counts),
                }
        except KeyError:
            continue
    return histograms


def flatten_dataset_frequent_numbers(dataset_summary: DatasetSummary):
    """
    Flatten frequent number counts from a dataset summary
    """
    frequent_numbers = {}

    for col_name, col in dataset_summary.columns.items():
        try:
            summary = getter(getter(col, "number_summary"), "frequent_numbers")
            flat_dict = FrequentNumbersSketch.flatten_summary(summary)
            if len(flat_dict) > 0:
                frequent_numbers[col_name] = flat_dict
        except KeyError:
            continue
    return frequent_numbers


def flatten_dataset_frequent_strings(dataset_summary: DatasetSummary):
    """
    Flatten frequent strings summaries from a dataset summary
    """
    frequent_strings = {}

    for col_name, col in dataset_summary.columns.items():
        try:
            item_summary = getter(getter(col, "string_summary"), "frequent").items
            items = {item.value: int(item.estimate) for item in item_summary}
            if items:
                frequent_strings[col_name] = items
        except KeyError:
            continue

    return frequent_strings
