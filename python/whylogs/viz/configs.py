from dataclasses import dataclass


@dataclass
class HistogramConfig:
    max_hist_buckets: int = 30
    hist_avg_number_per_bucket: int = 4
    min_n_buckets: int = 2
