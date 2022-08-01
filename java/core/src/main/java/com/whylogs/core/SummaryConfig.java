package com.whylogs.core;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;

@Data
@NoArgsConstructor
public class SummaryConfig {
    private ArrayList<String> disabledMetrics;
    private FrequentItemsErrorType frequentItemsErrorType = FrequentItemsErrorType.NO_FALSE_POSITIVES;
    private int frequentItemsLimit = -1;
    private int hll_stddev = 1;

}
