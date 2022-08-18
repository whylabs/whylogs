package whylogs.core;

import java.util.ArrayList;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SummaryConfig {
  private ArrayList<String> disabledMetrics;
  private FrequentItemsErrorType frequentItemsErrorType = FrequentItemsErrorType.NO_FALSE_POSITIVES;
  private int frequentItemsLimit = -1;
  private int hll_stddev = 1;
}
