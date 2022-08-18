package whylogs.core.metrics;

import java.util.HashMap;
import lombok.*;
import whylogs.core.PreprocessedColumn;
import whylogs.core.SummaryConfig;

@EqualsAndHashCode
@Getter
@Setter
@RequiredArgsConstructor
public abstract class Metric {

  @NonNull private String namespace;

  public abstract HashMap<String, Object> toSummaryDict();

  public abstract HashMap<String, Object> toSummaryDict(SummaryConfig config);

  public abstract OperationResult columnarUpdate(PreprocessedColumn data);

  public @NonNull String getNamespace() {
    return namespace;
  }
}
