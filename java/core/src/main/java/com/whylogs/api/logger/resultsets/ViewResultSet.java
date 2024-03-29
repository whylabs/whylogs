package com.whylogs.api.logger.resultsets;

import com.whylogs.core.DatasetProfile;
import com.whylogs.core.views.DatasetProfileView;
import java.util.Optional;
import lombok.*;

@EqualsAndHashCode(callSuper = true)
@Data
public class ViewResultSet extends ResultSet {
  @NonNull private final DatasetProfileView view;

  public ViewResultSet(@NonNull DatasetProfileView view) {
    this.view = view;
  }

  public ViewResultSet(DatasetProfile profile) {
    this.view = profile.view();
  }

  @Override
  public Optional<DatasetProfileView> view() {
    return Optional.of(this.view);
  }

  @Override
  public Optional<DatasetProfile> profile() {
    throw new Error("No profile available for a view result set");
  }
}
