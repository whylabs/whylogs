package com.whylogs.api.logger.resultSets;

import com.whylogs.core.DatasetProfile;
import com.whylogs.core.views.DatasetProfileView;
import java.util.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

@EqualsAndHashCode(callSuper = true)
@Data
public class ProfileResultSet extends ResultSet {
  @NonNull private final DatasetProfile profile;

  public ProfileResultSet(DatasetProfile profile) {
    super();
    this.profile = profile;
  }

  public Optional<DatasetProfile> profile() {
    return Optional.of(this.profile);
  }

  public Optional<DatasetProfileView> view() {
    return Optional.of(this.profile.view());
  }
}
