package com.whylogs.api.logger.resultSets;

import com.whylogs.core.DatasetProfile;
import com.whylogs.core.errors.Error;
import com.whylogs.core.metrics.Metric;
import com.whylogs.core.views.DatasetProfileView;
import java.util.Optional;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A holder object for profiling results.
 *
 * <p>A whylogs.log call can result in more than one profile. This wrapper class simplifies the
 * navigation among these profiles.
 *
 * <p>Note that currently we only hold one profile but we're planning to add other kinds of profiles
 * such as segmented profiles here.
 */
@Data
@NoArgsConstructor
public abstract class ResultSet {

  // TODO: implement read and write when I make the reader and writer

  public abstract Optional<DatasetProfileView> view();

  public abstract Optional<DatasetProfile> profile();

  // TODO: Come back for ModelPerformanceMetrics

  public void addMetric(String name, Metric<?> metric) throws Error {
    DatasetProfile profile = this.profile().orElseThrow(() -> new Error("Cannot add " + name + " metric " + metric + " to a result set without a profile");
    profile.addMetric(name, metric);
  }
}
