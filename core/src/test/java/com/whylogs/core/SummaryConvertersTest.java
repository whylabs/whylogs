package com.whylogs.core;

import static org.testng.Assert.assertEquals;

import com.whylogs.core.statistics.NumberTracker;
import lombok.val;
import org.testng.annotations.Test;

public class SummaryConvertersTest {
  @Test
  public void convert_numbertracker_to_summary() {
    val numberTracker = new NumberTracker();
    final int count = 1000;
    for (int i = 0; i < count; i++) {
      numberTracker.track(i);
    }
    val summary = SummaryConverters.fromNumberTracker(numberTracker);
    assertEquals(summary.getCount(), count);
  }
}
