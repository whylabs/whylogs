package com.whylogs.core.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MetricConfig {
  private final int hll_lg_k = 12;
  private final int kll_k = 256;
  private final int fi_lg_max_k = 10;
  private final boolean fi_disabled = false;
  private final boolean track_unicode_ranges = false;
  private final boolean large_kll_k = true;
  private final int kll_k_large = 1024;

  private final boolean lower_case_unicode = true;
  private final boolean normalize_unicode = true;
  private static final HashMap<String, ArrayList<Integer>> unicode_ranges;

  static {
    unicode_ranges = new HashMap<>();
    unicode_ranges.put(
        "basic-latin",
        new ArrayList<Integer>() {
          {
            add(0x0000);
            add(0x007F);
          }
        });
    unicode_ranges.put(
        "emoticon",
        new ArrayList<Integer>() {
          {
            add(0x1F600);
            add(0x1F64F);
          }
        });
    unicode_ranges.put(
        "control",
        new ArrayList<Integer>() {
          {
            add(0x0000);
            add(0x001F);
          }
        });
    unicode_ranges.put(
        "digit",
        new ArrayList<Integer>() {
          {
            add(0x0030);
            add(0x0039);
          }
        });
    unicode_ranges.put(
        "latin-upper",
        new ArrayList<Integer>() {
          {
            add(0x0061);
            add(0x007A);
          }
        });
    unicode_ranges.put(
        "latin-lower",
        new ArrayList<Integer>() {
          {
            add(0x0041);
            add(0x005A);
          }
        });
    unicode_ranges.put(
        "extended-latin",
        new ArrayList<Integer>() {
          {
            add(0x0080);
            add(0x02AF);
          }
        });
  }
}
