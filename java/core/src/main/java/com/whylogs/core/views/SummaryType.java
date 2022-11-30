package com.whylogs.core.views;

public enum SummaryType {
  COLUMN("COLUMN"),
  DATASET("DATASET");

  public final String label;

  private SummaryType(String label) {
    this.label = label;
  }
}
