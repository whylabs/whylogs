package com.whylogs.core;

import java.util.HashMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SingleFieldProjector {
  private final String columnName;

  public <T> T apply(HashMap<String, T> row) {
    return row.get(columnName);
  }
}
