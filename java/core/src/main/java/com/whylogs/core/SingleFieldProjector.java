package com.whylogs.core;

import java.util.HashMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class SingleFieldProjector<T> {
  private final String columnName;

  public T apply(HashMap<String, Object> row) {
    if(!row.containsKey(this.columnName)) {
      throw new IllegalArgumentException("Column " + this.columnName + " not found in row");
    }

    return (T) row.get(this.columnName);
  }
}
