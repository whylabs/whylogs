package com.whylogs.core;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;

@RequiredArgsConstructor
@Getter
public class SingleFieldProjector {
    private final String columnName;

    public <T> T apply(HashMap<String, T> row) {
        return row.get(columnName);
    }
}
