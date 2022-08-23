package com.whylogs.core.resolvers;

import com.whylogs.core.metrics.Metric;
import com.whylogs.core.schemas.ColumnSchema;

import java.util.HashMap;

public abstract class Resolver {
    public abstract HashMap<String, Metric> resolve(ColumnSchema schema);
}
