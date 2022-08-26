package com.whylogs.core.resolvers;

import com.whylogs.core.metrics.Metric;
import com.whylogs.core.schemas.ColumnSchema;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;

public abstract class Resolver {



    public abstract  <T extends Metric> HashMap<String, T> resolve(ColumnSchema schema);
}
