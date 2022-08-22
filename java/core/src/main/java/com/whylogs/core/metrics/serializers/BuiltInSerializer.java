package com.whylogs.core.metrics.serializers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
public @interface BuiltInSerializer {
    String name() default "";
    int typeID() default -1;
}
