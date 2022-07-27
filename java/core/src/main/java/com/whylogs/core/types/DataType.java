package com.whylogs.core.types;

import org.apache.commons.lang3.NotImplementedException;

public abstract class DataType<T> {
    private T tpe;

    public DataType(T tpe) {
        this.tpe = tpe;
    }

    public T returnType() {
        return tpe;
    }
}
