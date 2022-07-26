package com.whylogs.core.types;

import org.apache.commons.lang3.NotImplementedException;

public class Fractional extends DataType<Float> {
    public Fractional(Float tpe) {
        super(tpe);
    }

    @Override
    public boolean _do_match(Float dtype_or_type) throws NotImplementedException {
        return false;
    }
}
