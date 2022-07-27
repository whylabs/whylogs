package com.whylogs.core.types;

import org.apache.commons.lang3.NotImplementedException;

public class Integral extends DataType<Integer> {

    public Integral(Integer tpe) {
        super(tpe);
    }

    @Override
    public boolean _do_match(Integer dtype_or_type) throws NotImplementedException {
        return false;
    }
}
