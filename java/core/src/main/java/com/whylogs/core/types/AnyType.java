package com.whylogs.core.types;

import org.apache.commons.lang3.NotImplementedException;

public class AnyType extends DataType<T> {
    public AnyType(T tpe) {
        super(tpe);
    }

    @Override
    public boolean _do_match(T dtype_or_type) throws NotImplementedException {
        return false;
    }
}
}
