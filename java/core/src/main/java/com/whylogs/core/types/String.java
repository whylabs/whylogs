package com.whylogs.core.types;

import org.apache.commons.lang3.NotImplementedException;

public class String extends DataType<String>{
    public String(String tpe) {
        super(tpe);
    }

    @Override
    public boolean _do_match(String dtype_or_type) throws NotImplementedException {
        return false;
    }
}
