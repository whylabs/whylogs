package com.whylogs.core.metics.components;

import lombok.Data;

@Data
public class MinIntegralComponent extends IntegralComponent {
    private final int type_id = 1;

    public MinIntegralComponent(Integer value) {
        super(value);
    }

    // TODO: python has the @_id_aggregator decorator, how do we update?
    public static Integer min(Integer a, Integer b) {
        return Integer.min(a, b);
    }
}
