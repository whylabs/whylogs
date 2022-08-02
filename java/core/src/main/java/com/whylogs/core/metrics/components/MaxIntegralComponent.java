package com.whylogs.core.metrics.components;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class MaxIntegralComponent extends IntegralComponent {
    private final int type_id = 2;

    public MaxIntegralComponent(Integer value) {
        super(value);
    }

    // TODO: python has the @_id_aggregator decorator, how do we update?
    public static Integer max(Integer a, Integer b) {
        return Integer.max(a, b);
    }
}
