package com.whylogs.core.metrics.components;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Collection;

@Getter
@EqualsAndHashCode(callSuper=false)
public class MinIntegralComponent extends IntegralComponent {
    private static final int TYPE_ID = 1;

    public MinIntegralComponent() {
        super(Integer.MAX_VALUE);
    }
    public MinIntegralComponent(Integer value) {
        super(value);
    }

    @Override
    public int getTypeId() {
        return TYPE_ID;
    }

    @Override
    public String getTypeName() {
        return this.getClass().getSimpleName();
    }

    public static MinIntegralComponent min(Collection<? extends Integer> list){
        int min = Integer.MAX_VALUE;
        for(Integer i : list){
            min = Integer.min(min, i);
        }
        return new MinIntegralComponent(min);
    }

    public static MinIntegralComponent min(MinIntegralComponent a, MinIntegralComponent b) {
        return new MinIntegralComponent(Integer.min(a.getValue(), b.getValue()));
    }
    public static MinIntegralComponent min(int a, int b) {
        return new MinIntegralComponent(Integer.min(a, b));
    }
}
