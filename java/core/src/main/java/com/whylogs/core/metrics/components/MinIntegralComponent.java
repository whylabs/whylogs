package com.whylogs.core.metrics.components;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Collection;

@Getter
@EqualsAndHashCode(callSuper=false)
public class MinIntegralComponent extends IntegralComponent {
    private static final ComponentTypeID TYPE_ID = ComponentTypeID.MIN_COMPONENT;

    public MinIntegralComponent() {
        super(Integer.MAX_VALUE);
    }
    public MinIntegralComponent(Integer value) {
        super(value);
    }

    @Override
    public ComponentTypeID getTypeId() {
        return TYPE_ID;
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
