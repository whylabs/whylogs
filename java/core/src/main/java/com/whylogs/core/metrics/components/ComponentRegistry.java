package com.whylogs.core.metrics.components;
import java.util.HashMap;

public class ComponentRegistry {
    private final HashMap<String, Class<?>> namedComponent;
    private final HashMap<Integer, Class<?>> idComponent;

    private ComponentRegistry() {
        this.namedComponent = new HashMap<>();
        this.idComponent = new HashMap<>();

        // Register Defaults
        this.namedComponent.put("int", IntegralComponent.class);
        this.namedComponent.put("min", MinIntegralComponent.class);
        this.namedComponent.put("max", MaxIntegralComponent.class);

        this.idComponent.put(0, IntegralComponent.class);
        this.idComponent.put(1, MinIntegralComponent.class);
        this.idComponent.put(2, MaxIntegralComponent.class);
    }

    // Instance holder to avoid double-checking antipattern for singleton
    private static final class InstanceHolder {
        static final ComponentRegistry instance = new ComponentRegistry();
    }

    public static ComponentRegistry getInstance() {
        return ComponentRegistry.InstanceHolder.instance;
    }

    public <T, A extends MetricComponent<?>> void register(String name, A component) {
        namedComponent.put(name, component.getClass());
    }

    public <T, A extends MetricComponent<?>> void register(int typeId, A component) {
        idComponent.put(typeId, component.getClass());
    }

    public Class<?> get(int typeId) {
        return idComponent.get(typeId);
    }

    public Class<?> get(String typeName) {
        return namedComponent.get(typeName);
    }
}
