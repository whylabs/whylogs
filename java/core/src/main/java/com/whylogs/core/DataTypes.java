package com.whylogs.core;

import java.lang.reflect.Type;
import java.util.HashSet;

public enum DataTypes {
    Numerical {
        @Override
        public HashSet<Type> getTypes() {
            HashSet<Type> dataTypes = new HashSet<>();
            dataTypes.add(Long.class);
            dataTypes.add(Integer.class);
            dataTypes.add(Double.class);
            dataTypes.add(Float.class);
            return dataTypes;
        }

        @Override
        public boolean includes(Type type) {
            return getTypes().contains(type);
        }
    },
    Integral {
        @Override
        public HashSet<Type> getTypes() {
             HashSet<Type> types = new HashSet();
             types.add(Long.class);
             types.add(Integer.class);
             return types;
        }

        @Override
        public boolean includes(Type type) {
            return getTypes().contains(type);
        }
    },
    Fractional {
        @Override
        public HashSet<Type> getTypes() {
            HashSet<Type> types = new HashSet();
            types.add(Double.class);
            types.add(Float.class);
            return types;
        }

        @Override
        public boolean includes(Type type) {
            return getTypes().contains(type);
        }

    },
    String {
        @Override
        public HashSet<Type> getTypes() {
            HashSet<Type> types = new HashSet();
            types.add(String.class);
            return types;
        }

        @Override
        public boolean includes(Type type) {
            return getTypes().contains(type);
        }
    }
    ;

    public abstract HashSet<Type> getTypes();
    public abstract boolean includes(Type type);
}
