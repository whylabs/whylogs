package com.whylogs.core;

import lombok.Data;

import java.util.ArrayList;

@Data
public class ListView {
    private ArrayList<Integer> ints;
    private ArrayList<String> strings;
    private ArrayList<Float> floats;
    private ArrayList<Object> objects;

    // TODO: iterable in python is easy , but here the typing is a bit more complicated

    public void add(int i){
        this.ints.add(i);
    }

    public void add(String s){
        this.strings.add(s);
    }

    public void add(float f){
        this.floats.add(f);
    }

    public void add(Object o){
        this.objects.add(o);
    }
}
