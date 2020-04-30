package com.james.design_pattern.iterator;

public interface Aggregate {
    void add(Object object);

    void remove(Object object);

    Iterator iterator();
}
