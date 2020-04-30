package com.james.design_pattern.iterator;

public interface Iterator {
    Object next();

    boolean hasNext();

    boolean remove();
}
