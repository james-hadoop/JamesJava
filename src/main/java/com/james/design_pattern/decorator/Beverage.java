package com.james.design_pattern.decorator;

public abstract class Beverage {
    String description = "Unknown Beverage";

    public String getDecription() {
        return description;
    }

    public abstract double cost();
}
