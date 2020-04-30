package com.james.design_pattern.command;

public class Light {
    String light;

    public Light(String light) {
        this.light = light;
    }

    public void on() {
        System.out.println(light + " is on!");
    }

    public void off() {
        System.out.println(light + " is off!");
    }
}
