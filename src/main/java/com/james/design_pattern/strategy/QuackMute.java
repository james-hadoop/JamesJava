package com.james.design_pattern.strategy;

public class QuackMute implements QuackBehavior {
	public void quack() {
		System.out.println("I'am slient!");
	}
}
