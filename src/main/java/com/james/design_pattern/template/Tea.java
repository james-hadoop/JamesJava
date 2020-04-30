package com.james.design_pattern.template;

public class Tea extends CaffeineBeverage {
	void brew() {
		System.out.println("Sleeping the tea");
	}

	void addCondiments() {
		System.out.println("Add Lemon");
	}
}
