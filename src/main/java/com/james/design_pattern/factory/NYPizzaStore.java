package com.james.design_pattern.factory;

public class NYPizzaStore extends PizzaStore {
	public Pizza createPizza(String item) {
		if (item.equals("cheese")) {
			return new NYStylePizza();
		} else {
			return null;
		}
	}
}
