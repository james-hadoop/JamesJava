package com.james.design_pattern.state;

public interface State {
	public abstract void insertQuarter();

	public abstract void ejectQuarter();
	
	public abstract void turnCrank();
	
	public abstract void dispense();
}
