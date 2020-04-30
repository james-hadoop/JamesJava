package com.james.design_pattern.proxy;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface AdditionInterface extends Remote {
	public int add(int a, int b) throws RemoteException;
}
