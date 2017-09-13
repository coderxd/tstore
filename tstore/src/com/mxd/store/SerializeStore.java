package com.mxd.store;

public abstract class SerializeStore<T> {
	
	public abstract T decode(byte[] data);
	
	public abstract byte[] encode(T t);
	
}
