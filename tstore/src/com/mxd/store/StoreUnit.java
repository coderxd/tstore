package com.mxd.store;


public class StoreUnit<T> implements Comparable<StoreUnit<T>>{
	
	private long timestamp;
	
	private long id;
	
	private T data;
	
	public StoreUnit(long timestamp, long id,T data) {
		super();
		this.timestamp = timestamp;
		this.id = id;
		this.data = data;
	}

	public StoreUnit(long timestamp, long id) {
		super();
		this.timestamp = timestamp;
		this.id = id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}

	@Override
	public int compareTo(StoreUnit<T> o) {
		return Long.valueOf(this.timestamp).compareTo(o.getTimestamp());
	}
}
