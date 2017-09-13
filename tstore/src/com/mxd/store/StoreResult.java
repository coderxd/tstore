package com.mxd.store;

import java.util.Collections;
import java.util.List;

public class StoreResult<T> {
	
	private List<StoreUnit<T>> data;
	
	private int consuming;
	
	private long size;
	
	public StoreResult(List<StoreUnit<T>> data, int consuming,boolean sort) {
		this(data, consuming);
		if(sort){
			Collections.sort(data);
		}
		
	}

	public StoreResult(List<StoreUnit<T>> data, int consuming) {
		super();
		this.data = data;
		this.consuming = consuming;
		if(data!=null){
			this.size = data.size();
		}
	}
	
	public StoreResult(long size, int consuming) {
		super();
		this.consuming = consuming;
		this.size = size;
	}
	
	public List<StoreUnit<T>> getData() {
		return data;
	}

	public int getConsuming() {
		return consuming;
	}

	public long getSize() {
		return size;
	}

	@Override
	public String toString() {
		return "StoreResult [consuming=" + consuming + ", size=" + size + "]";
	}
}
