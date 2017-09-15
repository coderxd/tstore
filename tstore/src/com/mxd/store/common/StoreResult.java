package com.mxd.store.common;

import java.util.Collections;
import java.util.List;

/**
 * 查询结果
 * @author mxd
 *
 */
public class StoreResult {
	
	/**
	 * 结果集,查询count时这里为空
	 */
	private List<StoreUnit> data;
	
	private int consuming;
	
	private long size;
	
	public StoreResult(List<StoreUnit> data, int consuming,boolean sort) {
		this(data, consuming);
		if(sort){
			Collections.sort(data);
		}
		
	}

	public StoreResult(List<StoreUnit> data, int consuming) {
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
	
	public List<StoreUnit> getData() {
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
