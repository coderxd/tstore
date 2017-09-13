package com.mxd.store;

import java.util.List;

/**
 * 时序存储基类
 * @author mxd
 *
 */
public abstract class TimestampStore<T> {
	
	public static enum SaveStatus{
		SUCCESS,OVERFLOW,EXCEPTION
	}
	
	/**
	 * 插入单条数据
	 * @param storeUnit
	 */
	public abstract SaveStatus insert(StoreUnit<T> storeUnit);
	
	/**
	 * 插入多条数据
	 * @param storeUnits
	 */
	public abstract SaveStatus insert(long id,List<StoreUnit<T>> storeUnits);
		
	
	/**
	 * 查找全部（包含最大最小时间戳）
	 * @param id
	 * @param minTimestamp	最小时间戳
	 * @param maxTimestamp	最大时间戳
	 * @return
	 */
	public abstract StoreResult<T> find(long id,long minTimestamp,long maxTimestamp);
	
	/**
	 * 查找全部数量（包含最大最小时间戳）
	 * @param id
	 * @param minTimestamp	最小时间戳
	 * @param maxTimestamp	最大时间戳
	 * @return
	 */
	public abstract StoreResult<T> findCount(long id,long minTimestamp,long maxTimestamp);
	
}
