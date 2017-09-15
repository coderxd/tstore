package com.mxd.store;

import java.util.List;
import com.mxd.store.common.SerializeStore;
import com.mxd.store.common.StoreResult;
import com.mxd.store.common.StoreUnit;

/**
 * 时序存储基类
 * @author mxd
 *
 */
public abstract class TimestampStore {
	
	public static enum SaveStatus{
		SUCCESS,OVERFLOW,EXCEPTION
	}
	
	/**
	 * 插入单条数据
	 * @param storeUnit
	 */
	public abstract SaveStatus insert(StoreUnit storeUnit);
	
	/**
	 * 插入多条数据(同一ID)
	 * @param storeUnits
	 */
	public abstract SaveStatus insert(long id,List<StoreUnit> storeUnits);
	
	/**
	 * 插入多条数据(不同ID)
	 * @param storeUnits
	 */
	public abstract SaveStatus insert(List<StoreUnit> storeUnits);
		
	
	/**
	 * 查找全部（包含最大最小时间戳）
	 * @param id
	 * @param minTimestamp	最小时间戳
	 * @param maxTimestamp	最大时间戳
	 * @return
	 */
	public abstract StoreResult find(long id,long minTimestamp,long maxTimestamp);
	
	/**
	 * 查找全部数量（包含最大最小时间戳）
	 * @param id
	 * @param minTimestamp	最小时间戳
	 * @param maxTimestamp	最大时间戳
	 * @return
	 */
	public abstract StoreResult findCount(long id,long minTimestamp,long maxTimestamp);
	
	
	public abstract SerializeStore getSerializeStore();
	
}
