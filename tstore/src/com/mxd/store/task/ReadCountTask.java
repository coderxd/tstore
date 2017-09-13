package com.mxd.store.task;

import java.util.concurrent.Callable;

import com.mxd.store.StoreResult;
import com.mxd.store.TimestampStore;

public class ReadCountTask<T> implements Callable<StoreResult<T>>{
	
	protected TimestampStore<T> store;
	
	protected long id;
	
	protected long minTimestamp;
	
	protected long maxTimestamp;
	
	public ReadCountTask(TimestampStore<T> store, long id, long minTimestamp, long maxTimestamp) {
		super();
		this.store = store;
		this.id = id;
		this.minTimestamp = minTimestamp;
		this.maxTimestamp = maxTimestamp;
	}

	@Override
	public StoreResult<T> call() throws Exception {
		return this.store.findCount(this.id, this.minTimestamp, this.maxTimestamp);
	}	

}