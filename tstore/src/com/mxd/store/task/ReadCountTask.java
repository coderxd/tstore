package com.mxd.store.task;

import java.util.concurrent.Callable;

import com.mxd.store.TimestampStore;
import com.mxd.store.common.StoreResult;

public class ReadCountTask implements Callable<StoreResult>{
	
	protected TimestampStore store;
	
	protected long id;
	
	protected long minTimestamp;
	
	protected long maxTimestamp;
	
	public ReadCountTask(TimestampStore store, long id, long minTimestamp, long maxTimestamp) {
		super();
		this.store = store;
		this.id = id;
		this.minTimestamp = minTimestamp;
		this.maxTimestamp = maxTimestamp;
	}

	@Override
	public StoreResult call() throws Exception {
		return this.store.findCount(this.id, this.minTimestamp, this.maxTimestamp);
	}	

}