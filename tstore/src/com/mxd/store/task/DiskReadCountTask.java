package com.mxd.store.task;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import com.mxd.store.StoreIndex;

public class DiskReadCountTask extends DiskTask implements Callable<Long> {
	
	private long count = 0;
	
	public DiskReadCountTask(long id, long minTimestamp, long maxTimestamp, long timestamp, StoreIndex storeIndex,
			boolean isMiddle, int storeUnitSize, int buffsetObjectSize) {
		super(id, minTimestamp, maxTimestamp, timestamp, storeIndex, isMiddle, storeUnitSize, buffsetObjectSize);
	}


	@Override
	protected boolean beforeRead(int len) {
		if(this.isMiddle){
			this.count = len;
			return false;
		}
		return true;
	}
	
	@Override
	protected void readTask(long id, long timestamp, ByteBuffer buffer) {
		this.count++;
		super.readTask(id, timestamp, buffer);//跳过数据部分
	}

	@Override
	public Long call() throws Exception {
		this.read();
		return this.count;
	}
}
