package com.mxd.store.task;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import com.mxd.store.StoreIndex;
import com.mxd.store.common.StoreResult;
import com.mxd.store.common.StoreUnit;

public class DiskReadTask extends DiskTask implements Callable<StoreResult> {
	
	private List<StoreUnit> result = new ArrayList<>();
	
	public DiskReadTask(long id, long minTimestamp, long maxTimestamp, long timestamp, StoreIndex storeIndex,
			boolean isMiddle, int storeUnitSize, int buffsetObjectSize) {
		super(id, minTimestamp, maxTimestamp, timestamp, storeIndex, isMiddle, storeUnitSize, buffsetObjectSize);
	}
	
	@Override
	protected void readTask(long id, long timestamp, ByteBuffer buffer) {
		//读出columns
		byte[] bytes = new byte[this.storeUnitSize];
		buffer.get(bytes);
		this.result.add(new StoreUnit(timestamp, id,bytes));
	}

	@Override
	public StoreResult call() throws Exception {
		long begin = System.currentTimeMillis();
		try {
			this.read();
		} catch (FileNotFoundException e) {
			
		}
		return new StoreResult(this.result, (int)(System.currentTimeMillis()-begin),true);
	}
}
