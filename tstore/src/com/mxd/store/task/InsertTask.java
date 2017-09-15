package com.mxd.store.task;

import com.mxd.store.DefaultTimestampStore;
import com.mxd.store.MemoryStore;
import com.mxd.store.TimestampStore;
import com.mxd.store.common.StoreUnit;

public class InsertTask implements Runnable{
	
	private DefaultTimestampStore store;
	
	private MemoryStore memoryStore;
	
	private StoreUnit storeUnit;
	
	public InsertTask(DefaultTimestampStore store, StoreUnit storeUnit) {
		super();
		this.store = store;
		this.storeUnit = storeUnit;
		this.memoryStore = this.store.getMemoryStore();
	}

	@Override
	public void run() {
		//当memoryStore数据满了的时候，将memoryStore写入diskStore
		while(TimestampStore.SaveStatus.OVERFLOW==this.memoryStore.insert(this.storeUnit)){	
			this.store.flush();
			try {
				Thread.sleep(500L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
