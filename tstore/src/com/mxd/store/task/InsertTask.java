package com.mxd.store.task;

import com.mxd.store.DefaultTimestampStore;
import com.mxd.store.MemoryStore;
import com.mxd.store.StoreUnit;
import com.mxd.store.TimestampStore;

public class InsertTask<T> implements Runnable{
	
	private DefaultTimestampStore<T> store;
	
	private MemoryStore<T> memoryStore;
	
	private StoreUnit<T> storeUnit;
	
	public InsertTask(DefaultTimestampStore<T> store, StoreUnit<T> storeUnit) {
		super();
		this.store = store;
		this.storeUnit = storeUnit;
		this.memoryStore = this.store.getMemoryStore();
	}

	@Override
	public void run() {
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
