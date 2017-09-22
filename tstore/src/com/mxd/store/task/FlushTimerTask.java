package com.mxd.store.task;

import com.mxd.store.DefaultTimestampStore;

public class FlushTimerTask implements Runnable{
	
	private DefaultTimestampStore defaultTimestampStore;
	
	private boolean running = true;
	
	public FlushTimerTask(DefaultTimestampStore defaultTimestampStore) {
		super();
		this.defaultTimestampStore = defaultTimestampStore;
	}
	
	@Override
	public void run() {
		long begin = System.currentTimeMillis();
		while(this.running){
			while(System.currentTimeMillis() - begin <60000){
				try {
					Thread.sleep(100L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			this.defaultTimestampStore.flush();
			begin = System.currentTimeMillis();	
		}
	}
	
	public void stop(){
		this.running = false;
	}

}
