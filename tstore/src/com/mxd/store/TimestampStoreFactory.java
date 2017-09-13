package com.mxd.store;

import java.io.File;
import java.io.IOException;

public class TimestampStoreFactory {
	
	public StoreConfiguration configuration;
	
	public TimestampStoreFactory(StoreConfiguration configuration) {
		super();
		if(configuration == null){
			throw new NullPointerException("storeConfiguration is null");
		}else if(configuration.getDiskPath() == null){
			throw new NullPointerException("diskPath is null");
		}
		this.configuration = configuration.get();
	}
	
	public <T> TimestampStore<T> create() throws IOException{
		
		File file = new File(this.configuration.getDiskPath());
		if(!file.exists()){
			file.mkdirs();
		}
		
		StoreIndex storeIndex = new StoreIndex(this.configuration);
		
		DiskStore<T> diskStore = new DiskStore<T>(this.configuration,storeIndex);
		
		MemoryStore<T> memoryStore = new MemoryStore<T>(this.configuration);
		
		return (TimestampStore<T>) new DefaultTimestampStore<>(this.configuration,memoryStore,diskStore);
	}
}
