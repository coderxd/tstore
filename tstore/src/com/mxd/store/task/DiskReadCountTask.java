package com.mxd.store.task;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;
import com.mxd.store.StoreIndex;
import com.mxd.store.io.FileCache;

public class DiskReadCountTask implements Callable<Long> {
	
	protected long id;
	
	protected int index;
	
	protected long minTimestamp;
	
	protected long maxTimestamp;
	
	protected long timestamp;
	
	protected StoreIndex storeIndex;
	
	protected boolean isMiddle;
	
	protected int storeUnitSize;
	
	protected int buffsetObjectSize;
	
	public DiskReadCountTask(long id,int index, long minTimestamp, long maxTimestamp,long timestamp,StoreIndex storeIndex,boolean isMiddle,int storeUnitSize,int buffsetObjectSize) {
		super();
		this.id = id;
		this.index = index;
		this.minTimestamp = minTimestamp;
		this.maxTimestamp = maxTimestamp;
		this.timestamp = timestamp;
		this.storeIndex = storeIndex;
		this.isMiddle = isMiddle;
		this.storeUnitSize = storeUnitSize;
		this.buffsetObjectSize = buffsetObjectSize;
	}


	private long readFromBuffer(MappedByteBuffer mappedByteBuffer){
		long count = 0;
		while(mappedByteBuffer.remaining()>8){
			long id = mappedByteBuffer.getLong();
			long timestamp = mappedByteBuffer.getLong();
			if(id==0||timestamp==0){
				mappedByteBuffer.position(mappedByteBuffer.position()+this.storeUnitSize);
				continue;
			}
			if(!isMiddle){
				if(timestamp<minTimestamp||timestamp>maxTimestamp){
					mappedByteBuffer.position(mappedByteBuffer.position()+this.storeUnitSize);
					continue;
				}
			}
			count++;
			System.out.println("count:"+count);
			mappedByteBuffer.position(mappedByteBuffer.position()+this.storeUnitSize);
		}
		return count;
	}
	

	@Override
	public Long call() throws Exception {
		long count = 0;
		try {
			RandomAccessFile indexRaf = this.storeIndex.getIndexFile(this.timestamp,StoreIndex.READONLY);
			if(indexRaf.length()>=this.index*20+20){
				MappedByteBuffer indexBuffer = FileCache.getMappedByteBuffer(indexRaf, FileChannel.MapMode.READ_ONLY, this.index*20, 20);
				long offset = indexBuffer.getLong();
				indexBuffer.getLong();
				int len  = indexBuffer.getInt();
				FileCache.close(indexBuffer);
				if(isMiddle){
					count = len;
				}else if(len>0){
					RandomAccessFile dataRaf = this.storeIndex.getDataFile(this.timestamp, StoreIndex.READONLY);
					int size = this.buffsetObjectSize*(this.storeUnitSize+16)+8;
					for(int i = len /this.buffsetObjectSize;i>=0;i--){
						MappedByteBuffer buffer = FileCache.getMappedByteBuffer(dataRaf, FileChannel.MapMode.READ_ONLY, offset,size);
						count +=readFromBuffer(buffer);
						if(i>0){
							offset = buffer.getLong();
							if(offset==0){
								break;
							}
						}
						FileCache.close(buffer);
					}
				}
			}
		} catch (FileNotFoundException e) {
			
		}
		return count;
	}
}
