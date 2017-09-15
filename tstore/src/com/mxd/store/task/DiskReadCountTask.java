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
			if(!isMiddle){	//是否是中间文件,是的话就不用判断时间戳了
				if(timestamp<minTimestamp||timestamp>maxTimestamp){	//判断时间是否在该区域内
					mappedByteBuffer.position(mappedByteBuffer.position()+this.storeUnitSize);
					continue;
				}
			}
			count++;
			
			//跳过数据部分
			mappedByteBuffer.position(mappedByteBuffer.position()+this.storeUnitSize);
		}
		return count;
	}
	

	@Override
	public Long call() throws Exception {
		long count = 0;
		try {
			//取出索引文件
			RandomAccessFile indexRaf = this.storeIndex.getIndexFile(this.timestamp,FileCache.READONLY);
			if(indexRaf.length()>=this.index*20+20){	//索引可能在的情况下才去寻找
				MappedByteBuffer indexBuffer = FileCache.getMappedByteBuffer(indexRaf, FileChannel.MapMode.READ_ONLY, this.index*20, 20);
				long offset = indexBuffer.getLong();	//数据文件中的offset
				indexBuffer.getLong();	//下一位置(不用管)
				int len  = indexBuffer.getInt();	//数据个数
				FileCache.close(indexBuffer);
				if(isMiddle){	//如果是中间的数据文件，则直接读出count返回即可
					count = len;
				}else if(len>0){
					//取出数据文件
					RandomAccessFile dataRaf = this.storeIndex.getDataFile(this.timestamp, FileCache.READONLY);
					int size = this.buffsetObjectSize*(this.storeUnitSize+16)+8;	//计算一个缓冲区的大小
					for(int i = len /this.buffsetObjectSize;i>=0;i--){	//根据长度来判断存在几个缓冲区
						//取出一个缓冲区
						MappedByteBuffer buffer = FileCache.getMappedByteBuffer(dataRaf, FileChannel.MapMode.READ_ONLY, offset,size);
						count +=readFromBuffer(buffer);	//从缓冲区读取数据个数
						if(i>0){
							offset = buffer.getLong();	//取出下一缓冲区的位置
							if(offset==0){	//等于零的情况是不存在的
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
