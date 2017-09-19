package com.mxd.store.task;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.mxd.store.StoreIndex;
import com.mxd.store.StoreIndex.HistoryIndex;
import com.mxd.store.io.FileCache;

public class DiskTask {
	
	protected long id;
	
	protected long minTimestamp;
	
	protected long maxTimestamp;
	
	protected long timestamp;
	
	protected StoreIndex storeIndex;
	
	protected boolean isMiddle;
	
	protected int storeUnitSize;
	
	protected int buffsetObjectSize;
	
	public DiskTask(long id,long minTimestamp, long maxTimestamp,long timestamp,StoreIndex storeIndex,boolean isMiddle,int storeUnitSize,int buffsetObjectSize) {
		super();
		this.id = id;
		this.minTimestamp = minTimestamp;
		this.maxTimestamp = maxTimestamp;
		this.timestamp = timestamp;
		this.storeIndex = storeIndex;
		this.isMiddle = isMiddle;
		this.storeUnitSize = storeUnitSize;
		this.buffsetObjectSize = buffsetObjectSize;
	}
	
	/**
	 * 获取索引
	 * @return
	 */
	protected HistoryIndex getIndex(){
		
		return this.storeIndex.getHistoryIndex(this.timestamp, this.id);
		
	}
	
	/**
	 *	读取数据部分 
	 */
	protected void readTask(long id,long timestamp,ByteBuffer buffer){
		//跳过数据部分
		buffer.position(buffer.position()+this.storeUnitSize);
	}
	
	protected void readFromBuffer(ByteBuffer buffer){
		while(buffer.remaining()>8){	//剩余字节大于8,后8个字节是下一缓冲区的位置,不在这里读
			long id = buffer.getLong();	//读出id
			long timestamp = buffer.getLong();	//读出时间戳
			if(id==0||timestamp==0){
				//跳过该区域
				buffer.position(buffer.position()+this.storeUnitSize);
				continue;
			}
			if(!isMiddle){	//是否是中间文件,是的话就不用判断时间戳了
				if(timestamp<minTimestamp||timestamp>maxTimestamp){	//判断时间是否在该区域内
					buffer.position(buffer.position()+this.storeUnitSize);
					continue;
				}
			}
			this.readTask(id,timestamp,buffer);
		}
	}
	
	protected boolean beforeRead(int len){
		return len > 0;
	}
	
	public void read() throws Exception{
		HistoryIndex historyIndex = getIndex();
		if(historyIndex!=null){	//索引可能在的情况下才去寻找
			try {
				RandomAccessFile dataRaf = this.storeIndex.getDataFile(this.timestamp, FileCache.READONLY);
				int size = this.buffsetObjectSize*(this.storeUnitSize+16)+8;	//计算一个缓冲区的大小
				long offset =historyIndex.getOffset();	//数据文件中的offset
				int len  = historyIndex.getLen();	//数据个数
				if(beforeRead(len)){
					for(int i = len /this.buffsetObjectSize;i>=0;i--){//根据长度来判断存在几个缓冲区
						ByteBuffer buffer = FileCache.getMappedByteBuffer(dataRaf, FileChannel.MapMode.READ_ONLY, offset,size);
						readFromBuffer(buffer);
						if(i>0){
							offset = buffer.getLong();	//取出下一缓冲区的位置
							if(offset==0){	//等于零的情况是不存在的
								buffer = null;
								break;
							}
						}
						buffer = null;
					}
				}
			} catch (FileNotFoundException e) {
				
			}
		}
	}
}
