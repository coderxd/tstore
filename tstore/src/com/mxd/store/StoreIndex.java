package com.mxd.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mxd.store.io.FileCache;

/**
 * 存储索引
 * @author mxd
 *
 */
public class StoreIndex {
	
	private Logger logger = LoggerFactory.getLogger(StoreIndex.class);
	
	private String storeIndexPath;
	
	private SimpleDateFormat sdf = null;
	
	private Map<Long, Integer> relationMap = new HashMap<>();
	
	private int maxId = 0;
	
	private ReentrantLock indexLock = new ReentrantLock();
	
	private StoreConfiguration configuration;
	
	public StoreIndex(StoreConfiguration storeConfiguration) {
		super();
		this.configuration = storeConfiguration;
		this.storeIndexPath = storeConfiguration.getDiskPath();
		this.init();
	}
	
	/**
	 * 根据id获取索引值
	 * @param id
	 * @return
	 */
	public int getIndex(long id){
		Integer index = relationMap.get(Long.valueOf(id));
		if(index!=null){
			return index.intValue();
		}
		indexLock.lock();
		this.maxId++;
		try {
			index = this.maxId;
			RandomAccessFile raf = FileCache.getFile(FileCache.READ_WRITE,this.storeIndexPath+"relation.rs");
			raf.seek(raf.length());
			raf.writeLong(id);
			raf.writeInt(index);
			this.relationMap.put(Long.valueOf(id), index);
			return index;
		} catch (Exception e) {
			logger.error("write index error",e);
			return -1;
		} finally{
			indexLock.unlock();
		}
	}
	
	private void init(){
		try {
			String pattern = "yyyyMM";
			switch(this.configuration.getTimeUnit()){
				case 0: 
					pattern = "yyyyMMddHH";
					break;
				case 1:
					pattern = "yyyyMMdd";
					break;
			}
			this.sdf = new SimpleDateFormat(pattern);
			logger.info("index file use {} format",pattern);
			
			//把id映射关系读入到内存中
			RandomAccessFile raf = FileCache.getFile(FileCache.READONLY,this.storeIndexPath+"relation.rs");
			long length = raf.length();
			while(raf.getFilePointer()<length){
				long id = raf.readLong();
				int index = raf.readInt();
				this.relationMap.put(id,index);
				this.maxId = Math.max(index, this.maxId);
			}
			logger.info("index load finish,count:"+this.relationMap.size());
		} catch(FileNotFoundException e){
			logger.warn("index file is not found");
		}catch (Exception e) {
			logger.error("init index error",e);
		}
	}
	
	public String getTimestampKey(long timestamp){
		String fileName = null;
		synchronized (this.sdf) {
			fileName = sdf.format(new Date(timestamp*1000));
		}
		return fileName;
	}
	
	public HistoryIndex getHistoryIndex(long timestamp,long id){
		try {
			int index = this.getIndex(id);
			RandomAccessFile raf = getIndexFile(timestamp,FileCache.READONLY);
			long indexOffset = index*20;
			if(raf.length() >=indexOffset+20){
				ByteBuffer buffer = FileCache.getMappedByteBuffer(raf, FileChannel.MapMode.READ_ONLY, indexOffset, 20);
				return new HistoryIndex(buffer.getLong(), buffer.getLong(), buffer.getInt());
			}
		} catch (IOException e) {
			
		}
		return null;
	}
	
	public RandomAccessFile getIndexFile(long timestamp,String mode) throws FileNotFoundException{
		String fileName = null;
		synchronized (this.sdf) {
			fileName = sdf.format(new Date(timestamp*1000));
		}
		return getIndexFile(fileName, mode);
	}
	public RandomAccessFile getIndexFile(String key,String mode) throws FileNotFoundException{
		return FileCache.getFile(mode, this.storeIndexPath+key+".tsi");
	}
	
	public RandomAccessFile getDataFile(long timestamp,String mode) throws FileNotFoundException{
		String fileName = null;
		synchronized (this.sdf) {
			fileName = sdf.format(new Date(timestamp*1000));
		}
		return getDataFile(fileName, mode);
	}
	
	public RandomAccessFile getDataFile(String key,String mode) throws FileNotFoundException{
		return FileCache.getNewFile(mode, this.storeIndexPath+key+".ts");
	}
	
	public static class HistoryIndex{
		
		private long offset;
		
		private long nextPosition;
		
		private int len;
		
		public HistoryIndex(long offset, long nextPosition, int len) {
			super();
			this.offset = offset;
			this.nextPosition = nextPosition;
			this.len = len;
		}

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		public long getNextPosition() {
			return nextPosition;
		}

		public void setNextPosition(long nextPosition) {
			this.nextPosition = nextPosition;
		}

		public int getLen() {
			return len;
		}

		public void setLen(int len) {
			this.len = len;
		}
	}
}
