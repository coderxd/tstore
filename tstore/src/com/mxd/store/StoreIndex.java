package com.mxd.store;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
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
	
	public static final String READONLY ="r";
	
	public static final String READ_WRITE ="rw";
	
	private SimpleDateFormat sdf = null;
	
	private Map<Long, Integer> relationMap = new HashMap<>();
	
	private int maxId = 0;
	
	private ReentrantLock indexLock = new ReentrantLock();
	
	public StoreIndex(StoreConfiguration storeConfiguration) {
		super();
		this.storeIndexPath = storeConfiguration.getDiskPath();
		String pattern = "yyyyMM";
		switch(storeConfiguration.getTimeUnit()){
			case 0: 
				pattern = "yyyyMMddHH";
				break;
			case 1:
				pattern = "yyyyMMdd";
				break;
		}
		this.sdf = new SimpleDateFormat(pattern);
		logger.info("index file use {} format",pattern);
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
			RandomAccessFile raf = FileCache.getFile(READ_WRITE,this.storeIndexPath+"relation.rs");
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
			RandomAccessFile raf = FileCache.getFile(READONLY,this.storeIndexPath+"relation.rs");
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
	
	public RandomAccessFile getIndexFile(long timestamp,String mode) throws FileNotFoundException{
		String fileName = null;
		synchronized (this.sdf) {
			fileName = sdf.format(new Date(timestamp*1000));
		}
		return FileCache.getFile(mode, this.storeIndexPath+fileName+".tsi");
	}
	public RandomAccessFile getDataFile(long timestamp,String mode) throws FileNotFoundException{
		String fileName = null;
		synchronized (this.sdf) {
			fileName = sdf.format(new Date(timestamp*1000));
		}
		return FileCache.getFile(mode, this.storeIndexPath+fileName+".ts");
	}
}
