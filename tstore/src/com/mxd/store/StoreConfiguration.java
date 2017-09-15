package com.mxd.store;

import java.io.File;
import java.io.Serializable;

import com.mxd.store.common.SerializeStore;
import com.mxd.store.common.Store;

public class StoreConfiguration implements Cloneable,Serializable{
	
	private static final long serialVersionUID = 2179863293236756457L;

	private Store store;
	
	/**
	 * memory.mts最大占用空间,默认64M
	 */
	private int memoryMaxSize = 64*1024*1024;
	
	/**
	 * 对象序列化方法
	 */
	private SerializeStore serializeStore;
	
	/**
	 * 单个存储对象最大大小
	 */
	private int storeUnitSize;
	
	/**
	 * 文件保存路径
	 */
	private String diskPath ="./";
	
	/**
	 * 读取最大线程数
	 */
	private int readThreads = 8;
	
	/**
	 * 插入最大线程数
	 */
	private int insertThreads = 8;
	
	/**
	 * 读取硬盘最大线程数
	 */
	private int readDiskThreads = 16;
	
	/**
	 * 读取超时时间
	 */
	private int readTimeout = 60000;
	
	/**
	 * 存储周期，0小时，1天，2月，
	 */
	private int timeUnit = 1;
	
	/**
	 * 每个存储对象缓冲区大小（存储在硬盘中连续存放的缓冲个数，当缓冲区不够时，将在尾部扩充追加一个diskUnitBufferSize的缓冲区
	 * 这个单位不是字节,是对象数目
	 */
	private int diskUnitBufferSize = 1000;
	
	public StoreConfiguration() {
		super();
	}

	public int getMemoryMaxSize() {
		return memoryMaxSize;
	}

	public String getDiskPath() {
		return diskPath;
	}

	public StoreConfiguration setDiskPath(String diskPath) {
		this.diskPath = diskPath;
		if(this.diskPath!=null){
			this.diskPath = this.diskPath.replace("/", File.separator).replace("\\", File.separator);
			if(!this.diskPath.endsWith(File.separator)){
				this.diskPath = this.diskPath+File.separator;
			}
		}
		return this;
	}

	public int getStoreUnitSize() {
		return this.store.getStoreUnitSize();
	}

	public int getReadThreads() {
		return readThreads;
	}

	public StoreConfiguration setReadThreads(int readThreads) {
		this.readThreads = readThreads;
		return this;
	}
	
	public int getInsertThreads() {
		return insertThreads;
	}

	public StoreConfiguration setInsertThreads(int insertThreads) {
		this.insertThreads = insertThreads;
		return this;
	}

	public int getReadDiskThreads() {
		return readDiskThreads;
	}
	public StoreConfiguration setReadDiskThreads(int readDiskThreads) {
		this.readDiskThreads = readDiskThreads;
		return this;
	}
	
	public int getReadTimeout() {
		return readTimeout;
	}

	public StoreConfiguration setReadTimeout(int readTimeout) {
		this.readTimeout = readTimeout;
		return this;
	}
	
	public int getDiskUnitBufferSize() {
		return diskUnitBufferSize;
	}

	public StoreConfiguration setDiskUnitBufferSize(int diskUnitBufferSize) {
		this.diskUnitBufferSize = diskUnitBufferSize;
		return this;
	}
	
	public int getTimeUnit() {
		return timeUnit;
	}

	public StoreConfiguration setTimeUnit(int timeUnit) {
		this.timeUnit = timeUnit;
		return this;
	}
	
	public Store getStore() {
		return store;
	}
	public StoreConfiguration setStore(Store store) {
		this.store = store;
		this.serializeStore = new SerializeStore(store);
		return this;
	}
	
	public SerializeStore getSerializeStore(){
		return this.serializeStore;
	}

	@Override
	public String toString() {
		return "StoreConfiguration [memoryMaxSize=" + memoryMaxSize + ", serializeStore=" + serializeStore
				+ ", storeUnitSize=" + storeUnitSize + ", diskPath=" + diskPath + ", readThreads=" + readThreads
				+ ", insertThreads=" + insertThreads + ", readDiskThreads=" + readDiskThreads + ", readTimeout="
				+ readTimeout + ", timeUnit=" + timeUnit + ", diskUnitBufferSize=" + diskUnitBufferSize + "]";
	}

	public StoreConfiguration get(){
		try {
			return (StoreConfiguration) clone();
		} catch (CloneNotSupportedException e) {
			return null;
		}
	}
	
}
