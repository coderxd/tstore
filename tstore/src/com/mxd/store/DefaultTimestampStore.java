package com.mxd.store;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mxd.store.task.InsertTask;
import com.mxd.store.task.ReadCountTask;
import com.mxd.store.task.ReadTask;

public class DefaultTimestampStore<T> extends TimestampStore<T>{
	
	private static Logger logger = LoggerFactory.getLogger(DefaultTimestampStore.class);
	
	private MemoryStore<T> memoryStore;
	
	private DiskStore<T> diskStore;
	
	private ExecutorService threadPool;
	
	private ThreadPoolExecutor insertThreadPool;
	
	private StoreConfiguration configuration;
	
	private ReentrantLock flushLock = new ReentrantLock();
	
	public DefaultTimestampStore(StoreConfiguration configuration,MemoryStore<T> memoryStore, DiskStore<T> diskStore) {
		super();
		this.memoryStore = memoryStore;
		this.diskStore = diskStore;
		this.threadPool = Executors.newFixedThreadPool(configuration.getReadThreads());
		this.insertThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(configuration.getInsertThreads());
		this.configuration = configuration;
	}
	@Override
	public SaveStatus insert(StoreUnit<T> storeUnit) {
		while(insertThreadPool.getTaskCount() - insertThreadPool.getCompletedTaskCount() >100){
			try {
				Thread.sleep(50L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.insertThreadPool.submit(new InsertTask<T>(this,storeUnit));
		return SaveStatus.SUCCESS;
	}

	@Override
	public SaveStatus insert(long id,List<StoreUnit<T>> storeUnits) {
		for (StoreUnit<T> storeUnit : storeUnits) {
			this.insert(storeUnit);
		}
		return SaveStatus.SUCCESS;
	}

	@Override
	public StoreResult<T> find(long id, long minTimestamp, long maxTimestamp) {
		long begin = System.currentTimeMillis();
		FutureTask<StoreResult<T>> memoryTask = new FutureTask<StoreResult<T>>(new ReadTask<T>(this.memoryStore, id, minTimestamp, maxTimestamp));
		FutureTask<StoreResult<T>> diskTask = new FutureTask<StoreResult<T>>(new ReadTask<T>(this.diskStore, id, minTimestamp, maxTimestamp));
		this.threadPool.submit(diskTask);
		this.threadPool.submit(memoryTask);
		StoreResult<T> result = null;
		try {
			StoreResult<T> diskResult = diskTask.get(this.configuration.getReadTimeout(),TimeUnit.MILLISECONDS);
			if(logger.isDebugEnabled()){
				logger.debug("read disk task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,diskResult.getSize(),diskResult.getConsuming());
			}
			StoreResult<T> memoryResult = memoryTask.get(this.configuration.getReadTimeout(),TimeUnit.MILLISECONDS);
			if(logger.isDebugEnabled()){
				logger.debug("read memory task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,memoryResult.getSize(),memoryResult.getConsuming());
			}
			List<StoreUnit<T>> diskList = diskResult.getData();
			diskList.addAll(memoryResult.getData());
			result = new StoreResult<>(diskList, (int)(System.currentTimeMillis()-begin),true);
			logger.info("read task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,result.getSize(),result.getConsuming());
		} catch (Exception e) {
			logger.error("read error",e);
			throw new RuntimeException(e);
		} 
		return result;
	}
	
	@Override
	public StoreResult<T> findCount(long id, long minTimestamp, long maxTimestamp) {
		StoreResult<T> result;
		long begin = System.currentTimeMillis();
		FutureTask<StoreResult<T>> memoryTask = new FutureTask<StoreResult<T>>(new ReadCountTask<T>(this.memoryStore, id, minTimestamp, maxTimestamp));
		FutureTask<StoreResult<T>> diskTask = new FutureTask<StoreResult<T>>(new ReadCountTask<T>(this.diskStore, id, minTimestamp, maxTimestamp));
		this.threadPool.submit(diskTask);
		this.threadPool.submit(memoryTask);
		try {
			StoreResult<T> diskResult = diskTask.get(this.configuration.getReadTimeout(),TimeUnit.MILLISECONDS);
			if(logger.isDebugEnabled()){
				logger.debug("read count disk task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,diskResult.getSize(),diskResult.getConsuming());	
			}
			StoreResult<T> memoryResult = memoryTask.get(this.configuration.getReadTimeout(),TimeUnit.MILLISECONDS);
			if(logger.isDebugEnabled()){
				logger.info("read count memoru task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,memoryResult.getSize(),memoryResult.getConsuming());	
			}
			result = new StoreResult<>(diskResult.getSize() + memoryResult.getSize(), (int)(System.currentTimeMillis()-begin));
			logger.info("read count task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,result.getSize(),result.getConsuming());
		} catch (Exception e) {
			logger.error("read error",e);
			throw new RuntimeException(e);
		} 
		return result;
	}

	public MemoryStore<T> getMemoryStore() {
		return memoryStore;
	}
	/**
	 * 将内存数据持久化到硬盘
	 */
	public void flush(){
		flushLock.lock();
		try {
			if(memoryStore.isOverflow()){
				logger.info("ready flush");
				long begin = System.currentTimeMillis();
				Map<Long,List<StoreUnit<T>>> maps = this.memoryStore.findAll();
				long size = 0;
				for (Entry<Long, List<StoreUnit<T>>> entry : maps.entrySet()) {
					List<StoreUnit<T>> list = entry.getValue();
					long id = entry.getKey();
					this.diskStore.insert(id,list);
					size+=list.size();
				}
				this.memoryStore.clear();
				logger.info("flush finish,count:{},time:{}",size,System.currentTimeMillis()-begin);
			}
		} catch (Exception e) {
			logger.error("flush error",e);
		} finally {
			flushLock.unlock();
		}
	}

}
