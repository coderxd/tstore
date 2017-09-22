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
import com.mxd.store.common.SerializeStore;
import com.mxd.store.common.StoreResult;
import com.mxd.store.common.StoreUnit;
import com.mxd.store.task.FlushTimerTask;
import com.mxd.store.task.InsertTask;
import com.mxd.store.task.ReadCountTask;
import com.mxd.store.task.ReadTask;

public class DefaultTimestampStore extends TimestampStore{
	
	private static Logger logger = LoggerFactory.getLogger(DefaultTimestampStore.class);
	
	private MemoryStore memoryStore;
	
	private DiskStore diskStore;
	
	private ExecutorService threadPool;
	
	private ThreadPoolExecutor insertThreadPool;
	
	private StoreConfiguration configuration;
	
	private ReentrantLock flushLock = new ReentrantLock();
	
	private FlushTimerTask flushTimerTask;
	
	public DefaultTimestampStore(StoreConfiguration configuration,MemoryStore memoryStore, DiskStore diskStore) {
		super();
		this.memoryStore = memoryStore;
		this.diskStore = diskStore;
		this.threadPool = Executors.newFixedThreadPool(configuration.getReadThreads());
		this.insertThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(configuration.getInsertThreads());
		this.configuration = configuration;
		this.flushTimerTask = new FlushTimerTask(this);
		new Thread(this.flushTimerTask).start();
	}
	@Override
	public SaveStatus insert(StoreUnit storeUnit) {
		//防止内存占用过高，导致GC频繁,CPU升高
		while(insertThreadPool.getTaskCount() - insertThreadPool.getCompletedTaskCount() >100){
			try {
				Thread.sleep(50L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		this.insertThreadPool.submit(new InsertTask(this,storeUnit));
		return SaveStatus.SUCCESS;
	}

	@Override
	public SaveStatus insert(long id,List<StoreUnit> storeUnits) {
		for (StoreUnit storeUnit : storeUnits) {
			this.insert(storeUnit);
		}
		return SaveStatus.SUCCESS;
	}

	@Override
	public StoreResult find(long id, long minTimestamp, long maxTimestamp) {
		long begin = System.currentTimeMillis();
		FutureTask<StoreResult> memoryTask = new FutureTask<StoreResult>(new ReadTask(this.memoryStore, id, minTimestamp, maxTimestamp));
		FutureTask<StoreResult> diskTask = new FutureTask<StoreResult>(new ReadTask(this.diskStore, id, minTimestamp, maxTimestamp));
		this.threadPool.submit(diskTask);
		this.threadPool.submit(memoryTask);
		StoreResult result = null;
		try {
			StoreResult diskResult = diskTask.get(this.configuration.getReadTimeout(),TimeUnit.MILLISECONDS);
			if(logger.isDebugEnabled()){
				logger.debug("read disk task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,diskResult.getSize(),diskResult.getConsuming());
			}
			StoreResult memoryResult = memoryTask.get(this.configuration.getReadTimeout(),TimeUnit.MILLISECONDS);
			if(logger.isDebugEnabled()){
				logger.debug("read memory task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,memoryResult.getSize(),memoryResult.getConsuming());
			}
			List<StoreUnit> diskList = diskResult.getData();
			diskList.addAll(memoryResult.getData());
			result = new StoreResult(diskList, (int)(System.currentTimeMillis()-begin),true);
			logger.info("read task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,result.getSize(),result.getConsuming());
		} catch (Exception e) {
			logger.error("read error",e);
			throw new RuntimeException(e);
		} 
		return result;
	}
	
	@Override
	public StoreResult findCount(long id, long minTimestamp, long maxTimestamp) {
		StoreResult result;
		long begin = System.currentTimeMillis();
		FutureTask<StoreResult> memoryTask = new FutureTask<StoreResult>(new ReadCountTask(this.memoryStore, id, minTimestamp, maxTimestamp));
		FutureTask<StoreResult> diskTask = new FutureTask<StoreResult>(new ReadCountTask(this.diskStore, id, minTimestamp, maxTimestamp));
		this.threadPool.submit(diskTask);
		this.threadPool.submit(memoryTask);
		try {
			StoreResult diskResult = diskTask.get(this.configuration.getReadTimeout(),TimeUnit.MILLISECONDS);
			if(logger.isDebugEnabled()){
				logger.debug("read count disk task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,diskResult.getSize(),diskResult.getConsuming());	
			}
			StoreResult memoryResult = memoryTask.get(this.configuration.getReadTimeout(),TimeUnit.MILLISECONDS);
			if(logger.isDebugEnabled()){
				logger.info("read count memory task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,memoryResult.getSize(),memoryResult.getConsuming());	
			}
			result = new StoreResult(diskResult.getSize() + memoryResult.getSize(), (int)(System.currentTimeMillis()-begin));
			logger.info("read count task finish,query {},{}-{},result:{count:{},time:{}ms}",id,minTimestamp,maxTimestamp,result.getSize(),result.getConsuming());
		} catch (Exception e) {
			logger.error("read error",e);
			throw new RuntimeException(e);
		} 
		return result;
	}

	public MemoryStore getMemoryStore() {
		return memoryStore;
	}
	/**
	 * 将memoryStore中的数据写入diskStore中
	 */
	public void flush(){
		flushLock.lock();
		try {
			this.memoryStore.readyFlush();
			logger.info("ready flush");
			long begin = System.currentTimeMillis();
			Map<Long,List<StoreUnit>> maps = this.memoryStore.findAll();
			long size = 0;
			for (Entry<Long, List<StoreUnit>> entry : maps.entrySet()) {
				List<StoreUnit> list = entry.getValue();
				long id = entry.getKey();
				this.diskStore.insert(id,list);
				size+=list.size();
			}
			logger.info("flush finish,count:{},time:{}",size,System.currentTimeMillis()-begin);
		} catch (Exception e) {
			logger.error("flush error",e);
		} finally {
			this.memoryStore.flushFinally();
			flushLock.unlock();
		}
	}
	
	@Override
	public SerializeStore getSerializeStore() {
		return this.configuration.getSerializeStore();
	}
	@Override
	public SaveStatus insert(List<StoreUnit> storeUnits) {
		for (StoreUnit unit : storeUnits) {
			this.insert(unit);
		}
		return SaveStatus.SUCCESS;
	}
	public DiskStore getDiskStore() {
		return diskStore;
	}
}
