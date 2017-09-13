package com.mxd.store;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mxd.store.io.FileCache;
import com.mxd.store.task.DiskReadCountTask;
import com.mxd.store.task.DiskReadTask;

public class DiskStore<T> extends TimestampStore<T>{
	
	private static Logger logger = LoggerFactory.getLogger(DiskStore.class);
	
	private StoreIndex storeIndex;
	
	private StoreConfiguration configuration;
	
	private SerializeStore<T> serializerStore;
	
	private int storeUnitSize;
	
	/**
	 * 每个缓冲区大小(对象数)
	 */
	private int buffsetObjectSize;
	
	/**
	 * 每个存储对象头部大小
	 */
	private int storeHeadSize = 16;	//实际id（8字节）+时间戳（8字节）
	
	private ExecutorService threadPool;
	
	@SuppressWarnings("unchecked")
	public DiskStore(StoreConfiguration configuration,StoreIndex storeIndex) {
		super();
		this.storeIndex = storeIndex;
		this.configuration = configuration;
		this.storeUnitSize = this.configuration.getStoreUnitSize();
		this.buffsetObjectSize = this.configuration.getDiskUnitBufferSize();
		this.serializerStore = (SerializeStore<T>) this.configuration.getSerializeStore();
		this.threadPool = Executors.newFixedThreadPool(this.configuration.getReadDiskThreads());
	}
	
	@Override
	public SaveStatus insert(StoreUnit<T> storeUnit) {
		return insert(storeUnit.getId(),Arrays.asList(storeUnit));
	}
	
	@Override
	public SaveStatus insert(long id,List<StoreUnit<T>> storeUnits) {
		long index = this.storeIndex.getIndex(id);
		try {
			String lastKey = null;
			RandomAccessFile raf = null;
			MappedByteBuffer indexBuffer = null;
			RandomAccessFile dataRaf = null;
			MappedByteBuffer dataBuffer = null;
			int len = 0;
			long offset = 0;
			int size =(this.storeHeadSize+this.storeUnitSize)*this.buffsetObjectSize;
			for (StoreUnit<T> storeUnit : storeUnits) {
				String key = this.storeIndex.getTimestampKey(storeUnit.getTimestamp());
				if(!key.equals(lastKey)){
					if(raf!=null){
						FileCache.close(indexBuffer);
						FileCache.close(dataBuffer);
						indexBuffer = null;
						dataBuffer = null;
					}
					raf = this.storeIndex.getIndexFile(storeUnit.getTimestamp(), StoreIndex.READ_WRITE);
					indexBuffer = FileCache.getMappedByteBuffer(raf, FileChannel.MapMode.READ_WRITE, index*20, 20);
					indexBuffer.getLong();
					offset = indexBuffer.getLong();
					dataRaf = this.storeIndex.getDataFile(storeUnit.getTimestamp(), StoreIndex.READ_WRITE);
					len = indexBuffer.getInt();
					if(len==0){
						offset = dataRaf.length();
						indexBuffer.position(0);
						indexBuffer.putLong(offset);
						indexBuffer.putLong(offset);
						dataRaf.setLength(offset+size+8);
						dataBuffer = FileCache.getMappedByteBuffer(dataRaf, FileChannel.MapMode.READ_WRITE, offset,size+8);
					}else{
						int remainSize = len % this.buffsetObjectSize;
						remainSize = remainSize ==0 ? 0 : (this.buffsetObjectSize - remainSize)*(this.storeHeadSize+this.storeUnitSize);
						dataBuffer = FileCache.getMappedByteBuffer(dataRaf, FileChannel.MapMode.READ_WRITE, offset,remainSize+8);
					}
					
				}
				lastKey = key;
				if(dataBuffer.remaining()==8){
					offset = dataRaf.length();
					dataBuffer.putLong(offset);
					dataRaf.setLength(offset+size+8);
					FileCache.close(dataBuffer);
					dataBuffer = FileCache.getMappedByteBuffer(dataRaf, FileChannel.MapMode.READ_WRITE, offset,size+8);
				}
				//System.out.println(key+"\t"+offset+"\t"+dataBuffer.position()+"\t"+storeUnit.getTimestamp());
				dataBuffer.putLong(storeUnit.getId());
				dataBuffer.putLong(storeUnit.getTimestamp());
				dataBuffer.put(this.serializerStore.encode(storeUnit.getData()));
				indexBuffer.position(8);
				indexBuffer.putLong(dataBuffer.remaining()==8 ? offset-8:offset+dataBuffer.position());
				indexBuffer.putInt(++len);
			}
			if(indexBuffer!=null){
				FileCache.close(indexBuffer);
				FileCache.close(dataBuffer);
				indexBuffer = null;
				dataBuffer = null;
			}
		} catch (Exception e) {
			logger.error("disk insert error,",e);
			return SaveStatus.EXCEPTION;
		}
		return SaveStatus.SUCCESS;
	}
	
	private List<Long> getTimestampes(long minTimestamp,long maxTimestamp){
		int[] fields = new int[]{Calendar.HOUR_OF_DAY,Calendar.DAY_OF_MONTH,Calendar.MONTH};
		int field = fields[this.configuration.getTimeUnit()];
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date(minTimestamp*1000));
		List<Long> result = new ArrayList<>();
		result.add(minTimestamp);
		for(;calendar.getTimeInMillis()/1000<maxTimestamp;){
			calendar.add(field, 1);
			result.add(calendar.getTimeInMillis()/1000);
		}
		return result;
	}
	
	@Override
	public StoreResult<T> find(long id, long minTimestamp, long maxTimestamp) {
		long begin = System.currentTimeMillis();
		List<Long> timestampes = this.getTimestampes(minTimestamp,maxTimestamp);
		int index = this.storeIndex.getIndex(id);
		List<StoreUnit<T>> resultList = new ArrayList<>(); 
		List<FutureTask<StoreResult<T>>> tasks = new ArrayList<>();
		for (int i = 0,size = timestampes.size(); i < size; i++) {
			long timestamp = timestampes.get(i);
			DiskReadTask<T> diskReadTask = new DiskReadTask<T>(id, index, minTimestamp, maxTimestamp, timestamp, this.storeIndex, i>0&&i+1<size,this.storeUnitSize,this.buffsetObjectSize,this.serializerStore);
			FutureTask<StoreResult<T>> task = new FutureTask<StoreResult<T>>(diskReadTask);
			this.threadPool.submit(task);
			tasks.add(task);
		}
		for (FutureTask<StoreResult<T>> futureTask : tasks) {
			try {
				StoreResult<T> taskResult = futureTask.get(this.configuration.getReadTimeout(), TimeUnit.MILLISECONDS);
				resultList.addAll(taskResult.getData());
			} catch (Exception e) {
				throw new RuntimeException(e);
			} 
		}
		return new StoreResult<>(resultList, (int)(System.currentTimeMillis()-begin));
	}

	@Override
	public StoreResult<T> findCount(long id, long minTimestamp, long maxTimestamp) {
		long begin = System.currentTimeMillis();
		long count = 0;
		List<Long> timestampes = this.getTimestampes(minTimestamp,maxTimestamp);
		int index = this.storeIndex.getIndex(id);
		List<FutureTask<Long>> tasks = new ArrayList<>();
		for (int i = 0,size = timestampes.size(); i < size; i++) {
			long timestamp = timestampes.get(i);
			DiskReadCountTask diskReadCountTask = new DiskReadCountTask(id, index, minTimestamp, maxTimestamp, timestamp, this.storeIndex, i>0&&i+1<size,this.storeUnitSize,this.buffsetObjectSize);
			FutureTask<Long> task = new FutureTask<Long>(diskReadCountTask);
			this.threadPool.submit(task);
			tasks.add(task);
		}
		for (FutureTask<Long> futureTask : tasks) {
			try {
				Long value = futureTask.get(this.configuration.getReadTimeout(), TimeUnit.MILLISECONDS);
				count += value.longValue();
			} catch (Exception e) {
				throw new RuntimeException(e);
			} 
		}
		return new StoreResult<>(count, (int)(System.currentTimeMillis()-begin));
	}
}
