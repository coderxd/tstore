package com.mxd.store;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
import com.mxd.store.common.SerializeStore;
import com.mxd.store.common.StoreResult;
import com.mxd.store.common.StoreUnit;
import com.mxd.store.io.FileCache;
import com.mxd.store.task.DiskReadCountTask;
import com.mxd.store.task.DiskReadTask;

public class DiskStore extends TimestampStore{
	
	private static Logger logger = LoggerFactory.getLogger(DiskStore.class);
	
	private StoreIndex storeIndex;
	
	private StoreConfiguration configuration;
	
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
	
	public DiskStore(StoreConfiguration configuration,StoreIndex storeIndex) {
		super();
		this.storeIndex = storeIndex;
		this.configuration = configuration;
		this.storeUnitSize = this.configuration.getStoreUnitSize();
		this.buffsetObjectSize = this.configuration.getDiskUnitBufferSize();
		this.threadPool = Executors.newFixedThreadPool(this.configuration.getReadDiskThreads());
	}
	
	@Override
	public SaveStatus insert(StoreUnit storeUnit) {
		return insert(storeUnit.getId(),Arrays.asList(storeUnit));
	}
	
	@Override
	public SaveStatus insert(long id,List<StoreUnit> storeUnits) {
		/**
		 * 在文件中数据格式类似于这样（会根据id计算出一个int型的id,这个id自增,从0开始，保存在relations.ts中）
		 * 索引文件：[偏移量(offset long),下一次写入位置(long),在数据文件中的个数]共20字节，找到这个索引位置就是映射后的id*20
		 * 数据文件: 
		 *		[id,timestamp,column1,column2,...,columnN,
		 *		 id,timestamp,column1,column2,...,columnN,
		 *		 ...,
		 *		 id,timestamp,column1,column2,...,columnN,//这里的个数始终等于buffsetObjectSize个，不管有没有数据
		 *		 下一缓冲区位置(long)	为0时就代表没有下一个缓冲区了
		 *		]
		 */
		long index = this.storeIndex.getIndex(id);
		try {
			String lastKey = null;
			RandomAccessFile raf = null;
			ByteBuffer indexBuffer = null;
			RandomAccessFile dataRaf = null;
			ByteBuffer dataBuffer = null;
			int len = 0;
			long offset = 0;
			//计算一个缓冲区的大小
			int size =(this.storeHeadSize+this.storeUnitSize)*this.buffsetObjectSize;
			for (StoreUnit storeUnit : storeUnits) {
				//根据timestamp计算出文件名
				String key = this.storeIndex.getTimestampKey(storeUnit.getTimestamp());
				if(!key.equals(lastKey)){
					if(raf!=null){
						FileCache.close(indexBuffer);
						FileCache.close(dataBuffer);
						indexBuffer = null;
						dataBuffer = null;
					}
					raf = this.storeIndex.getIndexFile(storeUnit.getTimestamp(), FileCache.READ_WRITE);
					indexBuffer = FileCache.getMappedByteBuffer(raf, FileChannel.MapMode.READ_WRITE, index*20, 20);
					indexBuffer.getLong();
					offset = indexBuffer.getLong();
					dataRaf = this.storeIndex.getDataFile(storeUnit.getTimestamp(), FileCache.READ_WRITE);
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
				//当目前可用空间就剩8字节时,代表该缓冲区已满
				if(dataBuffer.remaining()==8){	
					offset = dataRaf.length();
					dataBuffer.putLong(offset);
					dataRaf.setLength(offset+size+8);	//在文件尾部扩一个缓冲区
					FileCache.close(dataBuffer);
					dataBuffer = FileCache.getMappedByteBuffer(dataRaf, FileChannel.MapMode.READ_WRITE, offset,size+8);
				}
				dataBuffer.putLong(storeUnit.getId());	//写入id
				dataBuffer.putLong(storeUnit.getTimestamp());	//写入时间
				dataBuffer.put(storeUnit.getData());	//写入columns
				indexBuffer.position(8);
				indexBuffer.putLong(offset+dataBuffer.position());//写入下一位置
				indexBuffer.putInt(++len);	//个数+1
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
	/**
	 * 根据开始和结束时间计算应该在哪些文件中
	 * @param minTimestamp
	 * @param maxTimestamp
	 * @return
	 */
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
	public StoreResult find(long id, long minTimestamp, long maxTimestamp) {
		long begin = System.currentTimeMillis();
		
		//找出所有可能存在的文件
		List<Long> timestampes = this.getTimestampes(minTimestamp,maxTimestamp);
		List<StoreUnit> resultList = new ArrayList<>(); 
		List<FutureTask<StoreResult>> tasks = new ArrayList<>();
		//每个文件占一个线程去读
		for (int i = 0,size = timestampes.size(); i < size; i++) {
			long timestamp = timestampes.get(i);
			DiskReadTask diskReadTask = new DiskReadTask(id,minTimestamp, maxTimestamp, timestamp, this.storeIndex, i>0&&i+1<size,this.storeUnitSize,this.buffsetObjectSize);
			FutureTask<StoreResult> task = new FutureTask<StoreResult>(diskReadTask);
			this.threadPool.submit(task);
			tasks.add(task);
		}
		for (FutureTask<StoreResult> futureTask : tasks) {
			try {
				StoreResult taskResult = futureTask.get(this.configuration.getReadTimeout(), TimeUnit.MILLISECONDS);
				resultList.addAll(taskResult.getData());
			} catch (Exception e) {
				throw new RuntimeException(e);
			} 
		}
		return new StoreResult(resultList, (int)(System.currentTimeMillis()-begin));
	}

	@Override
	public StoreResult findCount(long id, long minTimestamp, long maxTimestamp) {
		long begin = System.currentTimeMillis();
		long count = 0;
		List<Long> timestampes = this.getTimestampes(minTimestamp,maxTimestamp);
		List<FutureTask<Long>> tasks = new ArrayList<>();
		for (int i = 0,size = timestampes.size(); i < size; i++) {
			long timestamp = timestampes.get(i);
			DiskReadCountTask diskReadCountTask = new DiskReadCountTask(id, minTimestamp, maxTimestamp, timestamp, this.storeIndex, i>0&&i+1<size,this.storeUnitSize,this.buffsetObjectSize);
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
		return new StoreResult(count, (int)(System.currentTimeMillis()-begin));
	}

	@Override
	public SerializeStore getSerializeStore() {
		return this.configuration.getSerializeStore();
	}

	@Override
	public SaveStatus insert(List<StoreUnit> storeUnits) {
		throw new UnsupportedOperationException();
	}
}
