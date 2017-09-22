package com.mxd.store;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;
import com.mxd.store.common.SerializeStore;
import com.mxd.store.common.StoreResult;
import com.mxd.store.common.StoreUnit;
import com.mxd.store.io.FileCache;

public class MemoryStore extends TimestampStore{
	
	private int maxSize;
	/**
	 * 每个存储对象占用大小（不包括id和timestamp）
	 */
	private int storeUnitSize;
	
	/**
	 * 每个存储对象头部大小
	 */
	private int storeHeadSize = 16;	//（id 8字节）+（时间戳 8字节）
	
	private ByteBuffer buffer;
	
	private StoreConfiguration configuration;
	
	private ReentrantLock lock = new ReentrantLock();
	
	public MemoryStore(StoreConfiguration configuration) throws IOException {
		super();
		this.maxSize = configuration.getMemoryMaxSize()+4;
		this.storeUnitSize = configuration.getStoreUnitSize();
		this.configuration = configuration;
		this.init();
	}
	
	private void init() throws IOException{
		RandomAccessFile raf = FileCache.getFile(FileCache.READ_WRITE,configuration.getDiskPath()+"memory.mts");
		buffer = FileCache.getMappedByteBuffer(raf,FileChannel.MapMode.READ_WRITE, 0, this.maxSize);
		int position = this.buffer.getInt();
		this.buffer.position(position ==0 ? 4 :position);
	}
	
	@Override
	public SaveStatus insert(StoreUnit storeUnit) {
		this.lock.lock();
		try {
			if(this.buffer.remaining() < this.storeUnitSize+this.storeHeadSize){
				return SaveStatus.OVERFLOW;
			}
			this.buffer.putLong(storeUnit.getId());
			this.buffer.putLong(storeUnit.getTimestamp());
			this.buffer.put(storeUnit.getData());
			this.buffer.putInt(0, this.buffer.position());
			return SaveStatus.SUCCESS;
		} catch(Exception e){
			e.printStackTrace();
			return SaveStatus.EXCEPTION;
		}finally{
			this.lock.unlock();
		}
	}

	@Override
	public SaveStatus insert(long id,List<StoreUnit> storeUnits) {
		this.lock.lock();
		try {
			if(this.buffer.remaining() <(this.storeUnitSize+this.storeHeadSize)*storeUnits.size()){
				return SaveStatus.OVERFLOW;
			}
			for (StoreUnit storeUnit : storeUnits) {
				this.buffer.putLong(id);
				this.buffer.putLong(storeUnit.getTimestamp());
				this.buffer.put(storeUnit.getData());
			}
			this.buffer.putInt(0, this.buffer.position());
			return SaveStatus.SUCCESS;
		} finally{
			this.lock.unlock();
		}
	}
	@Override
	public SaveStatus insert(List<StoreUnit> storeUnits) {
		throw new UnsupportedOperationException();
	}

	@Override
	public StoreResult find(long id, long minTimestamp, long maxTimestamp) {
		long begin = System.currentTimeMillis();
		ByteBuffer temp = getTempBuffer();
		List<StoreUnit> data = new ArrayList<>();
		while(temp.remaining()>=this.storeUnitSize+this.storeHeadSize){
			boolean skipTimestamp = true;
			boolean skipStoreUnit = true;
			if(temp.getLong()==id){
				long timestamp = temp.getLong();
				skipTimestamp = false;
				if(timestamp>=minTimestamp&&timestamp<=maxTimestamp){
					byte[] bytes = new byte[this.storeUnitSize];
					temp.get(bytes);
					data.add(new StoreUnit(timestamp, id,bytes));
					skipStoreUnit = false;
				}
			}
			if(skipTimestamp){
				skipTimestamp(temp);
			}
			if(skipStoreUnit){
				skipStoreUnit(temp);
			}
		}
		temp = null;
		return new StoreResult(data, (int)(System.currentTimeMillis() - begin),true);
	}
	
	@Override
	public StoreResult findCount(long id, long minTimestamp, long maxTimestamp) {
		long begin = System.currentTimeMillis();
		long count = 0;
		ByteBuffer temp = getTempBuffer();
		while(temp.remaining()>=this.storeUnitSize){
			if(temp.getLong()==id){
				long timestamp = temp.getLong();
				if(timestamp>=minTimestamp&&timestamp<=maxTimestamp){
					count++;
				}
			}else{
				skipTimestamp(temp);
			}
			skipStoreUnit(temp);
		}
		temp = null;
		return new StoreResult(count,(int)(System.currentTimeMillis()-begin));
	}
	
	private void skipTimestamp(ByteBuffer buffer){
		buffer.position(buffer.position()+8);
	}
	private void skipStoreUnit(ByteBuffer buffer){
		buffer.position(buffer.position()+this.storeUnitSize);
	}
	
	public Map<Long,List<StoreUnit>> findAll() throws IOException{
		ByteBuffer buffer = getTempBuffer();
		Map<Long,List<StoreUnit>> maps = new HashMap<>();
		while(buffer.remaining() >= this.storeUnitSize+this.storeHeadSize){
			long id = buffer.getLong();
			if(id==0){
				break;
			}
			List<StoreUnit> list = maps.get(id);
			if(list==null){
				list = new ArrayList<>();
				maps.put(id, list);
			}
			long timestamp = buffer.getLong();
			byte[] bytes = new byte[this.storeUnitSize];
			buffer.get(bytes);
			list.add(new StoreUnit(timestamp, id,bytes));
		}
		for (Entry<Long, List<StoreUnit>> entry : maps.entrySet()) {
			Collections.sort(entry.getValue(),new Comparator<StoreUnit>() {
				@Override
				public int compare(StoreUnit o1, StoreUnit o2) {
					return Long.valueOf(o1.getTimestamp()).compareTo(o2.getTimestamp());
				}
			});
		}
		return maps;
	}
	
	public boolean isOverflow(){
		this.lock.lock();
		try {
			return this.buffer.position() > this.storeUnitSize+this.storeHeadSize;
		} finally{
			this.lock.unlock();
		}
	}
	
	public ByteBuffer getTempBuffer(){
		this.lock.lock();
		int position  = this.buffer.getInt(0);
		try {
			if(position>4){
				RandomAccessFile raf = FileCache.getFile(FileCache.READONLY, this.configuration.getDiskPath()+"memory.mts");
				ByteBuffer result =  FileCache.getMappedByteBuffer(raf,FileChannel.MapMode.READ_ONLY, 0, position);
				result.position(4);
				return result;
			}
		} catch (Exception e) {
			
		} finally{
			this.lock.unlock();
		}
		
		return ByteBuffer.allocate(0);
		
	}
	
	public void readyFlush(){
		this.lock.lock();
	}
	public void flushFinally(){
		try {
			this.buffer.clear();
			this.buffer.position(0);
			this.buffer.putInt(0);
		} finally{
			this.lock.unlock();	
		}
	}

	@Override
	public SerializeStore getSerializeStore() {
		return this.configuration.getSerializeStore();
	}

}
