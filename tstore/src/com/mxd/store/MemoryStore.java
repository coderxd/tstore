package com.mxd.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;

import com.mxd.store.io.FileCache;

public class MemoryStore<T> extends TimestampStore<T>{
	
	private int maxSize;
	/**
	 * 每个存储对象占用大小（不包括id和timestamp）
	 */
	private int storeUnitSize;
	
	/**
	 * 每个存储对象头部大小
	 */
	private int storeHeadSize = 16;	//（id 8字节）+（时间戳 8字节）
	
	private MappedByteBuffer buffer;
	
	private SerializeStore<T> serializerStore;
	
	private StoreConfiguration configuration;
	
	private ReentrantLock lock = new ReentrantLock();
	
	@SuppressWarnings("unchecked")
	public MemoryStore(StoreConfiguration configuration) throws IOException {
		super();
		this.maxSize = configuration.getMemoryMaxSize()+4;
		this.storeUnitSize = configuration.getStoreUnitSize();
		this.serializerStore = (SerializeStore<T>) configuration.getSerializeStore();
		this.configuration = configuration;
		buffer = FileCache.getMappedByteBuffer(configuration.getDiskPath()+"memory.mts",StoreIndex.READ_WRITE, 0, this.maxSize);
		this.init();
	}
	
	private void init(){
		int position = this.buffer.getInt();
		this.buffer.position(position ==0 ? 4 :position);
	}
	
	@Override
	public SaveStatus insert(StoreUnit<T> storeUnit) {
		this.lock.lock();
		try {
			if(this.buffer.remaining() < this.storeUnitSize+this.storeHeadSize){
				return SaveStatus.OVERFLOW;
			}
			this.buffer.putLong(storeUnit.getId());
			this.buffer.putLong(storeUnit.getTimestamp());
			this.buffer.put(this.serializerStore.encode(storeUnit.getData()));
			this.buffer.putInt(0, this.buffer.position());
			return SaveStatus.SUCCESS;
		} finally{
			this.lock.unlock();
		}
	}

	@Override
	public SaveStatus insert(long id,List<StoreUnit<T>> storeUnits) {
		this.lock.lock();
		try {
			if(this.buffer.remaining() <(this.storeUnitSize+this.storeHeadSize)*storeUnits.size()){
				return SaveStatus.OVERFLOW;
			}
			for (StoreUnit<T> storeUnit : storeUnits) {
				this.buffer.putLong(id);
				this.buffer.putLong(storeUnit.getTimestamp());
				this.buffer.put(this.serializerStore.encode(storeUnit.getData()));
			}
			this.buffer.putInt(0, this.buffer.position());
			return SaveStatus.SUCCESS;
		} finally{
			this.lock.unlock();
		}
	}

	@Override
	public StoreResult<T> find(long id, long minTimestamp, long maxTimestamp) {
		long begin = System.currentTimeMillis();
		ByteBuffer temp = getTempBuffer();
		List<StoreUnit<T>> data = new ArrayList<>();
		while(temp.remaining()>=this.storeUnitSize+this.storeHeadSize){
			boolean skipTimestamp = true;
			boolean skipStoreUnit = true;
			if(temp.getLong()==id){
				long timestamp = temp.getLong();
				skipTimestamp = false;
				if(timestamp>=minTimestamp&&timestamp<=maxTimestamp){
					byte[] bytes = new byte[this.storeUnitSize];
					temp.get(bytes);
					data.add(new StoreUnit<>(timestamp, id,this.serializerStore.decode(bytes)));
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
		return new StoreResult<>(data, (int)(System.currentTimeMillis() - begin),true);
	}
	
	@Override
	public StoreResult<T> findCount(long id, long minTimestamp, long maxTimestamp) {
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
		return new StoreResult<T>(count,(int)(System.currentTimeMillis()-begin));
	}
	
	private void skipTimestamp(ByteBuffer buffer){
		buffer.position(buffer.position()+8);
	}
	private void skipStoreUnit(ByteBuffer buffer){
		buffer.position(buffer.position()+this.storeUnitSize);
	}
	
	public Map<Long,List<StoreUnit<T>>> findAll() throws IOException{
		ByteBuffer buffer = getTempBuffer();
		Map<Long,List<StoreUnit<T>>> maps = new HashMap<>();
		while(buffer.remaining() >= this.storeUnitSize+this.storeHeadSize){
			long id = buffer.getLong();
			if(id==0){
				break;
			}
			List<StoreUnit<T>> list = maps.get(id);
			if(list==null){
				list = new ArrayList<>();
				maps.put(id, list);
			}
			long timestamp = buffer.getLong();
			byte[] bytes = new byte[this.storeUnitSize];
			buffer.get(bytes);
			list.add(new StoreUnit<>(timestamp, id,this.serializerStore.decode(bytes)));
		}
		for (Entry<Long, List<StoreUnit<T>>> entry : maps.entrySet()) {
			Collections.sort(entry.getValue(),new Comparator<StoreUnit<T>>() {
				@Override
				public int compare(StoreUnit<T> o1, StoreUnit<T> o2) {
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
		int position  =  this.buffer.getInt(0);
		try {
			if(position>4){
				MappedByteBuffer result =  FileCache.getMappedByteBuffer(this.configuration.getDiskPath()+"memory.mts",StoreIndex.READ_WRITE, 0, position);
				result.position(4);
				return result;
			}
		} catch (Exception e) {
			
		} 
		return ByteBuffer.allocate(0);
		
	}
	
	public void clear(){
		this.lock.lock();
		try {
			this.buffer.clear();
			this.buffer.position(0);
			this.buffer.putInt(0);
		} finally{
			this.lock.unlock();	
		}
		
	}
}
