package com.mxd.store.common;

/**
 * 存储单元
 * @author mxd
 *
 */
public class StoreUnit implements Comparable<StoreUnit>{
	/**
	 * 时间戳
	 */
	private long timestamp;
	/**
	 * id
	 */
	private long id;
	
	/**
	 * columns编码后的数据
	 */
	private byte[] data;
	
	public StoreUnit(long timestamp, long id,byte[] data) {
		super();
		this.timestamp = timestamp;
		this.id = id;
		this.data = data;
	}

	public StoreUnit(long timestamp, long id) {
		super();
		this.timestamp = timestamp;
		this.id = id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	@Override
	public int compareTo(StoreUnit o) {
		return Long.valueOf(this.timestamp).compareTo(o.getTimestamp());
	}
}
