package com.mxd.store.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import com.mxd.store.common.Column.Type;

public class SerializeStore implements Serializable{
	
	private static final long serialVersionUID = 853270224144056300L;

	private Store store;
	
	private int storeUnitSize = 0;
	
	public SerializeStore(Store store) {
		super();
		this.store = store;
		this.storeUnitSize = this.store.getStoreUnitSize();
	}
	public Object[] decode(StoreUnit storeUnit){
		if(storeUnitSize!=storeUnit.getData().length){
			return null;
		}
		ByteBuffer buffer = ByteBuffer.wrap(storeUnit.getData());
		List<Column> columns = store.getColumns();
		int size = columns.size();
		Object[] result = new Object[size+2];
		result[0] = buffer.getLong();
		result[1] = buffer.getLong();
		for(int i =0;i<size;i++){
			Column column = columns.get(i);
			switch(column.getType()){
				case Type.INT:
					result[i+2] = buffer.getInt();
					break;
				case Type.DOUBLE:
					result[i+2] = buffer.getDouble();
					break;
				case Type.FLOAT:
					result[i+2] = buffer.getFloat();
					break;
				case Type.LONG:
					result[i+2] = buffer.getLong();
					break;
				case Type.SHORT:
					result[i+2] = buffer.getShort();
					break;
				case Type.STRING:
					int len = buffer.getInt();
					if(len>0){
						byte[] bytes = new byte[len];
						buffer.get(bytes);
						try {
							result[i+2] = new String(bytes,"ISO-8859-1");
						} catch (UnsupportedEncodingException e) {
						}
					}else{
						result[i+2] = "";
					}
					break;
			}
		}
		return result;
	}
	
	public List<Object[]> decode(List<StoreUnit> units){
		List<Object[]> result = new ArrayList<>();
		if(units.size()>0){
			for (StoreUnit unit : units) {
				result.add(decode(unit));
			}
		}
		return result;
	}
	public List<StoreUnit> encode(List<Object> objs){
		List<StoreUnit> result = new ArrayList<>();
		for (Object obj : objs) {
			@SuppressWarnings("unchecked")
			List<Object> objects = (List<Object>) obj;
			result.add(encode(FormatUtils.parseLong(objects.get(0), null), FormatUtils.parseLong(objects.get(1), null), objects.subList(2, objects.size()).toArray()));
		}
		return result;
		
	}
	/**
	 * 将数组解析成StoreUnit对象（Put操作）
	 * [storeName,id,timestamp,column1,column2,....,columnN]
	 * @param objs
	 * @return
	 */
	public StoreUnit encode(Object ... objs){
		
		//取出column1~columnN
		Object target[] = new Object[objs.length-3];
		System.arraycopy(objs, 3,target, 0,target.length);
		return encode(FormatUtils.parseLong(objs[1], null), FormatUtils.parseLong(objs[2], null), target);
	}
	/**
	 * 编码成StoreUnit对象
	 * @param id
	 * @param timestamp
	 * @param objs	各个列的值
	 * @return
	 */
	public StoreUnit encode(long id,long timestamp,Object ... objs){
		if(objs==null||objs.length==0){
			return new StoreUnit(timestamp, id, new byte[storeUnitSize]);
		}
		List<Column> columns = store.getColumns();
		int size = columns.size();
		if(objs.length!=size){
			throw new IllegalArgumentException("params length not equal column size");
		}
		ByteBuffer buffer = ByteBuffer.allocate(storeUnitSize);
		for (int i = 0; i <size; i++) {
			Column column = columns.get(i);
			Object value = objs[i];
			int position = buffer.position();
			if(value!=null){
				switch(column.getType()){
					case Type.INT:
						buffer.putInt(FormatUtils.parseInt(value, 0));
						break;
					case Type.DOUBLE:
						buffer.putDouble(FormatUtils.parseDouble(value, 0.0));
						break;
					case Type.FLOAT:
						buffer.putFloat(FormatUtils.parseFloat(value,0.0f));
						break;
					case Type.LONG:
						buffer.putLong(FormatUtils.parseLong(value,0L));
						break;
					case Type.SHORT:
						buffer.putShort(FormatUtils.parseShort(value,(short)0));
						break;
					case Type.STRING:
							try {
								byte[] val = FormatUtils.parseString(value, "").getBytes("ISO-8859-1");
								buffer.putInt(val.length);
								buffer.put(val);
								//略过不足的部分
								buffer.position(buffer.position()+column.getLength()-val.length);
							} catch (UnsupportedEncodingException e) {}
							
						break;
				}
			}
			if(i+1<size){
				buffer.position(position+column.getLength());	
			}
		}
		return new StoreUnit(timestamp, id,buffer.array());
	}
	public byte[] encode(StoreResult result){
		List<StoreUnit> units = result.getData();
		int unitSize = 0;
		if(units.size()>0){
			unitSize = units.size() * (units.get(0).getData().length+16);
		}
		ByteBuffer buffer = ByteBuffer.allocate(12+unitSize);
		buffer.putLong(result.getSize());
		buffer.putInt(result.getConsuming());
		for (StoreUnit unit : units) {
			buffer.putLong(unit.getId());
			buffer.putLong(unit.getTimestamp());
			buffer.put(unit.getData());
		}
		byte[] body = buffer.array();
		List<Column> columns = this.store.getColumns();
		ByteBuffer header = ByteBuffer.allocate(10240);
		header.putInt(columns.size());
		for(Column column : columns){
			byte[] bytes = new byte[0];
			try {
				bytes = column.getName().getBytes("ISO-8859-1");
			} catch (UnsupportedEncodingException e) {
			}
			header.putInt(bytes.length);
			header.put(bytes);
			header.putInt(column.getType());
			header.putInt(column.getLength());
		}
		header.flip();
		ByteBuffer resultBuffer = ByteBuffer.allocate(header.capacity()+body.length);
		resultBuffer.put(header);
		resultBuffer.put(body);
		byte[] bytes = resultBuffer.array();
		ByteArrayInputStream bais = null; 
		ByteArrayOutputStream baos = null;
		GZIPOutputStream gos = null;
		try {
			bais = new ByteArrayInputStream(bytes);
			baos = new ByteArrayOutputStream();
			gos = new GZIPOutputStream(baos);
			int count;  
		    byte data[] = new byte[1024];  
		    while ((count = bais.read(data, 0, 1024)) != -1) {  
		        gos.write(data, 0, count);  
		    }
		    gos.finish();
		    gos.flush();
		    return baos.toByteArray();
		} catch (IOException e) {
			
		} finally{
			if(gos!=null){
				try {
					gos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(bais!=null){
				try {
					bais.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return new byte[0];
	}
}
