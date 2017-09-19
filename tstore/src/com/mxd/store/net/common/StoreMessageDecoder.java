package com.mxd.store.net.common;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StoreMessageDecoder {
	
	public static long[] responseGet(byte[] data,List<Map<String,Object>> result,List<String> columns){
		long[] ret = new long[2];
		ByteBuffer buffer = ByteBuffer.wrap(data);
		int columnSize = buffer.getInt();
		String[] header = new String[columnSize];
		int[] types = new int[columnSize];
		int[] columnLength = new int[columnSize];
		columns.add("id");
		columns.add("timestamp");
		for (int i = 0; i < columnSize; i++) {
			int size = buffer.getInt();
			byte[] bytes = new byte[size];
			buffer.get(bytes);
			try {
				header[i] = new String(bytes,"ISO-8859-1");
				types[i] = buffer.getInt();
				columnLength[i] = buffer.getInt();
				columns.add(header[i]);
			} catch (UnsupportedEncodingException e) {
				return null;
			}
		}
		long len=buffer.getLong();
		ret[0] = len;
		ret[1] = buffer.getInt();
		for (int i = 0; i <len; i++) {
			Map<String,Object> map = new HashMap<>();
			map.put("id", buffer.getLong());
			map.put("timestamp", buffer.getLong());
			for (int j = 0; j < columnSize; j++) {
				Object value = null;
				int type = types[j];
				int cLen = columnLength[j];
				if(type==1){
					value = buffer.getInt();
				}else if(type==2){
					value = buffer.getShort();
				}else if(type==3){
					value = buffer.getFloat();
				}else if(type==4){
					value = buffer.getDouble();
				}else if(type==5){
					value = buffer.getLong();
				}else if(type==6){
					int slen = buffer.getInt();
					byte[] bytes = new byte[cLen];
					buffer.get(bytes);
					try {
						//get temp 100005 1504195200 1504195201
						value = new String(bytes,0,slen,"ISO-8859-1");
					} catch (UnsupportedEncodingException e) {
						return null;
					}
				}
				map.put(header[j], value);
			}
			result.add(map);
		}
		return ret;
	}
}
