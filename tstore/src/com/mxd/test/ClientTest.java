package com.mxd.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.mxd.store.net.client.TSClient;
import com.mxd.store.net.common.ResponseCode;
import com.mxd.store.net.common.StoreMessage;
import com.mxd.store.net.common.StoreMessageDecoder;

public class ClientTest {
	public static void main(String[] args) throws IOException {
		TSClient client = new TSClient("127.0.0.1", 5124,600000);
		client.connect();
		//创建一个名叫track的Store，列为longitude类型是double，列latitude类型是double,列altitude类型是short,列spped类型是short,列course类型是short
		//client.create("{\"name\":\"temp\",\"columns\":[{\"name\":\"longitude\",\"type\":4},{\"name\":\"latitude\",\"type\":4},{\"name\":\"altitude\",\"type\":2},{\"name\":\"speed\",\"type\":2},{\"name\":\"course\",\"type\":2}]}");
		//创建100*3000条数据，从1504195200到1504195200+5000*10
		//createGPSData(client,"temp",100,5000,10,1504195200);	
		for (int i = 0; i <100; i++) {
			testRead(client,100000+i,1504195200,1504195200+5000*10);
		}
	}
	private static void testRead(TSClient client,long id,long minTimestamp,long maxTimestamp) throws IOException{
		long begin = System.currentTimeMillis();
		StoreMessage message = client.get("track", id, minTimestamp, maxTimestamp);
		if(message.getRequestCode() == ResponseCode.RESPONSE_GET){
			List<Map<String,Object>> list = new ArrayList<>();
			long[] ret = StoreMessageDecoder.responseGet(message.getData(),list,new ArrayList<String>());
			System.out.println("get:{count:"+ret[0]+",query cost time:"+ret[1]+",read cost time:"+(System.currentTimeMillis()-begin)+"}");
		}else{
			System.out.println("error:0x"+Integer.toString(message.getRequestCode(), 16));
		}
		message = null;
	}
	/**
	 * 创建count*size条数据，平均分布在beginTime~beginTime+interval*size内
	 */
	private static void createGPSData(TSClient client,String storeName,int count,int size,int interval,long beginTime) throws IOException{
		for (int id = 1; id <=count; id++) {
			List<Object[]> params = new ArrayList<>();
			for (int d = 1; d <=size; d++) {
				Object[] objs = new Object[7];
				long timestamp = beginTime+d*interval;
				double longitude = 20+0.000001*d;
				double latitude = 10+0.000001*d;
				short altitude = (short) (d % 3600);
				short speed = (short) (d % 120);
				short course = (short) (d % 360);
				objs[0] =  id;
				objs[1] =  timestamp;
				objs[2] =  longitude;
				objs[3] =  latitude;
				objs[4] =  altitude;
				objs[5] =  speed;
				objs[6] =  course;
				params.add(objs);
			}
			client.puts(storeName, params);
			System.out.println("puts--"+(id*100.0/count)+"%");
		}
		System.out.println("100%");
		
	}
}
