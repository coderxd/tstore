package com.mxd.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.mxd.store.net.client.TSClient;
import com.mxd.store.net.common.StoreMessage;

public class ClientTest {
	public static void main(String[] args) throws IOException {
		TSClient client = new TSClient("127.0.0.1", 5124);
		client.connect();
		//创建一个名叫temp的Store，列为value类型是long，列name类型是String，长度20
		client.create("{\"name\":\"temp\",\"columns\":[{\"name\":\"value\",\"type\":5},{\"name\":\"name\",\"type\":6,\"length\":20}]}");
		long begin = System.currentTimeMillis();
		//创建一百万条数据
		for (int i = 1; i <=1000; i++) {
			List<Object[]> params = new ArrayList<>();
			for (int j = 1; j <=1000; j++) {
				long id = i+100001L;
				long timestamp = 1504195200L+j;
				long value = 1504195200L+j;
				String name = "test-"+j;
				params.add(new Object[]{id,timestamp,value,name});
			}
			StoreMessage message = client.puts("temp", params);
			System.out.println("write "+i*1000+" over - 0x"+Integer.toString(message.getRequestCode(), 16));
		}
		System.out.println(System.currentTimeMillis() - begin);
	}
	static class SendThead implements Runnable{
		
		private List<Object[]> params = new ArrayList<>();
		
		private TSClient client;
		
		public SendThead(List<Object[]> params, TSClient client) {
			super();
			this.params = params;
			this.client = client;
		}
		@Override
		public void run() {
			try {
				client.puts("temp",params);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
}
