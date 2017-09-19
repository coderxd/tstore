package com.mxd.store.net.server.handler;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mxd.store.TimestampStoreEngine;
import com.mxd.store.common.Store;
import com.mxd.store.net.common.StoreMessage;
import static com.mxd.store.net.common.RequestCode.REQUEST_STORE_CREATE;
import static com.mxd.store.net.common.RequestCode.REQUEST_STORE_LIST;
import static com.mxd.store.net.common.ResponseCode.*;

/**
 * 用来处理Store创建、删除类的操作
 * @author mxd
 *
 */
public class ServerStoreHandler extends ServerStoreMessageHandler{

	@Override
	public void onMessageReceived(SocketChannel channel, StoreMessage message) throws Exception {
		if(message.getRequestCode()==REQUEST_STORE_CREATE){	//创建Store
			ObjectMapper mapper = new ObjectMapper();
			Store store;
			try {
				//解析参数成Store对象(目前采用json)
				store = mapper.readValue(message.getData(), Store.class);
				if(TimestampStoreEngine.get(store.getName())!=null){
					super.write(channel,new StoreMessage(ERROR_CREATE_STORE_EXISTS));
				}else{
					TimestampStoreEngine.put(store);
					super.write(channel,new StoreMessage(RESPONSE_SUCCESS));
				}
			} catch (Exception e) {
				super.write(channel,new StoreMessage(RESPONSE_WRONG_FORMAT));
			}
			
		}else if(message.getRequestCode()==REQUEST_STORE_LIST){
			Set<String> stories = TimestampStoreEngine.list();
			ByteBuffer buffer = ByteBuffer.allocate(1024*30);
			buffer.putInt(stories.size());
			for (String storeName : stories) {
				byte[] bytes = storeName.getBytes("ISO-8859-1");
				buffer.putInt(bytes.length);
				buffer.put(bytes);
			}
			buffer.flip();
			byte[] bytes = new byte[buffer.limit()];
			buffer.get(bytes);
			super.write(channel, new StoreMessage(RESPONSE_STORE_LIST,bytes));
		}else{
			channel.write(new StoreMessage(RESPONSE_SUCCESS).getBuffer());
		}
	}

}
