package com.mxd.store.net.server.handler;

import java.nio.channels.SocketChannel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mxd.store.TimestampStoreEngine;
import com.mxd.store.common.Store;
import com.mxd.store.net.common.StoreMessage;
import static com.mxd.store.net.common.RequestCode.REQUEST_CREATE;
import static com.mxd.store.net.common.ResponseCode.*;

/**
 * 用来处理Store创建、删除类的操作
 * @author mxd
 *
 */
public class ServerStoreHandler extends ServerStoreMessageHandler{

	@Override
	public void onMessageReceived(SocketChannel channel, StoreMessage message) throws Exception {
		if(message.getRequestCode()==REQUEST_CREATE){	//创建Store
			ObjectMapper mapper = new ObjectMapper();
			Store store;
			try {
				//解析参数成Store对象(目前采用json)
				store = mapper.readValue(message.getData(), Store.class);
				if(TimestampStoreEngine.get(store.getName())!=null){
					super.write(channel,new StoreMessage(RESPONSE_CREATE_STORE_EXISTS));
				}else{
					TimestampStoreEngine.put(store);
					super.write(channel,new StoreMessage(RESPONSE_SUCCESS));
				}
			} catch (Exception e) {
				super.write(channel,new StoreMessage(RESPONSE_WRONG_FORMAT));
			}
			
		}else{
			channel.write(new StoreMessage(RESPONSE_SUCCESS).getBuffer());
		}
	}

}
