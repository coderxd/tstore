package com.mxd.store.net.server.handler;

import static com.mxd.store.net.common.ResponseCode.*;
import static com.mxd.store.net.common.RequestCode.REQUEST_GET;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mxd.store.TimestampStore;
import com.mxd.store.TimestampStoreEngine;
import com.mxd.store.common.FormatUtils;
import com.mxd.store.common.StoreResult;
import com.mxd.store.net.common.StoreMessage;

/**
 * 查询操作处理器
 * @author mxd
 *
 */
public class GetHandler extends ServerStoreMessageHandler{

	@Override
	public void onMessageReceived(SocketChannel channel, StoreMessage message) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Long id = null;
		Long minTimestamp = null;
		Long maxTimestamp = null;
		TimestampStore timestampStore = null;
		int response = 0;
		//格式：[storeName,id,minTimestamp,maxTimestamp]
		Object[] objs = mapper.readValue(message.getData(), Object[].class);
		if(objs!=null&&objs.length ==4){
			String storeName = FormatUtils.parseString(objs[0], null);
			if(storeName!=null){
				timestampStore = TimestampStoreEngine.get(storeName);
				id = FormatUtils.parseLong(objs[1], null);
				minTimestamp = FormatUtils.parseLong(objs[2], null);
				maxTimestamp = FormatUtils.parseLong(objs[3], null);
			}
		}
		if(id==null||minTimestamp==null||maxTimestamp==null){
			response = RESPONSE_WRONG_FORMAT;
		}else if(timestampStore==null){
			response = RESPONSE_STORE_NOTFOUND;
		}else if(message.getRequestCode() == REQUEST_GET){	//判断是查count还是查数据
			StoreResult result = timestampStore.find(id, minTimestamp, maxTimestamp);
			//将结果按一定的格式传输给客户端
			byte[] data = timestampStore.getSerializeStore().encode(result);
			super.write(channel,new StoreMessage(RESPONSE_GET,data));
			
			return;
		}else{
			StoreResult result = timestampStore.findCount(id, minTimestamp, maxTimestamp);
			byte[] data = ByteBuffer.allocate(12).putLong(result.getSize()).putInt(result.getConsuming()).array();
			super.write(channel,new StoreMessage(RESPONSE_GET_COUNT,data));
			return;
		}
		super.write(channel, new StoreMessage(response));
	}

}
