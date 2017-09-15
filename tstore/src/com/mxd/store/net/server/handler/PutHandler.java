package com.mxd.store.net.server.handler;

import static com.mxd.store.net.common.RequestCode.REQUEST_PUT;
import static com.mxd.store.net.common.ResponseCode.*;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mxd.store.TimestampStore;
import com.mxd.store.TimestampStoreEngine;
import com.mxd.store.TimestampStore.SaveStatus;
import com.mxd.store.common.FormatUtils;
import com.mxd.store.common.StoreUnit;
import com.mxd.store.net.common.StoreMessage;

/**
 * 插入操作处理器
 * @author mxd
 *
 */
public class PutHandler extends ServerStoreMessageHandler{

	@Override
	public void onMessageReceived(SocketChannel channel, StoreMessage message) throws Exception {
		int response = RESPONSE_WRONG_FORMAT;
		try {
			ObjectMapper mapper = new ObjectMapper();
			if(message.getRequestCode() == REQUEST_PUT){	//单条插入
				//[storeName,id,timestamp,column1,column2,...,columnN]
				Object[] objs = mapper.readValue(message.getData(), Object[].class);	
				if(objs.length>=3){	// 至少是三列
					String storeName = FormatUtils.parseString(objs[0], null);
					if(storeName!=null){
						TimestampStore timestampStore =  TimestampStoreEngine.get(storeName);
						if(timestampStore !=null){
							//将参数转化为SotreUnit对象
							StoreUnit unit = timestampStore.getSerializeStore().encode(objs);
							//执行插入操作
							SaveStatus status = timestampStore.insert(unit);
							if(status==SaveStatus.SUCCESS){
								response = RESPONSE_SUCCESS;
							}else{
								response = ERROR_SYSTEM;
							}
						}else{
							response = RESPONSE_STORE_NOTFOUND;
						}
					}
				}
			}else{	//多条插入
				@SuppressWarnings("unchecked")
				//{"store":"temp","params":[[id,timestamp,column1,column2,...,columnN],[id,timestamp,column1,column2,...,columnN],...,[id,timestamp,column1,column2,...,columnN]]}
				Map<String,Object> map = mapper.readValue(message.getData(), Map.class);
				if(map!=null){
					String storeName = FormatUtils.parseString(map.get("store"), null);
					if(storeName!=null){
						TimestampStore timestampStore =  TimestampStoreEngine.get(storeName);
						if(timestampStore !=null){
							@SuppressWarnings("unchecked")
							List<StoreUnit> units = timestampStore.getSerializeStore().encode((List<Object>)map.get("params"));
							SaveStatus status = timestampStore.insert(units);
							if(status==SaveStatus.SUCCESS){
								response = RESPONSE_SUCCESS;
							}else{
								response = ERROR_SYSTEM;
							}
						}else{
							response = RESPONSE_STORE_NOTFOUND;
						}
					}
				}
			}
			
		} catch(IllegalArgumentException e){
			response = RESPONSE_WRONG_FORMAT;
		}catch (Exception e) {
			response = ERROR_SYSTEM;
		}
		super.write(channel,new StoreMessage(response));
	}

}
