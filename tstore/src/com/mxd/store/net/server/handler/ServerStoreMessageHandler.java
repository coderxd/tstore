package com.mxd.store.net.server.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mxd.store.net.common.StoreMessage;

/**
 * 消息处理器基类
 * @author mxd
 *
 */
public abstract class ServerStoreMessageHandler{
	
	private static Logger logger = LoggerFactory.getLogger(ServerStoreMessageHandler.class);

	/**
	 * 接收到消息后执行
	 * @param channel
	 * @param message
	 * @throws Exception
	 */
	public abstract void onMessageReceived(SocketChannel channel,StoreMessage message) throws Exception;
	
	/**
	 * 写入数据
	 * @param channel
	 * @param message
	 * @throws IOException
	 */
	protected void write(SocketChannel channel,StoreMessage message) throws IOException{
		try {
			ByteBuffer buffer = message.getBuffer();
			while(buffer.hasRemaining()){
				channel.write(buffer);
			}
		} catch (Exception e) {
			if(logger.isDebugEnabled()){
				logger.debug("write error",e);
			}
		}
	}
}
