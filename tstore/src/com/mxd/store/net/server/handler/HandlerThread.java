package com.mxd.store.net.server.handler;

import static com.mxd.store.net.common.RequestCode.*;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mxd.store.net.common.StoreMessage;
public class HandlerThread implements Runnable{
	
	private static Logger logger = LoggerFactory.getLogger(HandlerThread.class); 
	
	private SelectionKey selectionKey;
	
	public HandlerThread(SelectionKey selectionKey) {
		super();
		this.selectionKey = selectionKey;
	}
	@Override
	public void run() {
		try {
			/**
			 * 解析接收到的数据并交给处理器去处理。
			 * [dataLength,requestCode,data] requestCode+data = dataLength
			 */
			SocketChannel channel = (SocketChannel) selectionKey.channel();
			ByteBuffer lenBuffer = ByteBuffer.allocate(4);
			int len = channel.read(lenBuffer);
			if(len==-1){
				channel.close();
				return;
			}
			if(len > 0){
				lenBuffer.flip();
				int dataLength = lenBuffer.getInt();
				ByteBuffer dataBuffer = ByteBuffer.allocate(dataLength);
				int rlen = 0;
				while(rlen!=dataLength){
					rlen+=channel.read(dataBuffer);
				}
				dataBuffer.flip();
				int requestCode = dataBuffer.getInt();
				byte[] data  = new byte[dataBuffer.remaining()];
				dataBuffer.get(data);
				this.handlerMessage(new StoreMessage(requestCode, data),channel);
			}
			selectionKey.interestOps(selectionKey.interestOps()|SelectionKey.OP_READ);
			selectionKey.selector().wakeup();
		} catch (Exception e) {
			this.selectionKey.cancel();
			logger.warn("handler error",e);
		}
	}
	private void handlerMessage(StoreMessage message,SocketChannel channel){
		ServerStoreMessageHandler handler = null;
		switch (message.getRequestCode()) {
			case REQUEST_CREATE:
			case REQUEST_DELETE:
				handler = new ServerStoreHandler();
				break;
			case REQUEST_GET:
			case REQUEST_GET_COUNT:
				handler = new GetHandler();
				break;
			case REQUEST_PUT:
			case REQUEST_PUTS:
				handler =  new PutHandler();
				break;
			default:
				handler = new DefaultStoreMessageHandler();
				break;
		}
		if(handler!=null){
			try {
				handler.onMessageReceived(channel, message);
			} catch (Exception e) {
				logger.error(handler.getClass() + " error",e);
			}
		}
	}

}
