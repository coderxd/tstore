package com.mxd.store.net.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mxd.store.net.common.StoreMessage;
import static com.mxd.store.net.common.RequestCode.*;

public class TSClient {
	
	private static Logger logger = LoggerFactory.getLogger(TSClient.class);
	
	private String host;
	
	private int port;
	
	private int connectTimeout;
	
	private int readTimeout;
	
	private SocketChannel socketChannel;
	
	private Selector selector;
	
	public TSClient(String host, int port,int connectTimeout,int readTimeout) {
		super();
		this.host = host;
		this.port = port;
		this.connectTimeout = connectTimeout;
		this.readTimeout = readTimeout;
	}
	
	public TSClient(String host, int port) {
		this(host,port,5000,60000);
	}
	
	public TSClient(String host, int port,int readTimeout) {
		this(host,port,5000,readTimeout);
	}

	public void connect() throws IOException{
		long begin = System.currentTimeMillis();
		this.selector = Selector.open();
		this.socketChannel = SocketChannel.open(new InetSocketAddress(this.host, this.port));
		this.socketChannel.configureBlocking(false);
		this.socketChannel.socket().setReuseAddress(true);
		this.socketChannel.socket().setTcpNoDelay(true);
		this.socketChannel.socket().setReceiveBufferSize(1024);
		while(!this.socketChannel.isConnected()){
			if(System.currentTimeMillis() - begin > this.connectTimeout){
				throw new SocketTimeoutException();
			}
			this.socketChannel.finishConnect();
		}
	}
	
	/**
	 * 创建store
	 * @param json
	 * @return
	 * @throws IOException
	 */
	public StoreMessage create(String json) throws IOException{
		return send(new StoreMessage(REQUEST_CREATE,json));
	}
	
	/**
	 * 获取某一时间段内的数据
	 * @param storeName
	 * @param id
	 * @param minTimestamp
	 * @param maxTimestamp
	 * @return
	 * @throws IOException
	 */
	public StoreMessage get(String storeName,long id,long minTimestamp,long maxTimestamp) throws IOException{
		if(storeName==null){
			throw new NullPointerException("storeName is null");
		}
		return send(new StoreMessage(REQUEST_GET,String.format("[\"%s\",%d,%d,%d]", storeName,id,minTimestamp,maxTimestamp)));
	}
	
	/**
	 * 获取总数
	 * @param storeName
	 * @param id
	 * @param minTimestamp
	 * @param maxTimestamp
	 * @return
	 * @throws IOException
	 */
	public StoreMessage getCount(String storeName,long id,long minTimestamp,long maxTimestamp) throws IOException{
		if(storeName==null){
			throw new NullPointerException("storeName is null");
		}
		return send(new StoreMessage(REQUEST_GET_COUNT,String.format("[\"%s\",%d,%d,%d]", storeName,id,minTimestamp,maxTimestamp)));
	}
	
	/**
	 * 插入数据
	 * @param storeName	store名称
	 * @param id	id
	 * @param timestamp 时间戳
	 * @param args	各个列的数据
	 * @return
	 * @throws IOException
	 */
	public StoreMessage put(String storeName,long id,long timestamp,Object ... args) throws IOException{
		if(storeName==null){
			throw new NullPointerException("storeName is null");
		}else if(args==null){
			args = new Object[0];
		}
		List<Object> params = new ArrayList<>();
		params.add(storeName);
		params.add(id);
		params.add(timestamp);
		params.addAll(Arrays.asList(args));
		return send(new StoreMessage(REQUEST_PUT,new ObjectMapper().writeValueAsString(params)));
	}
	/**
	 * 批量插入
	 * @param storeName	sotre名称
	 * @param params	参数集合,必须保证前两个是id和timestamp列..
	 * @return
	 * @throws IOException
	 */
	public StoreMessage puts(String storeName,List<Object[]> params) throws IOException{
		if(storeName==null){
			throw new NullPointerException("storeName is null");
		}else if(params==null||params.size()==0){
			throw new NullPointerException("params is null or size = 0");
		}
		Map<String,Object> map = new HashMap<>();
		map.put("store", storeName);
		map.put("params", params);
		return send(new StoreMessage(REQUEST_PUTS,new ObjectMapper().writeValueAsString(map)));
	}
	protected StoreMessage send(StoreMessage message) throws IOException{
		if(this.socketChannel.isConnected()){
			ByteBuffer buffer = message.getBuffer();
			this.socketChannel.write(buffer);
			return get();
		}
		return null;
	}
	
	private ByteBuffer readBuffer(int len,long begin) throws IOException{
		int readLen = 0;
		ByteBuffer buffer = ByteBuffer.allocate(len);
		while(readLen<len){
			int temp = this.socketChannel.read(buffer);
			if(temp==-1){
				throw new IOException("read len is -1");
			}
			if(System.currentTimeMillis() - begin > this.readTimeout){
				throw new SocketTimeoutException("read timeout");
			}
			readLen+=temp;
		}
		buffer.flip();
		return buffer;
	}
	
	public synchronized StoreMessage get() throws IOException{
		long begin = System.currentTimeMillis();
		while(System.currentTimeMillis() - begin < this.readTimeout){
			try {
				ByteBuffer lenBuffer = readBuffer(4,begin);
				int dataLength = lenBuffer.getInt();
				if(dataLength > 0){
					ByteBuffer dataBuffer = readBuffer(dataLength,begin);
					if(dataBuffer.capacity()>=4){
						int requestCode = dataBuffer.getInt();
						byte[] data  = new byte[dataBuffer.remaining()];
						dataBuffer.get(data);
						return new StoreMessage(requestCode, data);
					}
				}else{
					Thread.sleep(5L);
					continue;
				}
			} catch (IOException e) {
				logger.error("socket read error",e);
				this.close();
			} catch (InterruptedException e) {
				
			}
			return null;
		}
		throw new SocketTimeoutException("read timeout");
	}
	public void close(){
		try {
			this.socketChannel.close();
			this.selector.close();
		} catch (IOException e) {
			logger.error("close error",e);
		}
	}
}
