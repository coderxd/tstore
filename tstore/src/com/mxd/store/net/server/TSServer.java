package com.mxd.store.net.server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mxd.store.net.server.handler.HandlerThread;
public class TSServer {

	private static Logger logger = LoggerFactory.getLogger(TSServer.class);
	
	private boolean running = true;
	
	private ExecutorService executor;
	
	/**
	 * 启动Timestamp服务
	 * @param port	端口号
	 */
	public void start(int port) {
		this.start(port,Runtime.getRuntime().availableProcessors());
	}
	
	/**
	 * 启动Timestamp服务
	 * @param port	端口号
	 * @param nThreads	处理线程数
	 */
	public void start(int port,int nThreads) {
		ServerSocketChannel serverSocketChannel = null;
		Selector selector = null;
		executor = Executors.newFixedThreadPool(nThreads);
		try {
			selector = Selector.open();
			serverSocketChannel = ServerSocketChannel.open();
			serverSocketChannel.configureBlocking(false);
			serverSocketChannel.bind(new InetSocketAddress(port));
			serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
			while (running) {
				selector.select();
				Iterator<SelectionKey> it = selector.selectedKeys().iterator();
				while (it.hasNext()) {
					SelectionKey selectionKey = it.next();
					it.remove();
					if(selectionKey.isAcceptable()){
						SocketChannel socketChannel = ((ServerSocketChannel)selectionKey.channel()).accept();
						if(socketChannel!=null){
							logger.info("accept client socket "+socketChannel);
							socketChannel.configureBlocking(false);
							socketChannel.register(selectionKey.selector(), SelectionKey.OP_READ);
						}
					}else if(selectionKey.isValid()&&selectionKey.isReadable()){
						selectionKey.interestOps(selectionKey.interestOps()&(~SelectionKey.OP_READ));
						executor.execute(new HandlerThread(selectionKey));
					}
					
				}
			}
		} catch (IOException e) {
			logger.error("error",e);
		}
	}
	
	/**
	 * 关闭服务
	 */
	public void close(){
		if(this.running){
			this.running = false;
			if(executor!=null){
				try {
					executor.shutdown();
					if(!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)){
						executor.shutdownNow();
					}
				} catch (InterruptedException e) {
					executor.shutdownNow();
				}
			}
			
		}
	}
	public static void main(String[] args) {
		String configDir = System.getProperty("user.dir");
		if(new File(configDir+"/conf/log4j.properties").exists()){
			PropertyConfigurator.configure(configDir+"/conf/log4j.properties");
		}
		new TSServer().start(5124, 16);
	}
}
