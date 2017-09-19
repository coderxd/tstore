package com.mxd.store.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.FileChannelImpl;

/**
 * 缓存已打开的文件,默认180秒后关闭，每次get操作,都将重置关闭时间
 * @author mxd
 *
 */
public class FileCache {
	
	private static Logger logger = LoggerFactory.getLogger(FileCache.class);
	
	private final static Map<String,FileItem> fileMap = new ConcurrentHashMap<>();
	
	private final static Map<String,File> newFileMap = new ConcurrentHashMap<>();
	
	private static Method unmapMethod;
	
	private static final long defaultExpireTime = 1800000;
	
	public static final String READONLY ="r";
	
	public static final String READ_WRITE ="rw";
	
	static {
		 new Thread(new FileExpriedThread()).start();
		 try {
			unmapMethod = FileChannelImpl.class.getDeclaredMethod("unmap",MappedByteBuffer.class);
			unmapMethod.setAccessible(true);
		} catch (Exception e) {
		}
	}
	
	public static ByteBuffer getMappedByteBuffer(RandomAccessFile raf,MapMode mode,long position,int size) throws IOException{
		if(mode==FileChannel.MapMode.READ_ONLY){
			byte[] bytes = new byte[size];
			long filePointer = raf.getFilePointer();
			raf.seek(position);
			raf.read(bytes, 0, size);
			raf.seek(filePointer);
			return ByteBuffer.wrap(bytes);
		}
		return raf.getChannel().map(mode, position, size);
	}
	public static MappedByteBuffer getMappedByteBuffer(String file,String mode,long position,long size) throws IOException{
		String key = mode+":"+file;
		RandomAccessFile raf = null;
		MappedByteBuffer buffer = null;
		FileItem item = null;
		synchronized (fileMap) {
			if(fileMap.containsKey(key)){
				item = fileMap.get(key);
				item.expriedTime = System.currentTimeMillis()+defaultExpireTime;
				raf = item.raf;
				buffer = raf.getChannel().map("r".equals(READONLY) ? FileChannel.MapMode.READ_ONLY:FileChannel.MapMode.READ_WRITE, position, size);
			}else{
				raf = new RandomAccessFile(file, mode);
				item = new FileItem(raf, System.currentTimeMillis()+defaultExpireTime);
				buffer = raf.getChannel().map("r".equals(READONLY) ? FileChannel.MapMode.READ_ONLY:FileChannel.MapMode.READ_WRITE, position, size);
				fileMap.put(key, item);
			}
			item.addBuffer(buffer);
		}
		return buffer;
	}
	
	public static void close(String file,String mode){
		String key = mode+":"+file;
		synchronized (fileMap) {
			if(fileMap.containsKey(key)){
				FileItem item = fileMap.get(key);
				close(item.raf, item.buffers);
				fileMap.remove(key);
			}
		}
	}
	
	public static RandomAccessFile getNewFile(String mode,String file) throws FileNotFoundException{
		String key = file;
		File f = null;
		synchronized (newFileMap) {
			f = newFileMap.get(key);
			if(f==null){
				f = new File(file);
				newFileMap.put(key,f);
			}
		}
		return new RandomAccessFile(file, mode);
	}
	
	public static RandomAccessFile getFile(String mode,String file) throws FileNotFoundException{
		String key = mode+":"+file;
		RandomAccessFile raf = null;
		synchronized (fileMap) {
			if(fileMap.containsKey(key)){
				FileItem item = fileMap.get(key);
				item.expriedTime = System.currentTimeMillis()+defaultExpireTime;
				raf = item.raf;
			}else{
				raf = new RandomAccessFile(file, mode);
				FileItem item = new FileItem(raf, System.currentTimeMillis()+defaultExpireTime);
				fileMap.put(key, item);
			}
		}
		return raf;
	}
	
	public static void close(ByteBuffer buffer){
		if(buffer instanceof MappedByteBuffer){
			unmap((MappedByteBuffer) buffer);	
		}
		
	}
	
	private static void close(RandomAccessFile raf,List<MappedByteBuffer> buffers){
		try {
			boolean hasBuffer = buffers.size()>0;
			if(hasBuffer){
				raf.getChannel().close();
			}
			raf.close();
			if(hasBuffer){
				for (MappedByteBuffer mappedByteBuffer : buffers) {
					close(mappedByteBuffer);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private static void unmap(final MappedByteBuffer mappedByteBuffer){
		if(mappedByteBuffer==null){
			return;
		}
		try {
			unmapMethod.invoke(FileChannelImpl.class, mappedByteBuffer);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static class FileItem{
		
		private RandomAccessFile raf;
		
		private long expriedTime;
		
		private List<MappedByteBuffer> buffers = new ArrayList<>();
		
		public FileItem(RandomAccessFile raf,long expriedTime) {
			super();
			this.raf = raf;
			this.expriedTime = expriedTime;
		}

		public RandomAccessFile getRaf() {
			return raf;
		}

		public void setRaf(RandomAccessFile raf) {
			this.raf = raf;
		}

		public long getExpriedTime() {
			return expriedTime;
		}

		public void setExpriedTime(long expriedTime) {
			this.expriedTime = expriedTime;
		}

		public List<MappedByteBuffer> getBuffers() {
			return buffers;
		}

		public void setBuffers(List<MappedByteBuffer> buffers) {
			this.buffers = buffers;
		}
		
		public void addBuffer(MappedByteBuffer buffer){
			this.buffers.add(buffer);
		}
	}
	static class FileExpriedThread implements Runnable{

		@Override
		public void run() {
			while(true){
				try {
					synchronized (fileMap) {
						long timestamp = System.currentTimeMillis();
						for (String key : fileMap.keySet()) {
							FileItem item = fileMap.get(key);
							if(item.getExpriedTime()< timestamp){
								logger.info("close expried file:"+key);
								close(item.raf,item.buffers);
								fileMap.remove(key);
							}
						}
					}
					Thread.sleep(3000L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
