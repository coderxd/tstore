package com.mxd.store.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sun.nio.ch.FileChannelImpl;


public class FileCache {
	
	private final static Map<String,FileItem> fileMap = new HashMap<>();
	
	private static Method unmapMethod;
	
	private static final long defaultExpireTime = 180000;
	
	static {
		 new Thread(new FileExpriedThread()).start();
		 try {
			unmapMethod = FileChannelImpl.class.getDeclaredMethod("unmap",MappedByteBuffer.class);
			unmapMethod.setAccessible(true);
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}
	
	public static MappedByteBuffer getMappedByteBuffer(RandomAccessFile raf,MapMode mode,long position,long size) throws IOException{
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
				buffer = raf.getChannel().map("r".equals(mode) ? FileChannel.MapMode.READ_ONLY:FileChannel.MapMode.READ_WRITE, position, size);
			}else{
				raf = new RandomAccessFile(file, mode);
				item = new FileItem(raf, System.currentTimeMillis()+defaultExpireTime);
				buffer = raf.getChannel().map("r".equals(mode) ? FileChannel.MapMode.READ_ONLY:FileChannel.MapMode.READ_WRITE, position, size);
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
	
	public static void close(MappedByteBuffer buffer){
		unmap(buffer);
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
