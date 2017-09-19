package com.mxd.store;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mxd.store.common.FormatUtils;
import com.mxd.store.common.Store;

public class TimestampStoreEngine {
	
	private static Logger logger = LoggerFactory.getLogger(TimestampStoreEngine.class);
	
	private static Map<String,TimestampStore> stories = new ConcurrentHashMap<>();
	
	private static StoreConfiguration defaultConfiguration = null;
	
	static{
		init();
	}
	
	private static void init(){
		String diskPath = getDefaultStoreConfiguration().getDiskPath();
		File[] dirs = new File(diskPath).listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.isDirectory();	
			}
		});
		if(dirs!=null){
			for (File dir : dirs) {
				FileInputStream fis = null;
				ObjectInputStream ois = null;
				try {
					fis = new FileInputStream(dir.getAbsolutePath()+File.separator+dir.getName()+".cts");
					ois = new ObjectInputStream(fis);
					StoreConfiguration configuration = (StoreConfiguration) ois.readObject();
					stories.put(dir.getName(), create(configuration));
				} catch (Exception e) {
					logger.error("load store ["+dir.getName()+"] error",e);
				} finally{
					if(ois!=null){
						try {
							ois.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					if(fis!=null){
						try {
							fis.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
	
	/**
	 * 获取一个store
	 * @param storeName
	 * @return
	 */
	public static TimestampStore get(String storeName){
		return stories.get(storeName);
	}
	
	/**
	 * 创建一个Store
	 * @param store
	 * @return
	 * @throws IOException
	 */
	public static TimestampStore put(Store store) throws IOException{
		StoreConfiguration configuration = getDefaultStoreConfiguration();
		configuration.setStore(store);
		TimestampStore timestampStore = create(configuration);
		File file = new File(configuration.getDiskPath()+store.getName());
		file.mkdirs();
		//配置文件通过序列化对象的方式保存（以后会修改存储方式）
		FileOutputStream fos = new FileOutputStream(configuration.getDiskPath()+store.getName()+File.separator+store.getName()+".cts");
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(configuration);
		oos.flush();
		oos.close();
		fos.close();
		stories.put(store.getName(),timestampStore);
		logger.info("create store success ",store);
		return timestampStore;
	}
	private static void checkConfig(StoreConfiguration configuration){
		if(configuration.getTimeUnit()<0||configuration.getTimeUnit()>2){
			configuration.setTimeUnit(1);
		}
	}
	
	/**
	 * 获取默认配置
	 * @return
	 */
	private static StoreConfiguration getDefaultStoreConfiguration(){
		if(defaultConfiguration == null){
			String configDir = System.getProperty("user.dir");
			Properties properties = new Properties();
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(new File(configDir+"/conf/conf.properties"));
				properties.load(fis);
				defaultConfiguration = new StoreConfiguration();
				defaultConfiguration.setDiskPath(properties.getProperty("diskPath",configDir+"/data/"))
					.setReadThreads(FormatUtils.parseInt(properties.getProperty("readThreads"),defaultConfiguration.getReadThreads()))
					.setInsertThreads(FormatUtils.parseInt(properties.getProperty("insertThreads"),defaultConfiguration.getInsertThreads()))
					.setReadDiskThreads(FormatUtils.parseInt(properties.getProperty("readDiskThreads"),defaultConfiguration.getReadDiskThreads()))
					.setTimeUnit(FormatUtils.parseInt(properties.getProperty("timeUnit"),defaultConfiguration.getTimeUnit()))
					.setDiskUnitBufferSize(FormatUtils.parseInt(properties.getProperty("diskUnitBufferSize"),defaultConfiguration.getDiskUnitBufferSize()));
				checkConfig(defaultConfiguration);
			} catch (Exception e) {
				defaultConfiguration = new StoreConfiguration();
				logger.warn("config load error",e);
				logger.warn("use default config",defaultConfiguration);
			} finally{
				try {
					fis.close();
				} catch (IOException e) {
				}
			}
		}
		return defaultConfiguration.get();
	}
	
	/**
	 * 创建TimestampStore
	 * @param configuration
	 * @return
	 * @throws IOException
	 */
	private static TimestampStore create(StoreConfiguration configuration) throws IOException{
		if(configuration == null){
			
			throw new NullPointerException("storeConfiguration is null");
			
		}else if(configuration.getDiskPath() == null){
			
			throw new NullPointerException("diskPath is null");
			
		}else if(configuration.getStore() == null){
			
			throw new NullPointerException("store is null");
		}
		StoreConfiguration storeConfiguration = configuration.get();
		
		storeConfiguration.setDiskPath(storeConfiguration.getDiskPath() + storeConfiguration.getStore().getName()+File.separator);
		
		File file = new File(storeConfiguration.getDiskPath());
		
		if(!file.exists()){
			
			file.mkdirs();
			
		}
		
		StoreIndex storeIndex = new StoreIndex(storeConfiguration);
		
		DiskStore diskStore = new DiskStore(storeConfiguration,storeIndex);
		
		/**
		 * 这里指的并不是真正的内存，是虚拟内存
		 */
		MemoryStore memoryStore = new MemoryStore(storeConfiguration);
		
		return new DefaultTimestampStore(storeConfiguration,memoryStore,diskStore);
	}
	/**
	 * 获取Store列表
	 * @return
	 */
	public static Set<String> list(){
		return stories.keySet();
	}
}
