package com.mxd.test;
import java.io.IOException;
import java.util.Random;
import com.mxd.store.SerializeStore;
import com.mxd.store.StoreConfiguration;
import com.mxd.store.StoreResult;
import com.mxd.store.StoreUnit;
import com.mxd.store.TimestampStore;
import com.mxd.store.TimestampStoreFactory;
public class Test {
	
	private static final long defaultTimestamp = 1504195200L; 

	public static void main(String[] args) throws IOException {
		StoreConfiguration configuration = new StoreConfiguration();
		configuration.setSerializeStore(new LongSerialize());
		configuration.setDiskPath("D:\\tstore\\");
		configuration.setStoreUnitSize(8);
		configuration.setDiskUnitBufferSize(128);
		configuration.setTimeUnit(0);
		configuration.setMemoryMaxSize(1024*1024*5);
		TimestampStoreFactory factory = new TimestampStoreFactory(configuration);
		TimestampStore<Long> store = factory.create();
		testInsert(store);
		testRead(store);
	}
	
	private static void testReadCount(long id,TimestampStore<Long> store){
		StoreResult<?> result = store.findCount(id, defaultTimestamp-86400*7, defaultTimestamp+86400*7);
		System.out.println(String.format("findCount:%s\tcount:%s\ttime:%s", id,result.getSize(),result.getConsuming()));
	}
	
	private static void testRead(long id,TimestampStore<Long> store){
        StoreResult<Long> result = store.find(id, defaultTimestamp-86400*7, defaultTimestamp+86400*7);
        for (StoreUnit<Long> unit : result.getData()) {
			System.out.println(String.format("%s\t%s\t%s", unit.getId(),unit.getTimestamp(),unit.getData()));
		}
        System.out.println(result);
	}
	
	private static void testRead(TimestampStore<Long> store){
		Random random = new Random();
		
		int max=500;
        int min=1;
        long id = random.nextInt(max)%(max-min+1) + min;
        testRead(id, store);
        testReadCount(id,store);
	}
	
	private static void testInsert(TimestampStore<Long> store){
		//1,000,000
		for (int j = 1; j <=1000; j++) {
			for (int i = 1; i <=1000; i++) {
				store.insert(new StoreUnit<Long>(defaultTimestamp+j*60, i, i*10000000L+j*60L));
			}
		}
		try {
			Thread.sleep(3000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("insert finish");
	}
	
	static class LongSerialize extends SerializeStore<Long> {

		@Override
		public Long decode(byte[] data) {
			return ((((long)data[0]       ) << 56) |
	                (((long)data[1] & 0xff) << 48) |
	                (((long)data[2] & 0xff) << 40) |
	                (((long)data[3] & 0xff) << 32) |
	                (((long)data[4] & 0xff) << 24) |
	                (((long)data[5] & 0xff) << 16) |
	                (((long)data[6] & 0xff) <<  8) |
	                (((long)data[7] & 0xff)      ));
		}

		@Override
		public byte[] encode(Long t) {
			long value = t.longValue();
			return new byte[]{
					(byte) (value>>56),
					(byte) (value>>48),
					(byte) (value>>40),
					(byte) (value>>32),
					(byte) (value>>24),
					(byte) (value>>16),
					(byte) (value>>8),
					(byte) (value)
			};
		}
	}

}
