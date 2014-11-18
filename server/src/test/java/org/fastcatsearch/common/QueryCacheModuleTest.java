package org.fastcatsearch.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.fastcatsearch.settings.Settings;
import org.junit.Test;

public class QueryCacheModuleTest {

	@Test
	public void testBulkPut() throws InterruptedException {

		int count = 1000000;

		QueryCacheModule<String, String> cacheModule = new QueryCacheModule<String, String>("", null, new Settings());
		cacheModule.load();
		long lap = System.nanoTime();
		for (int i = 0; i < count; i++) {
			String key = String.valueOf(i);
			cacheModule.put(key, key);
			// Thread.sleep(20);
			// if(i % 100 == 0){
			// System.out.println("doing "+i +".. size="+cache.size()+" >> " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) +"/ " +
			// Runtime.getRuntime().totalMemory());
			// }

			if (i % 10000 == 0) {
				long diff = (System.nanoTime() - lap) / 1000000;
				System.out.println("time = " + (diff) + "ms");
				lap = System.nanoTime();
			}
		}
	}

	@Test
	public void testBulkPutAndGet() throws InterruptedException {

		int count = 1000000;

		QueryCacheModule<String, String> cacheModule = new QueryCacheModule<String, String>("", null, new Settings());
		cacheModule.load();
		long lap = System.nanoTime();
		for (int i = 0; i < count; i++) {
			String key = String.valueOf(i);
			cacheModule.put(key, key);
			cacheModule.get(key);
			if (i % 10000 == 0) {
				long diff = (System.nanoTime() - lap) / 1000000;
				System.out.println("time = " + (diff) + "ms");
				lap = System.nanoTime();
			}
		}

	}

	/*
	 * 30개의 Thread로 캐시부하 테스트.
	 * */
	@Test
	public void testBulkPutAndGetMultiThread() throws InterruptedException {

		final AtomicInteger totalCount = new AtomicInteger();
		final QueryCacheModule<String, String> cacheModule = new QueryCacheModule<String, String>("", null, new Settings());
		cacheModule.load();
		final AtomicLong lap = new AtomicLong(System.nanoTime());

		class Runner extends Thread {

			public void run() {
				int count = 1000000;
				for (int i = 0; i < count; i++) {
					String key = String.valueOf(i);
//					System.out.println("put > "+key);
					cacheModule.put(key, key);
					String v = cacheModule.get(key);
					if(v == null){
						System.out.println(key +" >> null");
					}
//					try {
//						Thread.sleep(5);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
					if (totalCount.incrementAndGet() % 10000 == 0) {
						long diff = (System.nanoTime() - lap.get()) / 1000000;
						System.out.println("time = " + (diff) + "ms. sz="+cacheModule.size()+ " > "+this);
						lap.set(System.nanoTime());
					}
				}
			}
		};
		
		Runner[] r = new Runner[30];
		for (int i = 0; i < r.length; i++) {
			r[i] = new Runner();
		}
		for (int i = 0; i < r.length; i++) {
			r[i].start();
			System.out.println(r[i] +" started!");
		}
		for (int i = 0; i < r.length; i++) {
			r[i].join();
			System.out.println(r[i] +" finished!");
		}
	}
}
