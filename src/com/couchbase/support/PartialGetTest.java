package com.couchbase.support;

// Brian Williams
// October 5, 2016

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.LegacyDocument;

// If using maven, use this dependency to get the 2.3.4 SDK 

// <dependencies>
// <dependency>
//   <groupId>com.couchbase.client</groupId>
//   <artifactId>java-client</artifactId>
//   <version>2.3.4</version>
//   <scope>compile</scope>
// </dependency>
// </dependencies>


public class PartialGetTest {

	public static void main(String[] args) {
		System.out.println("Welcome to PartialGetTest");

		PartialGetTest tester = new PartialGetTest();

		Cluster cluster = CouchbaseCluster.create("hostname");
		Bucket bucket = cluster.openBucket("BUCKETNAME");

		tester.doExample(bucket, 100);
		tester.doExample(bucket, 200);
		tester.doExample(bucket, 300);
		tester.doExample(bucket, 400);
		tester.doExample(bucket, 500);
		tester.doExample(bucket, 1000);
		tester.doExample(bucket, 2000);
		tester.doExample(bucket, 3000);

		System.out.println("Closing bucket");
		bucket.close();

		System.out.println("Disconnecting cluster");
		cluster.disconnect();

		System.out.println("Now leaving PartialGetTest");

	}

	private void doExample(Bucket bucket, int timeoutInMilliseconds) {

		Map<String, Object> results = new HashMap<>(); 
		AtomicBoolean isTimeout = new AtomicBoolean(false);

		int keysToGenerate = 50000;

		String[] theKeys = new String[keysToGenerate];

		for (int i = 0; i < keysToGenerate; i++) {
			theKeys[i] = "sbfDocument" + i;
		}

		System.out.println("Made a list of " + keysToGenerate + " unique keys.");

		long startTime = System.currentTimeMillis();

		AtomicInteger counter = new AtomicInteger();
		
		Observable.from(theKeys).flatMap(id -> { counter.incrementAndGet(); return bucket.async().get(LegacyDocument.create(id)); }) 
		.doOnNext(doc -> results.put(doc.id(), doc.content())) 
		.last() 
		.timeout(timeoutInMilliseconds, TimeUnit.MILLISECONDS).toBlocking() 
		.subscribe(doc -> { 
			System.out.println("doc");
		}, throwable -> { 
			if (throwable instanceof TimeoutException) { 		
				System.out.println("Got a timeout exception after " + ( System.currentTimeMillis() - startTime) + "ms.");
				isTimeout.set(true); 
			}
			else {
				System.out.println("Unexpected throwable: " + throwable.getClass().getName());
			}
		});		

		System.out.println("After " + (System.currentTimeMillis() - startTime) + "ms, the size of results is " + results.size() + " and counter is " + counter.get());

		boolean doSleeping = false;

		if (doSleeping) {
			try {
				Thread.sleep(5000); // sleep 5 seconds
			}
			catch (Exception e) {
				System.out.println("Exception while sleeping: " + e);
			}

			System.out.println("Done sleeping.  After " + (System.currentTimeMillis() - startTime) + "ms, the size of results is " + results.size());
		}

		boolean doPrinting = false;

		if (doPrinting) {
			int numToPrint = 5;
			for (String eachResult : results.keySet()) {
				if (numToPrint > 0 ) {
					System.out.println("key: " + eachResult);
					numToPrint--;
				}
				else {
					break;
				}
			}
		}

		System.out.println();
		
	} // doExample()

}

// EOF
