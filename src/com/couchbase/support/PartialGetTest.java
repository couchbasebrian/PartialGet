package com.couchbase.support;

// Brian Williams
// October 5, 2016
// Updated December 16, 2016

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import rx.functions.Func1;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.LegacyDocument;
import com.couchbase.client.java.util.retry.RetryBuilder;

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

		Cluster cluster = CouchbaseCluster.create("172.23.99.170");
		Bucket bucket = cluster.openBucket("BUCKETNAME");

		// Do a number of tests in 100ms increments
		tester.doExample(bucket, 100);
		tester.doExample(bucket, 200);
		tester.doExample(bucket, 300);
		tester.doExample(bucket, 400);
		tester.doExample(bucket, 500);
		tester.doExample(bucket, 600);
		tester.doExample(bucket, 700);
		tester.doExample(bucket, 800);
		tester.doExample(bucket, 900);
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

		Map<String, Object> results = new ConcurrentHashMap<>(); 
		AtomicBoolean isTimeout = new AtomicBoolean(false);

		int keysToGenerate = 50000;

		String[] theKeys = new String[keysToGenerate];

		for (int i = 0; i < keysToGenerate; i++) {
			theKeys[i] = "sbfDocument" + i;
		}

		System.out.println("Made a list of " + keysToGenerate + " unique keys.");

		AtomicInteger counter = new AtomicInteger();

		long startTime = System.currentTimeMillis();

		// new way from http://hastebin.com/nugufeculi.java
		rx.Observable.from(theKeys).flatMap(id ->
		{
			counter.incrementAndGet();
			return rx.Observable.defer(() -> bucket.async().get(LegacyDocument.create(id))
					.retryWhen(RetryBuilder.anyOf(BackpressureException.class) //only retry on Backpressure
							.max(2) //you could tune the maximum number of attempts here
							.delay(Delay.exponential(TimeUnit.MILLISECONDS, 1, 2)) //delay between attempts grows exponentially
							.build())
							.onErrorResumeNext(new Func1<Throwable, rx.Observable<? extends LegacyDocument>>() {
								@Override
								public rx.Observable<? extends LegacyDocument> call(Throwable throwable) {
									if (throwable instanceof TimeoutException) {
										isTimeout.set(true);
										//if one request is slow and times out, we should not proactively cancel other requests
										return rx.Observable.just(null);
									} else {
										return rx.Observable.error(throwable);
									}
								}
							}));
		})
		.doOnNext(doc -> {
			if (doc != null) {
				results.put(doc.id(), doc.content());
			}
		})
		.timeout(500, TimeUnit.MILLISECONDS)
		.last()
		.toBlocking()
		.subscribe(doc -> {
		}, throwable -> {
			if (throwable instanceof BackpressureException) {
				System.out.println("Got back pressure exception after " + (System.currentTimeMillis() - startTime) + "ms.");
			} else {
				System.out.println("Unexpected throwable: " + throwable.getClass().getName());
			}
		});

		System.out.println(isTimeout.get());

		System.out.println("After " + (System.currentTimeMillis() - startTime) + "ms, the size of results is " + results.size() + " and counter is " + counter.get());

		boolean doSleeping = false;

		if (doSleeping) {
			try {
				Thread.sleep(5000); // sleep 5 seconds
			}
			catch (Exception e) {
				System.out.println("Unexpected exception while sleeping: " + e);
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