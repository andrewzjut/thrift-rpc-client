package com.trcloud.thrift.util;


import com.trcloud.thrift.service.KafkaService;
import com.trcloud.thrift.service.KafkaService.AsyncClient;
import com.trcloud.thrift.service.KafkaService.AsyncClient.Factory;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/**
 * Created by hzzt on 2016/9/29.
 */
public class ThriftPool {
	public static abstract class ThriftReq {
		private void go(final ThriftPool thrift, final AsyncClient client) {
			on(thrift, client);
		}

		public abstract void on(ThriftPool thrift, AsyncClient client);
	}

	private final ConcurrentLinkedQueue<AsyncClient> instances = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<ThriftReq> requests = new ConcurrentLinkedQueue<>();
	private Executor executor = null;

	public ThriftPool(final int clients,
					  final String host,
					  final int port) throws IOException {
		TProtocolFactory protocolFactory = new TCompactProtocol.Factory();
		Factory factory = null;
		for (int i = 0; i < clients; i++) {
			factory = new Factory(new TAsyncClientManager(), protocolFactory);
			instances.add(factory.getAsyncClient(new TNonblockingSocket(host, port)));
		}

		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					ThriftReq req;
					if ((req = requests.poll()) != null) {
						AsyncClient client;
						if ((client = instances.poll()) != null) {
							req.go(ThriftPool.this, client);
						} else {
							requests.add(req);
						}
					} else {
						try {
							Thread.sleep(10);
						} catch (Exception e) {

						}
					}
				}
			}
		}).start();
	}

	public ThriftPool(Executor executor,
					  final int clients,
					  final String host,
					  final int port) throws IOException {
		this.executor = executor;
	}

	public void req(final ThriftReq request) {
		final AsyncClient client;
		synchronized (instances) {
			client = instances.poll();
		}
		if (client != null) {
			if (executor != null) {
				executor.execute(new Runnable() {
					@Override
					public void run() {
						request.go(ThriftPool.this, client);
					}
				});
			} else {
				request.go(this, client);
			}
			return;
		}
		requests.add(request);
	}

	public void addClient(AsyncClient client) {
		instances.add(client);
	}
	public void close(){
		Iterator<AsyncClient> it = instances.iterator();
		while (it.hasNext()){
			AsyncClient client = it.next();

		}
	}
}

