package com.trcloud.thrift.util;


import com.trcloud.thrift.service.KafkaService.AsyncClient;
import com.trcloud.thrift.service.Status;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by hzzt on 2016/9/29.
 */
public class ThriftAsynRequest extends ThriftPool.ThriftReq {
	private static final Logger logger = LoggerFactory.getLogger(ThriftAsynRequest.class);

	private KafkaRecord record;

	public ThriftAsynRequest(KafkaRecord record) {
		this.record = record;
	}

	@Override
	public void on(final ThriftPool thrift, final AsyncClient client) {
		try {

			client.sendMessage(
					this.record.getTopic(),
					this.record.getValue(),
					new AsyncMethodCallback<AsyncClient.sendMessage_call>() {
						@Override
						public void onComplete(AsyncClient.sendMessage_call response) {
							try {
								if (Status.SUCCESS == response.getResult().getStatus()) {
									logger.info("Complete:" + response.getResult());
								} else if (Status.EXCEPTION == response.getResult().getStatus()) {
									logger.error("Send failed:" + response.getResult());
									logger.error("Send message:{}" + response.getResult().getMessage()
											+ " error with exception:{}" + response.getResult().getThriftException().getMessage());
								}
							} catch (TException e) {
								e.printStackTrace();
							} finally {
								thrift.addClient(client);
							}
						}

						@Override
						public void onError(Exception exception) {
							logger.error("Error: " + exception.getMessage());
							thrift.addClient(client);
						}
					});
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}
