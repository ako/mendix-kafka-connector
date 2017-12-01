package kafkaconnector.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;

public class ConsumerHandler {
	private final static ILogNode logNode = Core.getLogger("Kafka Connector");	
	private final static ExecutorService executor = Executors.newCachedThreadPool();
	private final static List<Consumer> consumers  = new ArrayList<Consumer>();
	
	public static void startConsumers(int numberOfConsumers, String groupId, String topic, String microflowName) {
		if (groupId == null || groupId.isEmpty() || topic == null || topic.isEmpty() || microflowName == null || microflowName.isEmpty()) {
			logNode.error("Unable to start consumer(s), because there is no consumer group, topic or microflow provided");
			return;
		}
		if (!Core.getMicroflowNames().contains(microflowName)) {
			logNode.error("Unable to start consumer(s), microflow "+microflowName+" could not be found (specificy as ModuleName.MicroflowName)");
			return;
		}
		int newConsumerNumber = consumers.size()==0?0:consumers.size();
	    for (int i = 0;i<numberOfConsumers;i++) {
			Consumer consumer = new Consumer(logNode, i+newConsumerNumber, groupId, topic, microflowName);
			consumers.add(consumer);
			executor.submit(consumer);
		}
	    logNode.info("Started "+numberOfConsumers+" consumers with microflow "+microflowName+" for consumer group "+groupId+" and topic "+topic);
	}
	
	public static List<Consumer> getConsumers() {
		return consumers;
	}
	
	public static void restartConsumer(int consumerNumber) {
		for (Consumer consumer : consumers) {
			if (consumer.getConsumerNumber() == consumerNumber && !(consumer.getRunning())) {
				Consumer newConsumer = new Consumer(logNode, consumerNumber, consumer.getGroupId(), consumer.getTopic(), consumer.getMicroflowName());
				consumers.set(consumerNumber, newConsumer);
				executor.submit(newConsumer);
				if( logNode.isDebugEnabled()) {
					logNode.debug("Restarted consumer "+consumerNumber+" subscribed to topic "+consumer.getTopic());
				}
			}
	    } 
	}
	
	public static void stopConsumer(int consumerNumber) {
		for (Consumer consumer : consumers) {
			if (consumer.getConsumerNumber() == consumerNumber && consumer.getRunning())
				consumer.close();
	    } 
	}

	public static void shutdown() {
	      for (Consumer consumer : consumers) {
	        consumer.close();
	      } 
	      executor.shutdown();
	      try {
	        executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
	        logNode.info("Closed consumer handler");
	      } catch (InterruptedException e) {
	    	logNode.error("Error when closing consumer handler "+e.getStackTrace());
	      }
	    }

}
