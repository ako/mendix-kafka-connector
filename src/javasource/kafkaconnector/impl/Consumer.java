package kafkaconnector.impl;

import static org.apache.commons.lang3.StringUtils.split;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;

import kafkaconnector.proxies.constants.Constants;

public class Consumer implements Runnable {
	private ILogNode logNode;
	
	private final KafkaConsumer<String, String> consumer;
	
	private final int consumerNumber;
	private final String groupId;
	private final String topic;
	private final String microflowName;
	private boolean running = false;
	
	public Consumer(ILogNode logNode, int consumerNumber, String groupId, String topic, String microflowName) {
		this.logNode = logNode;
		this.consumerNumber = consumerNumber;
		this.groupId = groupId;
	    this.topic = topic;	    
	    this.microflowName = microflowName;
		Properties props = new Properties();
		props.put("bootstrap.servers", Constants.getBootstrapServers().contains(",")?split(Constants.getBootstrapServers(), ","):Constants.getBootstrapServers());
		props.put("group.id", groupId);
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<>(props);
	}
	
	@Override
	public void run() {
		try {
			List<String> topics = Arrays.asList(topic);
			consumer.subscribe(topics);
			if( logNode.isDebugEnabled()) {
				logNode.debug("Consumer "+consumerNumber+" subscribed to topic "+topic);
			}
			this.running = true;

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, String> record : records) {
					if( logNode.isDebugEnabled()) {
						logNode.debug("Consumer "+consumerNumber+" received value for microflow "+this.microflowName+" with topic "+record.topic()+" from partition "+record.partition()+" with offset "+record.offset()+": " + record.value());
					}
					Map<String,Object> parameters = new HashMap<String, Object>();
					parameters.put("Message", record.value());
					try {
						IContext systemContext = Core.createSystemContext();
						Core.execute(systemContext, this.microflowName, parameters);
					} catch (CoreException e) {
						logNode.error("Error in microflow "+this.microflowName+" for topic "+record.topic()+" and value "+record.value()+": "+e.getMessage());
					}
				}
			}
		} catch (WakeupException e) {
			// ignore for shutdown 
		} finally {
			consumer.close();
			this.running = false;
			if( logNode.isDebugEnabled()) {
				logNode.debug("Consumer "+consumerNumber+" closed");
			}
		}
	}
	
	public int getConsumerNumber() {
		return consumerNumber;
	}
	
	public String getGroupId() {
		return groupId;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public String getMicroflowName() {
		return microflowName;
	}
	
	public boolean getRunning() {
		return running;
	}
	
	public void close(){
		consumer.wakeup();
	}
}
