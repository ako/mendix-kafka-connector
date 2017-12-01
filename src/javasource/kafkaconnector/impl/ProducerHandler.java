package kafkaconnector.impl;

import static org.apache.commons.lang3.StringUtils.split;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;

import kafkaconnector.proxies.constants.Constants;

public class ProducerHandler {
	private final static ILogNode logNode = Core.getLogger("Kafka Connector");
	private final static Producer<String, String> producer;
	
	static {
		Properties props = new Properties();
		props.put("bootstrap.servers", Constants.getBootstrapServers().contains(",")?split(Constants.getBootstrapServers(), ","):Constants.getBootstrapServers());
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());
		producer = new KafkaProducer<String, String>(props);
		
	}
	
	public static void send(String topic, String key, String message) {
		if (topic == null || topic.isEmpty() || message == null || message.isEmpty()) {
			logNode.error("Unable to send message, because there is no topic or message provided");
			return;
		}
		if (key != null && !key.isEmpty()) {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, key, message);
			producer.send(data);
			if( logNode.isDebugEnabled()) {
				logNode.debug("Producer sent message for topic "+topic+" with key "+key+": "+message);
			}
		} else {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, message);
			producer.send(data);
			if( logNode.isDebugEnabled()) {
				logNode.debug("Producer sent message for topic "+topic+" without key: "+message);
			}
		}
		
	}
	
	public static void shutdown() {
		producer.close();
	}
	
	
	

}
