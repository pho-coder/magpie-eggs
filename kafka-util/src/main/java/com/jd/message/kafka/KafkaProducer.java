/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2014年12月26日
 */
package com.jd.message.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.jd.message.exception.UnHandledKafkaException;
import com.jd.message.kafka.bean.PartitionMessage;
import com.jd.message.kafka.configuration.KafkaConfiguration;
import com.jd.message.kafka.util.DynamicBrokersReader;

public class KafkaProducer<T> implements com.jd.message.Producer<T>,com.jd.message.PartitionProducer<T> {
	private static final Logger logger = Logger.getLogger(KafkaProducer.class);
	private Producer<String, T> producer;

	public static <T> KafkaProducer<T> createProducer(KafkaConfiguration conf) throws Exception {
		return new KafkaProducer<T>(conf);
	}

	private KafkaProducer(KafkaConfiguration conf) throws Exception {
		init(conf);
	}

	private void init(KafkaConfiguration conf) throws Exception {
		Properties props = new Properties();
		props = prepareConfig(props,conf);
		// 创建producer
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, T>(config);
		logger.info("kafka init producer ok !");
	}

	public Properties prepareConfig(Properties props,KafkaConfiguration conf) throws Exception {
		if (conf == null)
			throw new NullPointerException("KafkaConfiguration is null");
		// 获得zk_connect. eg:host1:port,host2:port/root
		String zk_connect = conf.getZk_connect();
		if (StringUtils.isBlank(zk_connect))
			throw new NullPointerException("zk_connect is null");
		String zk_root = conf.getZk_root();
		if (StringUtils.isBlank(zk_root)) {
			zk_root = "/kafka";
			conf.setZk_root(zk_root);
		}
		props.put("zookeeper.connect", zk_connect + zk_root);
		// 定义zk connection 
		if (conf.get("zookeeper.connection.timeout.ms") != null && StringUtils.isNotBlank(conf.get("zookeeper.connection.timeout.ms").toString()))
			props.put("zookeeper.connection.timeout.ms", conf.get("zookeeper.connection.timeout.ms").toString());
		
		// 定义zk session的超时时间
		if (conf.get("zookeeper.session.timeout.ms") != null && StringUtils.isNotBlank(conf.get("zookeeper.session.timeout.ms").toString()))
			props.put("zookeeper.session.timeout.ms", conf.get("zookeeper.session.timeout.ms").toString());
		
		// 定义消息的序列化类,默认消息的key采用key.serializer.class标明的序列化类
		if (conf.get("serializer.class") != null && StringUtils.isNotBlank(conf.get("serializer.class").toString()))
			props.put("serializer.class", conf.get("serializer.class").toString());
		
		//默认key的序列化均为kafka.serializer.StringEncoder
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		
		// 定义producer是同步发送还是异步发送,默认异步
		if (conf.get("producer.type") != null && StringUtils.isNotBlank(conf.get("producer.type").toString())) {
			props.put("producer.type", conf.get("producer.type").toString());
		}
		
		// 定义producer ack备份数量
		if (conf.get("request.required.acks") != null && StringUtils.isNotBlank(conf.get("request.required.acks").toString())) {
			props.put("request.required.acks", conf.get("request.required.acks").toString());
		}
		
		// 从zk的kafka path下获得brokerlist
		DynamicBrokersReader rd = DynamicBrokersReader.getInstance(zk_connect + zk_root);
		String brokerlist = rd.getBrokerList();
		rd.close();
		props.put("metadata.broker.list", brokerlist);
		return props;
	}

	@Override
	public boolean sendBatch(String topic, List<T> messages) throws UnHandledKafkaException {
		boolean isSendSuccess = false;
		try {
			if (messages.size() <= 0) {
				return true;
			}
			List<KeyedMessage<String, T>> datalist = new ArrayList<KeyedMessage<String, T>>();
			for (T message : messages) {
				datalist.add(new KeyedMessage<String, T>(topic, message));
			}
			producer.send(datalist);
			isSendSuccess = true;
		} catch (Exception e) {
			logger.error("Kafka producer error ！" + e.getMessage(), e);
			throw new UnHandledKafkaException(e);
		}
		return isSendSuccess;
	}

	/**
	 * 发送messegs,每个message都有指定的key
	 * 
	 * @param topic
	 * @param messages
	 * @return
	 * @throws Exception
	 */
	public boolean sendBatchToPartition(String topic, List<PartitionMessage<T>> messages) throws UnHandledKafkaException {
		boolean isSendSuccess = false;
		try {
			if (messages.size() <= 0) {
				return true;
			}
			List<KeyedMessage<String, T>> datalist = new ArrayList<KeyedMessage<String, T>>();
			for (PartitionMessage<T> message : messages) {
//				logger.info("key " + message.getKey());
				datalist.add(new KeyedMessage<String, T>(topic, message.getKey(), message.getValue()));
			}
			producer.send(datalist);
			isSendSuccess = true;
		} catch (Exception e) {
			logger.error("Kafka producer error ！" + e.getMessage(), e);
			throw new UnHandledKafkaException(e);
		}
		return isSendSuccess;
	}

	@Override
	public boolean send(String topic, T message) throws UnHandledKafkaException {
		boolean isSendSuccess = false;
		try {
			KeyedMessage<String, T> data = new KeyedMessage<String, T>(topic, message);
			producer.send(data);
			isSendSuccess = true;
		} catch (Exception e) {
			logger.error("Kafka producer error ！" + e.getMessage(), e);
			throw new UnHandledKafkaException(e);
		}
		return isSendSuccess;
	}
	
	@Override
	public boolean sendToPartition(String topic, String key, T message) throws UnHandledKafkaException {
		boolean isSendSuccess = false;
		try {
			KeyedMessage<String, T> data = new KeyedMessage<String, T>(topic, key,message);
			producer.send(data);
			isSendSuccess = true;
		} catch (Exception e) {
			logger.error("Kafka producer error ！" + e.getMessage(), e);
			throw new UnHandledKafkaException(e);
		}
		return isSendSuccess;
	}
	
	@Override
	public void close() throws Exception {
		if (producer != null)
			producer.close();
	}
}
