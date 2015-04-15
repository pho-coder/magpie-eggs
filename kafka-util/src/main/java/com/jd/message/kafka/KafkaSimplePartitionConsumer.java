/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2014年12月26日
 */
package com.jd.message.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.jd.message.Consumer;
import com.jd.message.kafka.bean.BrokerInfo;
import com.jd.message.kafka.bean.ConsumeMessage;
import com.jd.message.kafka.configuration.ConsumerReadType;
import com.jd.message.kafka.configuration.KafkaConfiguration;
import com.jd.message.kafka.util.DynamicBrokersReader;
import com.jd.message.messagehandler.DefaultTextMessageHandler;
import com.jd.message.messagehandler.IMessageHandler;

public class KafkaSimplePartitionConsumer implements Consumer {
	private static final Logger logger = Logger.getLogger(KafkaSimplePartitionConsumer.class);
	private SimpleConsumer simpleConsumer;
	private List<BrokerInfo> m_replicaBrokers = new ArrayList<BrokerInfo>();
	private final static int kafkaBufferSize = 1024 * 1024;
	private final static int connectionTimeOut = 100000;
	private String clientName;
	private IMessageHandler defhandler;
	private BrokerInfo leader;
	private ConsumerReadType readtype = ConsumerReadType.ERLIEST;
	private LinkedBlockingQueue<ConsumeMessage> queue;
	private static final int CONSUMER_QUEUE_CAPACITY = 10000;
	private int partitionNum;
	private String topic;
	
	public static KafkaSimplePartitionConsumer createConsumer(KafkaConfiguration conf, String topic) throws Exception {
		return new KafkaSimplePartitionConsumer(conf, topic);
	}
	
	public void setReadType(ConsumerReadType readtype){
		this.readtype = readtype;
	}
	
	private KafkaSimplePartitionConsumer(KafkaConfiguration conf, String topic) throws Exception {
		init(conf, topic);
	}

	private void init(KafkaConfiguration conf, String topic) throws Exception {
		if (checkNull(conf))
			throw new NullPointerException("KafkaConfiguration is null");
		
		// 获得zk_connect. eg:host1:port,host2:port/root
		String zk_connect = conf.getZk_connect();
		if (StringUtils.isBlank(zk_connect))
			throw new NullPointerException("zk_connect is null");
		String zk_root = conf.getZk_root();//默认/kafka
		
		// 通过zk,获得topic的brokerinfo(host,port)
		DynamicBrokersReader rd = DynamicBrokersReader.getInstance(zk_connect + zk_root);
		List<BrokerInfo> brokers = rd.getBrokerListInfo();
		partitionNum = rd.getNumPartitionsByTopic(topic);
		logger.info("Topic " + topic + ", partitionNum " + partitionNum);
		
		//获得leader
		PartitionMetadata metadata = findLeader(brokers, topic, partitionNum);// 0 为partion num 因为是单partition,默认为0
		if (checkNull(metadata)) {
			logger.info("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (checkNull(metadata.leader())) {
			logger.info("Can't find Leader for Topic and Partition. Exiting");
			return;
		}
		String leaderBroker = metadata.leader().host();
		int leaderPort = metadata.leader().port();
		this.leader = new BrokerInfo(leaderBroker, leaderPort);
		this.simpleConsumer = new SimpleConsumer(leaderBroker, leaderPort, connectionTimeOut, kafkaBufferSize, clientName);
		this.defhandler = new DefaultTextMessageHandler();
		
		//client name
		Object cn = conf.get("client.name");
		if(!checkNull(cn))
			this.clientName = cn.toString();
		else
			this.clientName = "Client_" + topic + "_" + partitionNum;
		
		// queue
		Object queuesize = conf.get("consumer.queue.size");
		int capacity = CONSUMER_QUEUE_CAPACITY;
		if(!checkNull(queuesize))
			capacity = (int)queuesize;
		this.queue = new LinkedBlockingQueue<ConsumeMessage>(capacity);
		
		//topic
		this.topic = topic;
	}
	
	@Override
	public void consumeMessage() throws Exception {
		consume(-1, null);
	}
	
	@Override
	public void consumeMessage(long offset) throws Exception {
		consume(offset, null);
	}
	
	@Override
	public void consumeMessage(IMessageHandler messagehandler) throws Exception {
		consume(-1, messagehandler);
	}
	
	@Override
	public void consumeMessage(long offset, IMessageHandler messagehandler) throws Exception {
		consume(offset, messagehandler);
	}

	public void close() throws Exception {
		if (simpleConsumer != null)
			simpleConsumer.close();
	}
	
	public boolean checkNull(Object obj){
		return obj == null;
	}
	
	/**
	 * 
	 * @param <T>
	 * @param topic
	 *            : 消费的topic,与初始化时传入的topic要一至
	 * @param readOffset
	 *            :读取的位置,如果为-1,代表从最大位置开始读取
	 * @param partitionNum
	 *            : partition num 由于是simple partition,所以num = 0
	 * @throws Exception 
	 */
	private void consume(long readOffset, IMessageHandler messagehandler) throws Exception {
		int numErrors = 0;
		boolean getdata = false;
		do {
			logger.debug("Begin to read from offset " + readOffset);
			if (simpleConsumer == null) {
				logger.info("SimpleConsumer is null, create a new one....");
				simpleConsumer = new SimpleConsumer(leader.getHost(), leader.getPort(), connectionTimeOut, kafkaBufferSize, clientName);
			}
			FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(topic, partitionNum, readOffset, 100000).build();
			FetchResponse fetchResponse = simpleConsumer.fetch(req);
			if (fetchResponse.hasError()) {
				short code = fetchResponse.errorCode(topic, partitionNum);
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					switch(readtype){
						case ERLIEST:
							readOffset = getLastOffset(simpleConsumer,topic, partitionNum, kafka.api.OffsetRequest.EarliestTime(), clientName);
							break;
						case LASTEST:
							readOffset = getLastOffset(simpleConsumer,topic, partitionNum, kafka.api.OffsetRequest.LatestTime(), clientName);
							break;
					}
					logger.info("Out of Range .... newoffset " + readOffset);
					Thread.sleep(10000L);
					continue;
				}
				numErrors++;
				logger.info("Error fetching data from the Broker Reason: " + code);
				if(numErrors > 5)
					break;
				simpleConsumer.close();
				simpleConsumer = null;
				leader = findNewLeader(leader, topic, partitionNum);
				continue;
			}
			numErrors = 0;
			long numRead = 0;
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partitionNum)) {
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				if(messagehandler != null){//如果传入了handler默认采用handler处理
					messagehandler.setOffset(messageAndOffset.offset());
					messagehandler.setNextOffset(messageAndOffset.nextOffset());
					messagehandler.doMessageHandler(bytes);
				}else{//否则使用内部队列保存数据
					queue.put(new ConsumeMessage(bytes,messageAndOffset.offset(),messageAndOffset.nextOffset()));
				}
				numRead++;
			}
			logger.debug("KafkaSimpleConsumer read end....");
			if (numRead == 0) {
				try {
					Thread.sleep(1000);
					logger.debug("We do not get any data....");
				} catch (InterruptedException ie) {
				}
			}else{
				getdata = true;
			}
		}while(!getdata);
		//close();
	}
	
	public LinkedBlockingQueue<ConsumeMessage> getMessageQueue(){
		return queue;
	}
	
	private PartitionMetadata findLeader(List<BrokerInfo> brokers, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;
		loop: for (BrokerInfo broker : brokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(broker.getHost(), broker.getPort(), 100000, 64 * 1024, "leaderLookup");
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				logger.error("Error communicating with Broker [" + broker.getHost() + "] to find Leader for [" + a_topic + ", " + a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(new BrokerInfo(replica.host(), replica.port()));
			}
		}
		return returnMetaData;
	}

	private BrokerInfo findNewLeader(BrokerInfo oldLeader, String a_topic, int a_partition) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (oldLeader.getHost().equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				goToSleep = true;
			} else {
				return new BrokerInfo(metadata.leader().host(), metadata.leader().port());
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		logger.info("Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}

	private long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			logger.info("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
	
	public long getCurrentMaxOffset(String topic){
		return getCurrentMaxOffset(simpleConsumer, topic, 0);
	}
	
	public long getCurrentMaxOffset(SimpleConsumer consumer, String topic, int partition){
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			logger.info("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0] - 1;
	}
}
