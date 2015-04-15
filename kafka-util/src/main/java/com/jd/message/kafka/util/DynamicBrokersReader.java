package com.jd.message.kafka.util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.utils.ZkUtils;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.jd.message.kafka.bean.BrokerInfo;

/**
 * Kafka 读取broker，topic等信息操作类
 *
 */
public class DynamicBrokersReader {

	private static final Logger logger = Logger.getLogger(DynamicBrokersReader.class);

	private CuratorFramework _curator;
	private static DynamicBrokersReader dbr;
	public static final int INTERVAL_IN_MS = 100;

	private DynamicBrokersReader() {
	}

	public static DynamicBrokersReader getInstance(String zooKeeper) throws Exception {
		if (dbr == null) {
			dbr = new DynamicBrokersReader();
			dbr.init(zooKeeper);
		}
		return dbr;
	}

	public void init(String zkStr) throws Exception {
		try {
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			_curator = CuratorFrameworkFactory.builder().connectString(zkStr).connectionTimeoutMs(10 * 1000).sessionTimeoutMs(5 * 1000).retryPolicy(retryPolicy).build();
			_curator.start();
		} catch (Exception ex) {
			logger.error("can't connect to zookeeper");
			throw ex;
		}
	}

	/**
	 * brokerInfoPath为固定值:/brokers/ids 每个broker几点他的content为: { "version": 1, "host": "192.168.1.148", "port": 9092, "jmx_port": 9999 }的字符串
	 * 
	 * @return
	 * @throws Exception
	 */
	public List<BrokerInfo> getBrokerListInfo() throws Exception {
		List<BrokerInfo> brokers = new ArrayList<BrokerInfo>();
		try {
			String brokerInfoPath = ZkUtils.BrokerIdsPath();// 固定值:/brokers/ids
			List<String> brokersPath = _curator.getChildren().forPath(brokerInfoPath);
			for (int i = 0; i < brokersPath.size(); i++) {
				byte[] brokerbyte = _curator.getData().forPath(brokerInfoPath + "/" + brokersPath.get(i));
				String data = new String(brokerbyte,"UTF-8");
				Map<Object,Object> dmap = (Map<Object, Object>) JSON.parse(data);
				BrokerInfo brokerInfo = new BrokerInfo(dmap.get("host").toString(),Integer.valueOf(dmap.get("port").toString()));
				brokers.add(brokerInfo);
			}
		} catch (Exception e) {
			throw e;
		}
		return brokers;
	}
	
	public String getBrokerList() throws Exception {
		StringBuilder sb = new StringBuilder();
		try {
			String brokerInfoPath = ZkUtils.BrokerIdsPath();// 固定值:/brokers/ids
			List<String> brokersPath = _curator.getChildren().forPath(brokerInfoPath);
			for (int i = 0; i < brokersPath.size(); i++) {
				byte[] brokerbyte = _curator.getData().forPath(brokerInfoPath + "/" + brokersPath.get(i));
				sb.append(getBrokerHost(brokerbyte));
				if(i != brokersPath.size() - 1)
					sb.append(",");
			}
		} catch (Exception e) {
			throw e;
		}
		return sb.toString();
	}
	
	/**
	 * 返回topic下partition的个数
	 * @param topic
	 * @return
	 * @throws Exception
	 */
	public int getNumPartitionsByTopic(String topic) throws Exception {
		try {
			String topicBrokersPath = partitionPath(topic);
			List<String> children = _curator.getChildren().forPath(topicBrokersPath);
			return children.size();
		} catch (Exception e) {
			throw e;
		}
	}

	public Map<String, Integer> getNumPartitionsByTopics(Set<String> topics) {
		try {
			if (topics.size() <= 0) {
				return null;
			}
			Map<String, Integer> result = new HashMap<String, Integer>();
			for (String topic : topics) {
				String topicBrokersPath = partitionPath(topic);
				List<String> children = _curator.getChildren().forPath(topicBrokersPath);
				result.put(topic, children.size());
			}
			return result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * brokerTopicPath为固定值:/brokers/topics
	 * @param topic
	 * @return /brokers/topics/[topic]/partitions
	 */
	private String partitionPath(String topic) {
		return ZkUtils.BrokerTopicsPath() + "/" + topic + "/partitions";
	}

	/**
	 * get /brokers/topics/distributedTopic/partitions/1/state { "controller_epoch":4, "isr":[ 1, 0
	 * ], "leader":1, "leader_epoch":1, "version":1 }
	 * 
	 * @param partition
	 * @return
	 */
	// private int getLeaderFor(String topic,long partition) {
	// try {
	// String topicBrokersPath = partitionPath(topic);
	// byte[] hostPortData = _curator.getData().forPath(topicBrokersPath + "/" + partition +
	// "/state" );
	// Map<Object, Object> value = (Map<Object,Object>) JSON.parse(new String(hostPortData,
	// "UTF-8"));
	// Integer leader = ((Number) value.get("leader")).intValue();
	// return leader;
	// } catch (Exception e) {
	// throw new RuntimeException(e);
	// }
	// }

	public void close() {
		_curator.close();
		dbr = null;
	}

	/**
	 *
	 * [zk: localhost:2181(CONNECTED) 56] get /brokers/ids/0 { "host":"localhost", "jmx_port":9999,
	 * "port":9092, "version":1 }
	 *
	 * @param contents
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private String getBrokerHost(byte[] contents) {
		try {
			Map<Object, Object> value = (Map<Object, Object>) JSON.parse(new String(contents, "UTF-8"));
			String host = (String) value.get("host");
			Integer port = ((Integer) value.get("port")).intValue();
			return host + ":" + port;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) throws Exception {
		String zooKeeper = "192.168.213.41:2181,192.168.213.43:2181,192.168.213.50:2181/kafka";
		String topic = "qiaochaotest1";
		DynamicBrokersReader rd = DynamicBrokersReader.getInstance(zooKeeper);

		System.out.println("===============================================");
		System.out.println(rd.getBrokerListInfo());

		Thread.sleep(5000);

		System.out.println(rd.getNumPartitionsByTopic(topic));
		System.out.println("===============================================");

	}

}
