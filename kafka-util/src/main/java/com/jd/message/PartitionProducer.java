/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2014年12月27日
 */
package com.jd.message;

import java.util.List;

import com.jd.message.exception.UnHandledKafkaException;
import com.jd.message.kafka.bean.PartitionMessage;

public interface PartitionProducer<T>{
	/**
	 * 根据topic批量发送消息,每条消息根据不同的key做分区
	 * @param topic
	 * @param messages
	 * @return
	 * @throws Exception
	 */
	public boolean sendBatchToPartition(String topic, List<PartitionMessage<T>> messages) throws UnHandledKafkaException;
	
	/**
	 * 根据partiton key ,topic 发送单条消息
	 * @param topic
	 * @param messages
	 * @return
	 * @throws Exception
	 */
	public boolean sendToPartition(String topic, String key,T message) throws UnHandledKafkaException ;
	
	/**
	 * 关闭消费者
	 * @throws MetaClientException
	 */
	public void close() throws Exception ;
}
