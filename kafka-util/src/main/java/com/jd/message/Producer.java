/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2014年12月25日
 */
package com.jd.message;

import java.util.List;

import com.jd.message.exception.UnHandledKafkaException;

public interface Producer<T> {
	/**
	 * 根据topic批量发送消息
	 * @param topic
	 * @param messages
	 * @return
	 * @throws Exception
	 */
	public boolean sendBatch(String topic, List<T> messages) throws UnHandledKafkaException;
	
	/**
	 * 根据topic 发送单条消息
	 * @param topic
	 * @param messages
	 * @return
	 * @throws Exception
	 */
	public boolean send(String topic, T message) throws UnHandledKafkaException ;
	
	/**
	 * 关闭消费者
	 * @throws MetaClientException
	 */
	public void close() throws Exception ;
}
