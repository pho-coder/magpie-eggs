/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2014年12月25日
 */
package com.jd.message;

import com.jd.message.messagehandler.IMessageHandler;


public interface Consumer {
	
	/**
	 * 消费topic中的消息
	 * 
	 * @param topic 
	 * @throws Exception
	 */
	public void consumeMessage() throws Exception;
	
	/**
	 * 从指定消费topic中的消息
	 * 
	 * @param topic 
	 * @throws Exception
	 */
	public void consumeMessage(long offset) throws Exception;
	
	/**
	 * 采用指定handler,从指定位置消费topic中的消息
	 * @param topic 
	 * @param messagehandler -- 处理消息操作handler(处理消息逻辑需要自己实现) 
	 * @throws Exception
	 */
	public void consumeMessage(long offset,IMessageHandler messagehandler) throws Exception ;
	
	/**
	 * 采用指定handler消费topic中的消息
	 * @param topic 
	 * @param messagehandler -- 处理消息操作handler(处理消息逻辑需要自己实现) 
	 * @throws Exception
	 */
	public void consumeMessage(IMessageHandler messagehandler) throws Exception ;
	
	/**
	 * 关闭消费者
	 * @throws MetaClientException
	 */
	public void close() throws Exception ;
}
