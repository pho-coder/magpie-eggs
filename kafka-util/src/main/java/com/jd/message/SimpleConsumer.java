/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2014年12月27日
 */
package com.jd.message;

public interface SimpleConsumer {
	public void listen(String topic);
	public void listen(String topic,long offset);//从指定偏移点开始消费
	public void close();
}
