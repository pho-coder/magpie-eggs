/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2015年1月26日
 */
package com.jd.message.kafka.bean;

public class PartitionMessage<T> {
	private String key;
	private T value;
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public T getValue() {
		return value;
	}
	public void setValue(T value) {
		this.value = value;
	}
	
	
}
