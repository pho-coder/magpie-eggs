/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2014年12月25日
 */
package com.jd.message.kafka.configuration;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class KafkaConfiguration {
	private String zk_connect;
	private String zk_root = "/kafka";
	private Map<String,Object> others = new HashMap<String,Object>();
	
	public String getZk_connect() {
		return zk_connect;
	}
	public void setZk_connect(String zk_connect) {
		this.zk_connect = zk_connect;
	}
	public String getZk_root() {
		return zk_root;
	}
	public void setZk_root(String zk_root) {
		this.zk_root = zk_root;
	}
	public void set(String key,Object value){
		others.put(key, value);
	}
	public Object get(String key){
		return others.get(key);
	}
	public void setAll(Map<String,? extends Object> map){
		others.putAll(map);
	}
	
}
