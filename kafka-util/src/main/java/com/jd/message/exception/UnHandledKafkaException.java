/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2015年3月24日
 */
package com.jd.message.exception;

public class UnHandledKafkaException extends Exception{

	private static final long serialVersionUID = 6770057261124129788L;
	
	public UnHandledKafkaException(Exception e){
		super(e);
	}
	
	public UnHandledKafkaException(String msg){
		super(msg);
	}
	
	public UnHandledKafkaException(String msg,Exception e){
		super(msg,e);
	}
	
}
