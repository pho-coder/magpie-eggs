/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2015年4月14日
 */
package com.jd.message.kafka.bean;

public class ConsumeMessage {
	private byte[] bs;
	private long currentOffset;
	private long nextOffset;
	
	public ConsumeMessage(byte[] bs,long curr,long next){
		this.bs = bs;
		this.currentOffset = curr;
		this.nextOffset = next;
	}
	
	public byte[] getBs() {
		return bs;
	}
	public void setBs(byte[] bs) {
		this.bs = bs;
	}
	public long getCurrentOffset() {
		return currentOffset;
	}
	public void setCurrentOffset(long currentOffset) {
		this.currentOffset = currentOffset;
	}
	public long getNextOffset() {
		return nextOffset;
	}
	public void setNextOffset(long nextOffset) {
		this.nextOffset = nextOffset;
	}
	
	
}
