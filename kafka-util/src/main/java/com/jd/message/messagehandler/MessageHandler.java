/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2014年12月27日
 */
package com.jd.message.messagehandler;

public abstract class MessageHandler implements IMessageHandler {
	private long offset = 0; 
	private long nextOffset = 1;
	@Override
	public abstract void doMessageHandler(byte[] message);

	@Override
	public void setOffset(long offset) {
		this.offset = offset;
	}

	@Override
	public long getOffset() {
		return offset;
	}
	@Override
	public long getNextOffset() {
		return nextOffset;
	}
	@Override
	public void setNextOffset(long nextOffset) {
		this.nextOffset = nextOffset;
	}
	
}
