package com.jd.message.messagehandler;


public interface IMessageHandler {

	/**
	 * 处理消息操作handler(处理消息逻辑需要自己实现)
	 * @param data
	 */
	public void doMessageHandler(byte[] message);
	
	public void setOffset(long offset);
	
	public long getOffset();
	
	public void setNextOffset(long next);
	
	public long getNextOffset();
	
}
