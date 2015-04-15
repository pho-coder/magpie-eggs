/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2014年12月26日
 */
package com.jd.message.messagehandler;

import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

public class DefaultTextMessageHandler extends MessageHandler{
	private static final Logger logger = Logger.getLogger(DefaultTextMessageHandler.class);
	public void doMessageHandler(byte[] message) {
		try {
			logger.info("Recieve msg " + new String(message,"UTF-8"));
		} catch (UnsupportedEncodingException e) {
			logger.error(e.getMessage(),e);
		}
	}

}