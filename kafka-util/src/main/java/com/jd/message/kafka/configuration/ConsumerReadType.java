/**
 * @Title:
 * @Desciption:
 * @Author:liurx
 * @Time:2014年12月29日
 */
package com.jd.message.kafka.configuration;

public enum ConsumerReadType {
	ERLIEST
	{
		@Override
		public String getTypeName() {
			return "earlist";
		}

		@Override
		public long getWhileTime() {
			return kafka.api.OffsetRequest.EarliestTime();
		}
	},	
	LASTEST{
		@Override
		public String getTypeName() {
			return "lastest";
		}

		@Override
		public long getWhileTime() {
			return kafka.api.OffsetRequest.LatestTime();
		}
	}
		;
	public static ConsumerReadType getType(String type){
		if(type.equals(ConsumerReadType.ERLIEST.getTypeName()))
			return ConsumerReadType.ERLIEST;
		else if(type.equals(ConsumerReadType.LASTEST.getTypeName()))
			return ConsumerReadType.LASTEST;
		else
			return null;
	}

	public abstract String getTypeName();
	
	public abstract long getWhileTime();
	
}
