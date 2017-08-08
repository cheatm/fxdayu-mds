package com.fxdayu.client.enums;


public enum PriceTick {
	PRICE("price"), VOLUME("volume"), ASK("ask"), BID("bid");
	
	public String value;
	
	PriceTick(String name) {
		this.value = name;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.value;
	}
}
