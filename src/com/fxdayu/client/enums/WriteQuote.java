package com.fxdayu.client.enums;

public enum WriteQuote {
	UPDATE(0), INSERT(1), ERROR(0);
	
	private int code;
	
	WriteQuote(int code) {
		this.code = code;
	}
	
}
