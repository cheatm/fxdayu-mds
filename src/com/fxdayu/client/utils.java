package com.fxdayu.client;

public class utils {
	public static String unfold(String code){
		if (code.length()==6) {
			if (code.startsWith("6")) {
				return code + ".XSHG";
			}else if(code.startsWith("0") || code.startsWith("3")){
				return code + ".XSHE";
			}
			
		}
		return String.valueOf(code);
	}
	
	public static String fold(String code){
		if (code.length()==11) {
			return code.substring(0, 6);
		}else{
			return code;
		}
	}

}
