package com.fxdayu.client;

import java.util.Date;

import javax.xml.crypto.Data;

public class TimeMonitor extends Thread {
	
	private static int count = 0;
	private static long mSecond = 1000;
	private static long sleepTime = mSecond*60*1; 
	private static final int BEGIN = 93000;
	private static final int END = 150000;
	
	public static void main(String[] args) throws InterruptedException{
		TimeMonitor monitor = new TimeMonitor();
		monitor.start();
		monitor.join();
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (runnable()) {
			check();
			try {
				sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		runEnd();
	}
	
	public void runEnd(){}
	
	public void check(){}
	
	protected boolean runnable(){
		return true;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString();
	}
	
}
