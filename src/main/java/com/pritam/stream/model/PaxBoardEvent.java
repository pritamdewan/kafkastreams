package com.pritam.stream.model;

public class PaxBoardEvent extends PaxEvent {
	boolean boarding = false;
	public boolean isBoarding() {
		return boarding;
	}
	public void setBoarding(boolean boarding) {
		this.boarding = boarding;
	}
	public void deBoard(boolean boarding){
		setBoarding(boarding);
	}
	
	public PaxBoardEvent(){
		super();
	}
	public PaxBoardEvent(String flightNumber, String pnr, String paxId,boolean isChecked){
		super(flightNumber,pnr,paxId);
		setBoarding(isChecked);
	}

}
