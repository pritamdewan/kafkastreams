package com.pritam.stream.model;

public class PaxCheckInEvent extends PaxEvent{

	public PaxCheckInEvent(){
		super();
	}
	
	public PaxCheckInEvent(String flightNumber, String pnr, String paxId,boolean isChecked){
		super(flightNumber,pnr,paxId);
		setCheckin(isChecked);
	}
	
	boolean checkin = false;
	public boolean isCheckin() {
		return checkin;
	}
	public void setCheckin(boolean checkin) {
		this.checkin = checkin;
	}
	
	
}
