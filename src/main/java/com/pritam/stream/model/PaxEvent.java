package com.pritam.stream.model;

public class PaxEvent extends BaseEvent {

	String id;
	String flightNumber;

	String pnr;

	public PaxEvent() {
		super();
	}

	public String getPnr() {
		return pnr;
	}

	public void setPnr(String pnr) {
		this.pnr = pnr;
	}

	public String getId() {
		return id;
	}

	public void setId(String paxId) {
		this.id = paxId;
	}

	public String getFlightNumber() {
		return flightNumber;
	}

	public void setFlightNumber(String flightNumber) {
		this.flightNumber = flightNumber;
	}

	public PaxEvent(String flightNumber, String pnr, String paxId) {
		super();
		this.flightNumber = flightNumber;
		this.pnr = pnr;
		this.id = paxId;
	}

	public String toString() {
		String to = getFlightNumber() + " : " + getPnr() + ':' + getId();
		return to;
	}

}
