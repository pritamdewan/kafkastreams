package com.pritam.stream.model;

import java.util.ArrayList;
import java.util.List;

public class FlightEvent extends BaseEvent{
	
	String flightNumber;
	List<String> eventData = new ArrayList<String>();
	public FlightEvent(){
		
	}
	public FlightEvent(String flightnumber,String eventdata){
		super();
		this.flightNumber = flightnumber;
		this.eventData.add(eventdata);
	}
	public String getFlightNumber() {
		return flightNumber;
	}
	public void setFlightNumber(String flightNumber) {
		this.flightNumber = flightNumber;
	}
	public List<String> getEventData() {
		return eventData;
	}
	public void setEventData(String eventData) {
		this.eventData.add(eventData);
	}
	

}
