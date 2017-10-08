package pritam.kafka.processor.flights;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.pritam.stream.model.PaxBoardEvent;
import com.pritam.stream.model.PaxCheckInEvent;


public class FlightSummary implements Serializable{
	
	String flightNumber;
	private long lastUpdatedTime;
	int paxCount=0;
	boolean flightClosed = false;
	List<String> paxCheckedIn = new ArrayList<String>();
	List<String> paxBoarded = new ArrayList<String>();
	//List<String> missingPax = new ArrayList<String>();
	public FlightSummary(){
		paxCount  = 0;
	}
	
	
	public FlightSummary(String flightNumber,int cnt){
		this.flightNumber = flightNumber;
		paxCount  = cnt;
	}
	
	
	public boolean isFlightClosed() {
		return flightClosed;
	}

	

	public int getPaxCount() {
		return paxCount;
	}
	public String getFlightNumber() {
		return flightNumber;
	}
	
	
	public void addCheckedInPax(PaxCheckInEvent event){
		paxCheckedIn.add(event.getId());
	}

	public void addBoardedPax(PaxBoardEvent event){
		paxBoarded.add(event.getId());
	}
	public void update(String status){
		this.lastUpdatedTime = System.currentTimeMillis();
		if(status.equalsIgnoreCase("Close")){
		flightClosed = true;
		missingPax();
			
		}
		
		
	  }
	private void missingPax(){
		paxCheckedIn.removeAll(paxBoarded);
	}
	
	public List<String> getMissingPax(){
		return paxCheckedIn;
	}
	
  
	
}
