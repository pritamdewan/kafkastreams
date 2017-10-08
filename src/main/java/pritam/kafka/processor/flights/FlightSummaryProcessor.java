package pritam.kafka.processor.flights;


import java.util.Objects;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import com.pritam.stream.model.FlightEvent;
import com.pritam.stream.model.PaxBoardEvent;
import com.pritam.stream.model.PaxCheckInEvent;

public class FlightSummaryProcessor extends AbstractProcessor<Integer, Object> {

	private KeyValueStore<String, FlightSummary> summaryStore;
    private ProcessorContext context;
    FlightSummary flightSummary;
	public FlightSummaryProcessor(){
		System.out.println("FlightSummaryProcessor.FlightSummaryProcessor()");
			
	}
	
	public void process(Integer key, Object value) {
		// TODO Auto-generated method stub
		
        if (key == 501 ) {
        	
        	//Flight Open & Close states
        	FlightEvent fe = (FlightEvent)value;
        	//System.out.println("FlightSummaryProcessor.process  -> Flight "  + fe.getEventData());
        	String flightStatus = fe.getEventData().get(0);
        	String flightNumber = fe.getFlightNumber();
        	if(flightStatus.equalsIgnoreCase("Open")){
        	
        		flightSummary = summaryStore.get("flight-movement");
        		//erase old processed collection
        		flightSummary = null;
        		flightSummary = new FlightSummary(flightNumber,300);
        		summaryStore.put(flightNumber, flightSummary);
        		System.out.println("Flight Opened........................>");
        	}else if(flightStatus.equalsIgnoreCase("Close")){
        		try{
        			flightSummary.update("Close");
        			summaryStore.put(flightNumber, flightSummary);
        			context.forward(flightNumber, flightSummary);
        			this.context.commit();			
        			System.out.println("Flight Closed...............Forwarding to Boarding Summary Processor to find pax who did not board!>");
        		}catch(Exception ex){
        			ex.printStackTrace();
        		}
        	}
        	
        	
        } else {
        	if(value instanceof PaxCheckInEvent){
        		String flightNumber = ((PaxCheckInEvent) value).getFlightNumber();
        		FlightSummary flightSummary = summaryStore.get(flightNumber);
        		flightSummary.addCheckedInPax((PaxCheckInEvent) value);
        		summaryStore.put(flightNumber, flightSummary);
        		System.out.println("Pax Checked in :  " + ((PaxCheckInEvent)value).getId());
        	}else if (value instanceof PaxBoardEvent){
        		String flightNumber = ((PaxBoardEvent) value).getFlightNumber();
        		FlightSummary flightSummary = summaryStore.get(flightNumber);
        		flightSummary.addBoardedPax((PaxBoardEvent) value);
        		summaryStore.put(flightNumber, flightSummary);
        		
        		System.out.println("Pax boarded :  " + ((PaxBoardEvent)value).getId());
        	}else{
        		//do nothing
        	}
        	//System.out.println("FlightSummaryProcessor.process  -> key" + key + " value : " +  pe.getPnr());
    		//context.forward(pe.getFlightNumber(), flightSummary);
    		this.context.commit();
        }
        
        
		
	}
	@Override
    
    public void init(ProcessorContext context) {
        this.context = context;
        summaryStore = (KeyValueStore<String, FlightSummary>) this.context.getStateStore("flight-movement");
        
        Objects.requireNonNull(summaryStore, "State store can't be null");
        
        System.out.println("FlightSummaryProcessor.init");
     
    }


    @Override
    public void punctuate(long streamTime) {
    	
    	System.out.println("FlightSummaryProcessor.punctuate");
        
    }
    public void close(){
    	System.out.println("FlightSummaryProcessor.close");
    }
	
	
}
