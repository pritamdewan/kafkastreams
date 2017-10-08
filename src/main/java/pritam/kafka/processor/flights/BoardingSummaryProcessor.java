package pritam.kafka.processor.flights;

import java.util.List;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class BoardingSummaryProcessor extends AbstractProcessor<String, FlightSummary>{
	
	private ProcessorContext context;
	private KeyValueStore<String, FlightSummary> summaryStore;
	
	public void process(String key, FlightSummary value){

		if(value.isFlightClosed()){
			System.out.println("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< BoardingSummaryProcessor.process  ->>>>>>>>  Flight is Closing to  Depart"  );
			List<String> paxMissing  = value.getMissingPax();
			for(int i=0; i < paxMissing.size();i++){
				System.out.println("Pax who did not do boarding :$$$$ " + paxMissing.get(i));
			}
		}
			
	}
	
	 public void init(ProcessorContext context) {
	        this.context = context;
	        System.out.println("BoardingSummaryProcessor.init");
	     
	    }
	 @Override
	    public void punctuate(long streamTime) {
	    	System.out.println("BoardingSummaryProcessor.punctuate");
	        
	    }
	 
	 

}
