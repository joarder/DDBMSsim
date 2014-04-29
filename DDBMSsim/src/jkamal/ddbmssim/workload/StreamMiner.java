/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.ArrayList;
import java.util.List;

import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.incmine.core.SemiFCI;
import jkamal.ddbmssim.incmine.learners.IncMine;
import jkamal.ddbmssim.incmine.streams.ZakiFileStream;
import jkamal.ddbmssim.io.SimulationMetricsLogger;

public class StreamMiner {	
	private int id;
	private IncMine learner;
	private ZakiFileStream stream;
	private int tr_serial;	

	public StreamMiner(int id) {
		this.setId(id);
		this.tr_serial = 0;
		this.learner = new IncMine();		
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public void init() {
		// Configure the learner
		this.learner.minSupportOption.setValue(0.1d);
		this.learner.relaxationRateOption.setValue(0.5d);
		this.learner.fixedSegmentLengthOption.setValue(1000); //difference		
		this.learner.windowSizeOption.setValue(10);
		this.learner.maxItemsetLengthOption.setValue(-1);
		this.learner.resetLearning();
	}
		
	public void mining(Database db, Workload workload, SimulationMetricsLogger simulation_logger, String dir) {
		// Generates the DSM file
		String file = Integer.toString(workload.getWrl_id())+"-"+db.getDb_name();
		workload.setMiner_writer(simulation_logger.getWriter(dir, file));
		
		int tr_count = simulation_logger.traceWorkload(db, workload, this.tr_serial, workload.getMiner_writer());		
		this.tr_serial += (tr_count - this.tr_serial);
		
		workload.getMiner_writer().flush();
		workload.getMiner_writer().close();

		// Read the stream input
		this.stream = new ZakiFileStream(dir+"\\"+file+".txt");
		this.stream.prepareForUse();				
        
		// Perform DSM
		while(this.stream.hasMoreInstances()){
        	this.learner.trainOnInstance(this.stream.nextInstance());            
        }
		
		System.out.println(this.learner);
		
		//for(Iterator this.learner.getFciTable().iterator()
		ArrayList<List<Integer>> d_fci = new ArrayList<List<Integer>>();
		for(SemiFCI fci : this.learner.getFciTable()){
			//System.out.println("@ "+fci.getItems());
			
			if(fci.getItems().size() > 1){
				//System.out.println("@ "+fci.getItems());
				if(isDistributed(db, fci.getItems()))
					d_fci.add(fci.getItems());
			}
        }				
	}
	
	private boolean isDistributed(Database db, List<Integer> fci){
		for(Integer t : fci){
			
		}
				
		return true;
	} 
}