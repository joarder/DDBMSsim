/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import jkamal.ddbmssim.db.Database;
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
		this.learner = new IncMine();
		this.tr_serial = 0;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	public void init() {
		this.learner.minSupportOption.setValue(0.1d);
		this.learner.relaxationRateOption.setValue(0.5d);
		//this.learner.fixedSegmentLengthOption.setValue(1000);
		this.learner.windowSizeOption.setValue(10);
		this.learner.resetLearning();
	}
	
	public void mining(Database db, Workload workload, SimulationMetricsLogger simulation_logger, String dir) {
		// Create DSM file
		String file = Integer.toString(workload.getWrl_id())+"-"+db.getDb_name();
		workload.setMiner_writer(simulation_logger.getWriter(dir, file));
		
		int tr_count = simulation_logger.traceWorkload(db, workload, this.tr_serial, workload.getMiner_writer());
		int difference = tr_count - this.tr_serial;
		this.tr_serial += difference;
		
		workload.getMiner_writer().flush();
		workload.getMiner_writer().close();
		
		// DSM
		this.learner.fixedSegmentLengthOption.setValue(difference);
		this.stream = new ZakiFileStream(dir+"\\"+file+".txt");
		this.stream.prepareForUse();
		
		System.out.println("--> "+file+"|serial = "+this.tr_serial+"|count = "+tr_count+"|difference = "+difference);
        
		while(this.stream.hasMoreInstances()){
        	this.learner.trainOnInstance(this.stream.nextInstance());            
        }
	}
}