/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import jkamal.ddbmssim.db.Data;
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
		
	public void mine(Database db, Workload workload, SimulationMetricsLogger simulation_logger, String dir){
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
	}
	
	public int mining(Database db, Workload workload, SimulationMetricsLogger simulation_logger, String dir, boolean flag) {
		int tr_counts = 0;
		
		if(!flag)
			this.mine(db, workload, simulation_logger, dir);
		else{
			//Find the list of semi-FCI
			this.mine(db, workload, simulation_logger, dir);				
			System.out.println(this.learner);
		
			//Find the list of distributed semi-FCI
			ArrayList<List<Integer>> distributedSemiFCIList = new ArrayList<List<Integer>>();
			for(SemiFCI semiFCI : this.learner.getFCITable()){
				//System.out.println("@ "+fci.getItems());
				
				if(semiFCI.getItems().size() > 1){
					//System.out.println("@ "+fci.getItems());
					if(isDistributedFCI(db, semiFCI.getItems()))
						distributedSemiFCIList.add(semiFCI.getItems());				
				}
	        }
			
			System.out.println("@--> "+distributedSemiFCIList);
			
			//Find the transactions containing distributed semi-FCI
			int tr_frequent = 0;
			int tr_orange = 0;
			for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
				Set<Integer> infrequent = new TreeSet<Integer>();
				
				for(Transaction transaction : entry.getValue()) {
					if(!containsDistributedSemiFCI(transaction, distributedSemiFCIList)){
						if(!infrequent.contains(transaction.getTr_id()))
							infrequent.add(transaction.getTr_id());
					} else {
						++tr_frequent;
						
//						//Find the Orange transactions containing distributed semi-FCI
//						Iterator<Integer> data_iterator = transaction.getTr_dataSet().iterator();					
//						while(data_iterator.hasNext()) {
//							Data data = db.getData(data_iterator.next());
//							
//							for(int tr_id : workload.getWrl_dataInvolvedInTransactions().get(data.getData_id())) {
//								Transaction tr = workload.getTransaction(tr_id);
//								//System.out.println("-- "+tr_id);
//								if(tr.getTr_dtCost() > 0){
//									++tr_orange;
//									
//									if(infrequent.contains(tr.getTr_id()))
//										infrequent.remove(tr.getTr_id());
//								}
//							}
//						}
					}
				}								
				
				// Removes the infrequent transactions
				if(infrequent.size() > 0) {
					workload.removeTransactions(infrequent, entry.getKey(), true);
				}
			}
			
			System.out.println("[OUT] "+tr_frequent+" frequent transactions containing distributed semi-frequent closed tuples are identified.");
			//System.out.println("[OUT] "+tr_orange+" moveable "
		}
		
		return tr_counts;
	}
	
	// Returns true if a FCI is distributed between two or more physical servers
	private boolean isDistributedFCI(Database db, List<Integer> semiFCI){
		Data data;
		Set<Integer> nidSet = new TreeSet<Integer>();
		
		for(Integer t : semiFCI){
			data = db.getData(t);
			
			nidSet.add(data.getData_nodeId());
			if(nidSet.size() > 1)
				return true;
		}
				
		return false;
	} 
	
	// Returns true if a transaction contains a distributed FCI
	private boolean containsDistributedSemiFCI(Transaction transaction, ArrayList<List<Integer>> distributedSemiFCIList){
		for(List<Integer> dSemiFCI : distributedSemiFCIList){
			if(transaction.getTr_dataSet().containsAll(dSemiFCI))
				return true;
		} 
		
		return false;
	}
}