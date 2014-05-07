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
		this.learner.minSupportOption.setValue(0.05d);
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
	
	public int mining(Database db, Workload workload, SimulationMetricsLogger simulation_logger, String dir) {
		//Find the list of semi-FCI
		this.mine(db, workload, simulation_logger, dir);		
		//System.out.println(this.learner);
	
		//Find the list of distributed semi-FCI
		ArrayList<List<Integer>> semiFCIList = new ArrayList<List<Integer>>();
		ArrayList<List<Integer>> distributedSemiFCIList = new ArrayList<List<Integer>>();
		for(SemiFCI semiFCI : this.learner.getFCITable()){
			//System.out.println("@ "+fci.getItems());
			
			if(semiFCI.getItems().size() > 1){
				//System.out.println("@ "+fci.getItems());
				semiFCIList.add(semiFCI.getItems());
				
				if(isDistributedFCI(db, semiFCI.getItems()))
					distributedSemiFCIList.add(semiFCI.getItems());
			}
        }
		
		System.out.println("[OUT] Total "+distributedSemiFCIList.size()+" distributed semi-frequent closed data tuple sets have been identified.");
		
		//Find the transactions containing distributed semi-FCI
		int orange_data = 0, green_data = 0;
		int tr_red = 0, tr_orange = 0, tr_green = 0;
		int tr_id = 0;
		Set<Integer> frequent_dsfci = new TreeSet<Integer>();
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			Set<Integer> toBeRemoved = new TreeSet<Integer>();
			
			for(Transaction transaction : entry.getValue()) {
				orange_data = 0;
				green_data = 0;
				
				tr_id = transaction.getTr_id();
				
				// Infrequent Transactions
				if(!isFrequent(transaction, semiFCIList)){
					if(!toBeRemoved.contains(tr_id))
						toBeRemoved.add(tr_id);						
				}//end-if
				// Frequent Transactions
				else{
					// Distributed Transactions
					if(transaction.getTr_dtCost() > 0){
						// Distributed Transactions containing Distributed Semi-FCI
						if(containsDistributedSemiFCI(transaction, distributedSemiFCIList)){
							++tr_red;
							transaction.setTr_class("red");
							frequent_dsfci.add(transaction.getTr_id());
						}//end-if
						// Distributed Transactions containing Non-Distributed Semi-FCI
						else{
							if(!toBeRemoved.contains(tr_id))
								toBeRemoved.add(tr_id);
						}//end-else
					}//end-if
					// Non-Distributed Transactions
					else{
						// Evaluating Movable and Non-Movable Transactions
						Iterator<Integer> data_iterator = transaction.getTr_dataSet().iterator();
						while(data_iterator.hasNext()) {
							Data data = db.getData(data_iterator.next());
							int tr_counts = workload.getWrl_dataInvolvedInTransactions().get(data.getData_id()).size();
							if(tr_counts <= 1) 
								++green_data;
							else {							
								for(int tid : workload.getWrl_dataInvolvedInTransactions().get(data.getData_id())) {
									Transaction tr = workload.getTransaction(tid);
									if(tr.getTr_dtCost() > 0){
										if(frequent_dsfci.contains(tr.getTr_id()))
											++orange_data;
										else{
											if(!toBeRemoved.contains(transaction.getTr_id()))
												toBeRemoved.add(transaction.getTr_id());
										}//end-else												
									}//end-if
								}//end-for() 
							}//end-else
						}//end-while()
						
						if(transaction.getTr_dataSet().size() == green_data) { 
							transaction.setTr_class("green");
							++tr_green;
							
							if(!toBeRemoved.contains(transaction.getTr_id()))
								toBeRemoved.add(transaction.getTr_id());
						}
						
						if(orange_data > 0) {
							transaction.setTr_class("orange");
							++tr_orange;
						}
					}//end-else
				}//end-else
			}//end-for()
			
			// Removing the selected Transactions from the Workload
			if(toBeRemoved.size() > 0)
				workload.removeTransactions(toBeRemoved, entry.getKey(), true);					
		}//end-for()
		
		System.out.println("[OUT] Classified "+tr_red+" transactions as RED !!!");
		workload.setWrl_tr_green(tr_green);
		System.out.println("[OUT] Classified "+tr_green+" transactions as GREEN !!!");
		workload.setWrl_tr_orange(tr_orange);		
		System.out.println("[OUT] Classified "+tr_orange+" transactions as ORANGE !!!");
		
		workload.calculateDTandDTI(db);
		
		return (tr_red + tr_orange);
	}
	
	public int mining1(Database db, Workload workload, SimulationMetricsLogger simulation_logger, String dir) {
		//Find the list of semi-FCI
		this.mine(db, workload, simulation_logger, dir);		
		//System.out.println(this.learner);
	
		//Find the list of semi-FCI
		ArrayList<List<Integer>> semiFCIList = new ArrayList<List<Integer>>();
		//ArrayList<List<Integer>> distributedSemiFCIList = new ArrayList<List<Integer>>();
		for(SemiFCI semiFCI : this.learner.getFCITable()){
			//System.out.println("@ "+fci.getItems());
			
			if(semiFCI.getItems().size() > 1){
				//System.out.println("@ "+fci.getItems());
				semiFCIList.add(semiFCI.getItems());
				
				//if(isDistributedFCI(db, semiFCI.getItems()))
					//distributedSemiFCIList.add(semiFCI.getItems());
			}
        }
		
		System.out.println("[OUT] Total "+semiFCIList.size()+" semi-frequent closed data tuple sets have been identified.");
		
		//Find the transactions containing semi-FCI
		int orange_data = 0, green_data = 0;
		int tr_red = 0, tr_orange = 0, tr_green = 0;
		int tr_id = 0;
		Set<Integer> frequent_sfci = new TreeSet<Integer>();
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			Set<Integer> toBeRemoved = new TreeSet<Integer>();
			
			for(Transaction transaction : entry.getValue()) {
				orange_data = 0;
				green_data = 0;
				
				tr_id = transaction.getTr_id();
				
				// Infrequent Transactions
				if(!isFrequent(transaction, semiFCIList)){
					if(!toBeRemoved.contains(tr_id))
						toBeRemoved.add(tr_id);						
				}//end-if
				// Frequent Transactions
				else{
					// Distributed Transactions
					if(transaction.getTr_dtCost() > 0){
						// Distributed Transactions containing Distributed Semi-FCI
						//if(containsDistributedSemiFCI(transaction, distributedSemiFCIList)){
							++tr_red;
							transaction.setTr_class("red");
							frequent_sfci.add(transaction.getTr_id());
						//}//end-if
						// Distributed Transactions containing Non-Distributed Semi-FCI
						//else{
							//if(!toBeRemoved.contains(tr_id))
								//toBeRemoved.add(tr_id);
						//}//end-else
					}//end-if
					// Non-Distributed Transactions
					else{
						// Evaluating Movable and Non-Movable Transactions
						Iterator<Integer> data_iterator = transaction.getTr_dataSet().iterator();
						while(data_iterator.hasNext()) {
							Data data = db.getData(data_iterator.next());
							int tr_counts = workload.getWrl_dataInvolvedInTransactions().get(data.getData_id()).size();
							if(tr_counts <= 1) 
								++green_data;
							else {							
								for(int tid : workload.getWrl_dataInvolvedInTransactions().get(data.getData_id())) {
									Transaction tr = workload.getTransaction(tid);
									if(tr.getTr_dtCost() > 0){
										if(frequent_sfci.contains(tr.getTr_id()))
											++orange_data;
										else{
											if(!toBeRemoved.contains(transaction.getTr_id()))
												toBeRemoved.add(transaction.getTr_id());
										}//end-else												
									}//end-if
								}//end-for() 
							}//end-else
						}//end-while()
						
						if(transaction.getTr_dataSet().size() == green_data) { 
							transaction.setTr_class("green");
							++tr_green;
							
							if(!toBeRemoved.contains(transaction.getTr_id()))
								toBeRemoved.add(transaction.getTr_id());
						}
						
						if(orange_data > 0) {
							transaction.setTr_class("orange");
							++tr_orange;
						}
					}//end-else
				}//end-else
			}//end-for()
			
			// Removing the selected Transactions from the Workload
			if(toBeRemoved.size() > 0)
				workload.removeTransactions(toBeRemoved, entry.getKey(), true);					
		}//end-for()
		
		System.out.println("[OUT] Classified "+tr_red+" transactions as RED !!!");
		workload.setWrl_tr_green(tr_green);
		System.out.println("[OUT] Classified "+tr_green+" transactions as GREEN !!!");
		workload.setWrl_tr_orange(tr_orange);		
		System.out.println("[OUT] Classified "+tr_orange+" transactions as ORANGE !!!");
		
		workload.calculateDTandDTI(db);
		
		return (tr_red + tr_orange);
	}
	
	// Returns true if a transaction contains any of the mined semi-FCI
	private boolean isFrequent(Transaction transaction, ArrayList<List<Integer>> semiFCIList){
		for(List<Integer> semiFCI : semiFCIList){
			if(transaction.getTr_dataSet().containsAll(semiFCI))
				return true;
		}
		
		return false;
	}
	
	// Returns true if a transaction contains any of the mined distributed semi-FCI
	private boolean containsDistributedSemiFCI(Transaction transaction, ArrayList<List<Integer>> distributedSemiFCIList){
		for(List<Integer> dSemiFCI : distributedSemiFCIList){
			if(transaction.getTr_dataSet().containsAll(dSemiFCI))
				return true;
		} 
		
		return false;
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
}