/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import jkamal.ddbmssim.db.DataPopularityProfile;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.main.DBMSSimulator;

public class WorkloadGenerator {	
	private Map<Integer, Workload> workload_map;
	private Map<Integer, Workload> sampled_workload_map;
	
	public WorkloadGenerator() {
		this.setWorkload_map(new TreeMap<Integer, Workload>());
		this.setSampled_workload_map(new TreeMap<Integer, Workload>());	
	}	
	
	public Map<Integer, Workload> getWorkload_map() {
		return workload_map;
	}

	public void setWorkload_map(Map<Integer, Workload> workload_map) {
		this.workload_map = workload_map;
	}
	
	public Map<Integer, Workload> getSampled_workload_map() {
		return sampled_workload_map;
	}

	public void setSampled_workload_map(Map<Integer, Workload> workload_map) {
		this.sampled_workload_map = workload_map;
	}
	
	// Workload Initialisation
	public Workload workloadInitialisation(Database db, String workload_name, int workload_id) {
		// Workload Details : http://oltpbenchmark.com/wiki/index.php?title=Workloads
		switch(workload_name) {
			case "tpcc":
				return(new Workload(workload_id, 5, db.getDb_id()));
			case "twitter":
				return(new Workload(workload_id, 5, db.getDb_id()));
			case "wiki":
				return(new Workload(workload_id, 5, db.getDb_id()));
			case "ycsb":
				return(new Workload(workload_id, 6, db.getDb_id()));
			}		
		
		return null;
	}
	
	// Generates Workloads for the entire simulation
	public void generateWorkloads(Database db, int simulation_run_numbers) throws IOException {
		DataPopularityProfile popularityProfile = new DataPopularityProfile();				
		Workload workload = null;
		int workload_id = 0;
		
		while(workload_id != simulation_run_numbers) {
			System.out.println("--------------------------------------------------------------------------");
			System.out.println("[ACT] Starting workload generation for simulation round "+workload_id+"...");
			if(workload_id != 0) {
				workload = new Workload(this.getWorkload_map().get(workload_id -1));
				workload.setWrl_id(workload_id);
				workload.setWrl_label("W"+workload_id);
				
				// create the empty db operation lists
				workload.setDb_insert(new HashMap<Integer, ArrayList<Integer>>());
				workload.setDb_delete(new HashMap<Integer, ArrayList<Integer>>());
				workload.setInsert_count(0);
				workload.setDelete_count(0);
				
				System.out.println("[MSG] Total "+workload.getWrl_totalTransactions()+" transactions are present in the workload.");
				
				// Setting Death Rate
				workload.setWrl_transactionDying((int) ((int) workload.getWrl_totalTransactions() * 0.5));				
				workload.setWrl_transactionDeathRate(0.5); // fixed rate

				// Setting Birth Rate
				workload.setWrl_transactionBorning((int) ((int) workload.getWrl_totalTransactions() * 0.5));
				workload.setWrl_transactionBirthRate(0.5); // fixed rate
				
				// === Death Management ===
				workload.setWrl_transactionDeathProp(transactionPropConvertor(DBMSSimulator.TPCC_TRANSACTION_PROPORTION,
						workload.getWrl_transactionDying()));
				
				// Reducing Old Workload Transactions			
				TransactionReducer transactionReducer = new TransactionReducer();
				int old_tr = transactionReducer.reduceTransaction(db, workload);
				
				System.out.println("[ACT] Varying current workload by reducing "+old_tr+" old transactions ...");
				this.print(workload);

				// === Birth Management ===
				workload.setWrl_transactionBirthProp(transactionPropConvertor(DBMSSimulator.TPCC_TRANSACTION_PROPORTION,
						workload.getWrl_transactionBorning()));
				
				// Generating New Workload Transactions						
				TransactionGenerator transactionGenerator = new TransactionGenerator();
				int new_tr = transactionGenerator.generateTransaction(db, workload, DBMSSimulator.getGlobal_tr_id());	
				
				System.out.println("[ACT] Varying current workload by generating "+new_tr+" new transactions ...");
			} else {
				popularityProfile.generateDataPopularity(db); // Only for Primary tables
				
				// === Workload Generation Round 0 ===
				workload = this.workloadInitialisation(db, DBMSSimulator.WORKLOAD_TYPE, workload_id);				
				workload.setWrl_initTotalTransactions(DBMSSimulator.TRANSACTIONS);
				workload.setWrl_transactionProp(transactionPropConvertor(DBMSSimulator.TPCC_TRANSACTION_PROPORTION, 
						DBMSSimulator.TRANSACTIONS));
				
				// Generating New Workload Transactions						
				TransactionGenerator transactionGenerator = new TransactionGenerator();
				transactionGenerator.generateTransaction(db, workload, DBMSSimulator.getGlobal_tr_id());
			}						
			
			db.updateDb_data_numbers();
			this.print(workload);
			
			System.out.println("[OUT] Initially "+workload.getWrl_totalTransactions()+" transactions have been " +
					"gathered for the target workload of simulation round "+workload_id);						
			
			workload.show(db, "");
			db.show();
			
			// Clone the Workload
			Workload cloned_workload = new Workload(workload);
			this.getWorkload_map().put(workload_id, cloned_workload);
			
			++workload_id;
		}
	}		
	
	// Following the transaction proportion from TPC-C benchmark model 
	private int[] transactionPropConvertor(double[] prop, int transactions) {
		int proportionArray[] = new int[prop.length];
		
		for(int i=0; i < proportionArray.length; i++)
			proportionArray[i] = (int) (prop[i] * transactions);
		
		return proportionArray;
	}	
	
	// Printing the Workload contents
	private void print(Workload workload) {
		System.out.print("[MSG] Total "+workload.getWrl_totalTransactions()+" transactions of "
				+workload.getWrl_transactionTypes()+" types having a distribution of ");
		
		workload.printWrl_transactionProp(workload.getWrl_transactionProportions());
		
		System.out.println(" are currently in the workload.");
	}	
}
