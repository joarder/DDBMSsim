/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.main.DBMSSimulator;

public class TransactionGenerator {
	private Map<Double, Map<Integer, Integer>> normalised_cumulative_probability_to_data_map;
	
	public TransactionGenerator(){
		this.setNormalised_cumulative_probability_to_data_map(new TreeMap<Double, Map<Integer, Integer>>());
	}
	
	public Map<Double, Map<Integer, Integer>> getNormalised_cumulative_probability_to_data_map() {
		return this.normalised_cumulative_probability_to_data_map;
	}

	public void setNormalised_cumulative_probability_to_data_map(
			Map<Double, Map<Integer, Integer>> normalised_cumulative_probability_to_data_map) {
		this.normalised_cumulative_probability_to_data_map = normalised_cumulative_probability_to_data_map;
	}

	// Generates the required number of Transactions for a specific Workload with a specific Database
	public int generateTransaction(Database db, Workload workload, int global_tr_id) {				
		ArrayList<Transaction> transactionList;
		Transaction transaction;		
		Set<Integer> trDataSet;
		ArrayList<Integer> trDataList;		
		//Data data;
		Double rand = 0.0;
		int[] prop;
		int data_id = 0;
		int new_tr = 0;
		
		//Selecting Transaction Prop
		if(workload.getWrl_id() != 0)
			prop = workload.getWrl_transactionBirthProp();
		else
			prop = workload.getWrl_transactionProportions();
		
		// Reseed the Random Data Generator
		DBMSSimulator.randomDataGenerator.reSeed(0);		
		
		// i -- Transaction types
		for(int i = 0; i < workload.getWrl_transactionTypes(); i++) {	
			transactionList = new ArrayList<Transaction>();
			
			int typedTransactions = 0;
			// j -- a specific Transaction type in the Transaction proportion array
			for(int j = 0; j < prop[i]; j++) { //System.out.println(">> "+prop[i]);
				++global_tr_id;
				DBMSSimulator.incGlobal_tr_id();
				
				trDataSet = new TreeSet<Integer>();
				trDataList = new ArrayList<Integer>();
				
				// k -- required numbers of Data items based on Transaction type
				for(int k = 0; k < i+2; k++) {					
					rand = DBMSSimulator.randomDataGenerator.nextUniform(0.0, 1.0, true);
					//System.out.println("# rand = "+rand);
					data_id = db.getRandomData(rand);									
										
					if(trDataList.contains(data_id) && k > 0) {
						--k;
					} else {
						trDataList.add(data_id);						
												
						//data = db.search(data_id);							
						//data.getData_transactions_involved().add(global_tr_id);																														
						
						trDataSet.add(data_id);
						
						//System.out.println(data_id+" | P"+data.getData_partitionId()+" | "+data.getData_normalisedCumulativeZipfProbability()+" | rand = "+rand);
					}					
				} // end--k for() loop
																
				transaction = new Transaction(global_tr_id, trDataSet);				
				transaction.setTr_ranking(i+1);
				workload.incWrl_totalTransaction();
				++new_tr;
				
				if(workload.getWrl_transactionMap().containsKey(i)) {
					workload.getWrl_transactionMap().get(i).add(transaction);
					++typedTransactions;
				} else
					transactionList.add(transaction);							
			} // end--j for() loop
										
			if(workload.getWrl_id() == 0)
				workload.getWrl_transactionMap().put(i, transactionList);
			else 				
				workload.incWrl_transactionProportions(i, typedTransactions);									
		} // end--i for() loop
		
		return new_tr;
	}	
}