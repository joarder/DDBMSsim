/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import jkamal.ddbmssim.db.Database;

public class TransactionReducer {
	public TransactionReducer() {}
	
	// This function will reduce the required number of Transactions for a specific Workload with a specific Database
	public int reduceTransaction(Database db, Workload workload) {	
		int old_tr = 0;
		verifyTransactionDeathProportions(workload);
		
		//int transaction_id_tracker = 1;		 
		// i -- Transaction types
		for(int i = 0; i < workload.getWrl_transactionTypes(); i++) {
			int tr_nums = workload.getWrl_transactionProportions()[i];
			int dying_tr_nums = workload.getWrl_transactionDeathProportions()[i];
						
			int[] tr_array = new int[tr_nums];
			//System.out.print("\n");
			for(int j = 0; j < tr_nums; j++) {
				tr_array[j] = workload.getWrl_transactionMap().get(i).get(j).getTr_id();
				//System.out.print(tr_array[j]+", ");
			}
			//System.out.print("\n");
			
			//System.out.println("@ "+dying_tr_nums+" dying from "+tr_nums+" >> i = "+i);
			
			Set<Integer> unique = new TreeSet<Integer>();			
			for(int k = 0; k < dying_tr_nums; k++) {
				int val = this.getRandom(tr_array);
				
				if(!unique.contains(val)) {
					unique.add(val);
					//System.out.println("@ Random pick up val = "+val);
					//System.out.print(val+", ");
				} else {
					--k;
				}
			}
			//System.out.print("\n");
			
			if(tr_nums != 0)				
				workload.removeTransactions(db, unique, i, false);
			
			old_tr += dying_tr_nums; 
			//System.out.println("@debug >> total TR = "+workload.getWrl_totalTransaction());
		} // end -- i
		
		return old_tr;
	}
	
	private int getRandom(int[] array) {
	    int rnd = new Random().nextInt(array.length);
	    return array[rnd];
	}
	
	// Randomly selects Transactions for deletion
	/*private Set<Integer> getRandomTransactions(int dying_tr_nums, int tr_nums, int tr_id_tracker, Workload workload) {
		Set<Integer> random_transactions = new TreeSet<Integer>();
		DBMSSimulator.random_data.reSeed(0);
		
		for(int i = 0; i < dying_tr_nums; i++) {
			int tr_id = (int) DBMSSimulator.random_data.nextUniform(1, tr_nums);
			tr_id += tr_id_tracker;
			
			//System.out.println("@debug >> Randomly picked up T"+tr_id);
			
			if(random_transactions.contains(tr_id) || workload.search(tr_id) == null) {
				//System.out.println("@debug >> Choosing another random transaction ...");
				--i;
			} else {
				random_transactions.add(tr_id);
				System.out.println("@ Removing "+tr_id);
			}
		}
		
		return random_transactions;
	}*/
	
	private void verifyTransactionDeathProportions(Workload workload) {
		int difference = 0;
		int transactionProportions[] = workload.getWrl_transactionProportions();
		int deathProportions[] = workload.getWrl_transactionDeathProportions();
		
		for(int i = 0; i < transactionProportions.length; i++) {
			if(deathProportions[i] > transactionProportions[i]) {
				difference = deathProportions[i] - transactionProportions[i];
				workload.getWrl_transactionDeathProportions()[i] -= difference;
				
				workload.setWrl_transactionDying(workload.getWrl_transactionDying() - difference);
				
				System.out.print("[DBG] Killing "+workload.getWrl_transactionDying()+" old transactions with a distribution of ");				
				workload.printWrl_transactionProp(workload.getWrl_transactionDeathProportions());
				System.out.println();
			}
		}
	}
}