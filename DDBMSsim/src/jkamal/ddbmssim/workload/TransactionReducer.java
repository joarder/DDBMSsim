/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.main.DBMSSimulator;

public class TransactionReducer {
	public TransactionReducer() {}
	
	// This function will reduce the required number of Transactions for a specific Workload with a specific Database
	public void reduceTransaction(Database db, Workload workload) {
		ArrayList<Transaction> transactionList = null;
		Set<Integer> random_transactions = null;
		int size = 0;
		
		verifyTransactionDeathProportions(workload);
		
		// i -- Transaction types
		for(int i = 0; i < workload.getWrl_transactionTypes(); i++) {
			size = workload.getWrl_transactionDeathProportions()[i];			
			
			//System.out.println("@ "+size+" >> i = "+i);
			
			if(size != 0) {
				transactionList = workload.getWrl_transactionMap().get(i);
				random_transactions = this.randomTransactions(size);
				
				//System.out.println("* "+random_transactions.size());
				
				for(int j : random_transactions) {
					//System.out.println("> "+j+" T"+transactionList.get(j).getTr_id());
					this.releaseData(db, transactionList.get(j));
					transactionList.remove(j); // removing index
					
					workload.decWrl_totalTransactions();				
					workload.decWrl_transactionProportions(i);
				}
			}		
			//System.out.println("@debug >> total TR = "+workload.getWrl_totalTransaction());
		} // end -- i		
	}
	
	public void releaseData(Database db, Transaction transaction) {
		for(Integer data_id : transaction.getTr_dataSet()) {
			Data data = db.search(data_id);			
			data.getData_transaction_involved().remove((Object)transaction.getTr_id()); // removing object
			
			//Data dbData = db.search(data.getData_id());
			//dbData.getData_transaction_involved().remove((Object)transaction.getTr_id()); // removing object
		}
	}
	
	// Randomly selects Transactions for deletion
	public Set<Integer> randomTransactions(int nums) {
		Set<Integer> random_transactions = new TreeSet<Integer>();
		DBMSSimulator.random_data.reSeed(0);
		
		for(int i = 0; i < nums; i++)
			random_transactions.add((int) DBMSSimulator.random_data.nextUniform(0, nums));
		
		return random_transactions;
	}
	
	public void verifyTransactionDeathProportions(Workload workload) {
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