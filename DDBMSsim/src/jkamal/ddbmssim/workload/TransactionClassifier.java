/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;

public class TransactionClassifier {
	// red --> Distributed Transactions
	// orange --> Non-distributed Transactions with movable Data
	// green --> Non-distributed Transactions with non-movable Data

	public TransactionClassifier() {}

	public void classifyTransactions(Database db, Workload workload) {
		int orange_tr_data = 0;
		int green_tr_data = 0;
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				orange_tr_data = 0;
				green_tr_data = 0;
				
				if(transaction.getTr_dtCost() > 0)
					transaction.setTr_class("red");
				else {
					Iterator<Integer> data_iterator = transaction.getTr_dataSet().iterator();
					
					while(data_iterator.hasNext()) {
						Data data = db.search(data_iterator.next());
						int tr_counts = data.getData_transaction_involved().size();
						
						if(tr_counts <= 1) 
							++green_tr_data;
						else {							
							for(int tr_id : data.getData_transaction_involved()){
								Transaction tr = workload.getTransaction(tr_id);
								
								if(tr.getTr_dtCost() > 0)
									++orange_tr_data;
							} 
						}							
					}
					
					if(transaction.getTr_dataSet().size() == green_tr_data) 
						transaction.setTr_class("green");
					
					if(orange_tr_data > 0)
						transaction.setTr_class("orange");
				}				
			}
		}
	}
}