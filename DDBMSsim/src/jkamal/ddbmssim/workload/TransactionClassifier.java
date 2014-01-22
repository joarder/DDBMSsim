/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;

public class TransactionClassifier {
	// red --> Distributed Transactions
	// orange --> Non-distributed Transactions with movable Data
	// green --> Non-distributed Transactions with non-movable Data
	
	private int red_tr;
	private int green_tr;
	private int orange_tr;
	private int red_tr_data;
	private int green_tr_data;
	private int orange_tr_data;
	
	public TransactionClassifier() {
		this.setRed_tr(0);
		this.setGreen_tr(0);
		this.setOrange_tr(0);
		this.setRed_tr_data(0);
		this.setGreen_tr_data(0);
		this.setOrange_tr_data(0);
	}

	public int getRed_tr() {
		return red_tr;
	}

	public void setRed_tr(int red_tr) {
		this.red_tr = red_tr;
	}

	public int getGreen_tr() {
		return green_tr;
	}

	public void setGreen_tr(int green_tr) {
		this.green_tr = green_tr;
	}

	public int getOrange_tr() {
		return orange_tr;
	}

	public void setOrange_tr(int orage_tr) {
		this.orange_tr = orage_tr;
	}

	public int getRed_tr_data() {
		return red_tr_data;
	}

	public void setRed_tr_data(int red_tr_data) {
		this.red_tr_data = red_tr_data;
	}

	public int getGreen_tr_data() {
		return green_tr_data;
	}

	public void setGreen_tr_data(int green_tr_data) {
		this.green_tr_data = green_tr_data;
	}

	public int getOrange_tr_data() {
		return orange_tr_data;
	}

	public void setOrange_tr_data(int orage_tr_data) {
		this.orange_tr_data = orage_tr_data;
	}

	private void incRed_tr() {
		int value = this.getRed_tr();
		this.setRed_tr(++value);
	}

	private void incGreen_tr() {
		int value = this.getGreen_tr();
		this.setGreen_tr(++value);
	}
	
	/*private void decGreen_tr() {
		int value = this.getGreen_tr();
		this.setGreen_tr(--value);
	}*/
	
	private void decGreen_tr(int value) {		
		this.setGreen_tr(--value);
	}

	private void incOrage_tr() {
		int value = this.getOrange_tr();
		this.setOrange_tr(++value);
	}

	/*private void incRed_tr_data() {
		int value = this.getRed_tr_data();
		this.setRed_tr_data(++value);
	}
	
	private void incGreen_tr_data() {
		int value = this.getGreen_tr_data();
		this.setGreen_tr_data(++value);
	}

	private void incOrange_tr_data() {
		int value = this.getOrange_tr_data();
		this.setOrange_tr_data(++value);
	}*/
	
	// Method to perform transaction classification for a particular database Workload
	public int classifyTransactions(Database db, Workload workload) {
		workload.setWrl_tr_red(0);
		workload.setWrl_tr_green(0);
		workload.setWrl_tr_orange(0);
		
		this.setRed_tr(0);
		this.setGreen_tr(0);
		this.setOrange_tr(0);
		this.setRed_tr_data(0);
		this.setGreen_tr_data(0);
		this.setOrange_tr_data(0);
		
		int orange_data = 0;
		int green_data = 0;
		//System.out.println(">> "+workload.getWrl_totalTransactions());

		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			Set<Integer> unique = new TreeSet<Integer>();
			
			for(Transaction transaction : entry.getValue()) {				
				orange_data = 0;
				green_data = 0;
				
				if(transaction.getTr_dtCost() > 0) {
					transaction.setTr_class("red");
					this.incRed_tr();
				} else {
					Iterator<Integer> data_iterator = transaction.getTr_dataSet().iterator();
					
					while(data_iterator.hasNext()) {
						Data data = db.search(data_iterator.next());
						int tr_counts = data.getData_transactions_involved().size();
						
						if(tr_counts <= 1) 
							++green_data;
						else {							
							for(int tr_id : data.getData_transactions_involved()){
								Transaction tr = workload.getTransaction(tr_id);
								
								if(tr.getTr_dtCost() > 0)
									++orange_data;
							} 
						}							
					}
					
					if(transaction.getTr_dataSet().size() == green_data) { 
						transaction.setTr_class("green");
						this.incGreen_tr();
						
						if(!unique.contains(transaction.getTr_id()))
							unique.add(transaction.getTr_id());
						
						//System.out.println("@ Added T"+transaction.getTr_id());
					}
					
					if(orange_data > 0) {
						transaction.setTr_class("orange");
						this.incOrage_tr();
					}
				}				
			}
			
			// Removing Green Transactions from the Workload
			if(unique.size() > 0) {
				workload.removeTransactions(db, entry.getValue(), unique, entry.getKey());	
				//this.decGreen_tr(unique.size());
				//unique.removeAll(unique);
			}
		}
		
		workload.setWrl_tr_red(this.getRed_tr());
		System.out.println("[OUT] Classified "+this.getRed_tr()+" transactions as RED !!!");
		workload.setWrl_tr_green(this.getGreen_tr());
		System.out.println("[OUT] Classified "+this.getGreen_tr()+" transactions as GREEN !!!");
		workload.setWrl_tr_orange(this.getOrange_tr());		
		System.out.println("[OUT] Classified "+this.getOrange_tr()+" transactions as ORANGE !!!");
		
		return (this.getRed_tr()+this.getOrange_tr());
	}
}