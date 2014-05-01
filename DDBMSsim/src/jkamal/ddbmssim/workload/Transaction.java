/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.Partition;

public class Transaction implements Comparable<Transaction> {
	private int tr_id;
	private String tr_label;	
	private int tr_type;
	private int tr_frequency;
	private int tr_temporal_weight;
	private int tr_dtCost; // Node Span Cost or, Distributed Transaction Cost
	private int tr_psCost; // Partition Span Cost
	private int tr_dtImpact;
	private Set<Integer> tr_dataSet;
	private String tr_class;
	
	public Transaction(int id, Set<Integer> dataSet) {
		this.setTr_id(id);
		this.setTr_label("T"+Integer.toString(this.getTr_id()));
		this.setTr_type(0);
		this.setTr_frequency(0);
		this.setTr_temporal_weight(100);
		this.setTr_dtCost(0);
		this.setTr_psCost(0);
		this.setTr_dtImpact(0);
		this.setTr_dataSet(dataSet);
		this.setTr_class(null);
	}
	
	// Copy Constructor
	public Transaction(Transaction transaction) {
		this.setTr_id(transaction.getTr_id());
		this.setTr_label(transaction.getTr_label());
		this.setTr_type(transaction.getTr_type());
		this.setTr_frequency(transaction.getTr_frequency());
		this.setTr_temporal_weight(transaction.getTr_temporal_weight());
		this.setTr_dtCost(transaction.getTr_dtCost());
		this.setTr_psCost(transaction.getTr_psCost());
		this.setTr_dtImpact(transaction.getTr_dtImpact());
		this.setTr_class(transaction.getTr_class());
		
		Set<Integer> cloneDataSet = new TreeSet<Integer>();
		for(Integer data_id : transaction.getTr_dataSet()) {
			cloneDataSet.add(data_id);
		}		
		this.setTr_dataSet(cloneDataSet);
	}

	public int getTr_id() {
		return tr_id;
	}

	public void setTr_id(int tr_id) {
		this.tr_id = tr_id;
	}
	
	public String getTr_label() {
		return tr_label;
	}
	
	public void setTr_label(String tr_label) {
		this.tr_label = tr_label;
	}

	public int getTr_type() {
		return tr_type;
	}

	public void setTr_type(int tr_ranking) {
		this.tr_type = tr_ranking;
	}

	public int getTr_frequency() {
		return tr_frequency;
	}

	public void setTr_frequency(int tr_frequency) {
		this.tr_frequency = tr_frequency;
	}

	public int getTr_temporal_weight() {
		return tr_temporal_weight;
	}

	public void setTr_temporal_weight(int tr_temporal_weight) {
		this.tr_temporal_weight = tr_temporal_weight;
	}

	public int getTr_dtCost() {
		return tr_dtCost;
	}

	public void setTr_dtCost(int tr_dtCost) {
		this.tr_dtCost = tr_dtCost;
	}

	public int getTr_psCost() {
		return tr_psCost;
	}

	public void setTr_psCost(int tr_psCost) {
		this.tr_psCost = tr_psCost;
	}
	
	public int getTr_dtImpact() {
		return tr_dtImpact;
	}

	public void setTr_dtImpact(int tr_dtImpact) {
		this.tr_dtImpact = tr_dtImpact;
	}

	public Set<Integer> getTr_dataSet() {
		return tr_dataSet;
	}

	public void setTr_dataSet(Set<Integer> tr_dataSet) {
		this.tr_dataSet = tr_dataSet;
	}	
	
	public String getTr_class() {
		return tr_class;
	}

	public void setTr_class(String tr_class) {
		this.tr_class = tr_class;
	}

	public void incTr_frequency() {
		int tr_frequency = this.getTr_frequency();
		this.setTr_frequency(++tr_frequency);
	}
	
	public void decTr_temporalWeight() {
		int tr_temporal_weight = this.getTr_temporal_weight();
		this.setTr_temporal_weight(--tr_temporal_weight);
	}
	
	// This function will calculate the Node and Partition Span Cost for the representative Transaction
	public void calculateDTCost(Database db) {
		int pid = -1;
		Partition partition;
		Set<Integer> nsCost = new TreeSet<Integer>();
		Set<Integer> dataSet = this.getTr_dataSet();
		Data data;		
	
		// Calculate Node Span Cost which is equivalent to the Distributed Transaction Cost
		Iterator<Integer> ns = dataSet.iterator();
		while(ns.hasNext()) {
			data = db.getData(ns.next());
			
			if(data.isData_isRoaming())
				pid = data.getData_globalPartitionId();				
			else 
				pid = data.getData_homePartitionId();
				
			partition = db.getPartition(pid);
			nsCost.add(partition.getPartition_nodeId());
		}
		
		this.setTr_dtCost(nsCost.size()-1);
		
		// Calculate Partition Span Cost
		pid = -1;
		Set<Integer> psCost = new TreeSet<Integer>();
	
		Iterator<Integer> ps = dataSet.iterator();
		while(ps.hasNext()) {
			data = db.getData(ps.next());
			if(data.isData_isRoaming())
				pid = data.getData_globalPartitionId();
			else 
				pid = data.getData_homePartitionId();				
			
			psCost.add(pid);
		}
	
		this.setTr_psCost(psCost.size()-1);		
	}
	
	// Calculate DT Impacts for the Workload
	public void calculateDTImapct() {
		this.setTr_dtImpact(this.getTr_dtCost() * this.getTr_frequency() * this.getTr_temporal_weight());
	}	
	
	// Given a Data Id this function returns the corresponding Data from the Transaction
	public Data lookup(Database db, int id) {		
		for(Integer data_id : this.tr_dataSet) {
			Data data = db.getData(data_id);
					
			if(data.getData_id() == id)
				return data;
		}
		
		return null;
	}
	
	// Prints out all of the contents of the representative Transaction
	public void show(Database db) {
		System.out.print(this.toString());
		
		System.out.print("{");
		Iterator<Integer> data =  this.getTr_dataSet().iterator();		
		while(data.hasNext()) {
			System.out.print(db.getData(data.next()).toString());
			if(data.hasNext())
				System.out.print(", ");
		}				
		
		System.out.println("}");		
	}
	
	@Override
	public String toString() {	
		return (this.getTr_label()+"("
				+this.getTr_dtCost()+"|"
				+this.getTr_frequency()+"|"
				+this.getTr_dataSet().size()+")");
	}

	@Override
	public int compareTo(Transaction transaction) {
		int compare = ((int)this.tr_id < (int)transaction.tr_id) ? -1: ((int)this.tr_id > (int)transaction.tr_id) ? 1:0;
		return compare;
	}
}