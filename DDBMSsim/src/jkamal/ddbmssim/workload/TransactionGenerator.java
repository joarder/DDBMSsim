/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.DataPopularityProfile;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.Partition;
import jkamal.ddbmssim.db.Table;
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
		Set<Integer> trDataSet = null;
		int[] prop;
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
				// Gather Data objects from a Transaction
				trDataSet = this.getTransactionalDataSet(db, i, workload);				
				// Create a new Transaction												
				transaction = new Transaction(global_tr_id, trDataSet);				
				transaction.setTr_ranking(i+1);
				workload.incWrl_totalTransaction();
				++new_tr;
				
				System.out.println(">> T"+global_tr_id+"|"+trDataSet.size());
				
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
		
		db.getDb_dbs().updateNodeLoad();
		return new_tr;
	}	
	
	// Create a data set for a specific transaction of type i
	private Set<Integer> getTransactionalDataSet(Database db, int i, Workload workload) {
		//DataPopularityProfile popularityProfile = new DataPopularityProfile();
		Set<Integer> trDataSet = new TreeSet<Integer>();
		ArrayList<Integer> trDataList = new ArrayList<Integer>();
		ArrayList<Integer> keyList;
		int data_id = 0;
		int _w_rank, _i_rank = 0;
		int _w = 0, _i = 0, _d = 0, _s = 0, _c = 0, _h, _o = 0, _no, _ol = 0;
		
		for(Table table : db.getDb_tables()) {			
			int data_nums = DBMSSimulator.TPCC_TRANSACTION_DATA_DIST[i][table.getTbl_id()-1];
			int action = DBMSSimulator.TPCC_TRANSACTIONAL_CHANGE[i][table.getTbl_id()-1];
			
			System.out.println("\t<"+table.getTbl_name()+">| data = "+data_nums+"| action = "+action);//+"|min = "+table.getTbl_min_cp()+"|max = "+table.getTbl_max_cp());
			
			switch(action) {
			case 0:						
					if(table.getTbl_partitions().size() == 1 && data_nums != 0) { // e.g. for <Warehouse> table; W=1
						for(Partition partition : table.getTbl_partitions()) {
							for(Data data : partition.getPartition_dataSet()) {
								trDataSet.add(data.getData_id());
								
								//System.out.println("\t\t* d"+data.getData_id());
							}
						}
					} else {
						for(int d = 0; d < data_nums; d++) {
							//================new
							switch(table.getTbl_name()) {
								case "Warehouse":
									_w_rank = DBMSSimulator.randomDataGenerator.nextZipf(table.getTbl_data_count(), 2.0);
									data_id = table.getTableData(_w_rank);
									_w = data_id;
									System.out.println("\t\t--> W("+_w+")");
									break;
									
								case "Item":									
									_i_rank = DBMSSimulator.randomDataGenerator.nextZipf(table.getTbl_data_count(), 2.5);
									data_id = table.getTableData(_i_rank);
									_i = data_id;
									System.out.println("\t\t--> I("+_i+")");
									break;
									
								case "District":
									keyList = new ArrayList<Integer>();
									keyList.add(_w);
									data_id = table.getTableData(keyList, 1);
									_d = data_id;
									System.out.println("\t\t--> D("+_d+") for W("+_w+")");
									break;
									
								case "Stock":
									keyList = new ArrayList<Integer>();
									keyList.add(_w);
									keyList.add(_i);
									data_id = table.getTableData(keyList, 1);
									_s = data_id;
									System.out.println("\t\t--> S("+_s+") for W("+_w+") and I("+_i+")");
									break;
									
								case "Customer": // District Table
									keyList = new ArrayList<Integer>();
									keyList.add(_d);
									data_id = table.getTableData(keyList, 1);
									_c = data_id;
									System.out.println("\t\t--> C("+_c+") for D("+_d+")");
									break;
									
								case "History": // Customer Table
									// Nothing to do
									break;
									
								case "Orders":
									/*keyList = new ArrayList<Integer>();
									keyList.add(_c);
									data_id = table.getTableData(keyList, 1);
									_o = data_id;
									System.out.println("\t\t--> O("+_o+") for C("+_c+")");
									*/break;
									
								case "New-Order": // Customer Table (the last 1/3 values from the Order table)					
									// Nothing to do
									break;
									
								case "Order-Line": // Stock Table (10 most popular values from the Stock table)
									/*keyList = new ArrayList<Integer>();
									keyList.add(_s);
									keyList.add(_o);
									data_id = table.getTableData(keyList, 1);
									_ol = data_id;
									System.out.println("\t\t--> OL("+_ol+") for S("+_s+") and O("+_o+")");
									*/break;
							}
							
							
							if(trDataList.contains(data_id) && d > 0) {
								--d;
								//System.out.println("@ *d"+data_id);
							} else {
								trDataList.add(data_id);
								trDataSet.add(data_id);
																
								//System.out.println("\t\t# rand = "+rand+"|min = "+table.getTbl_min_cp()+"|max = "+table.getTbl_max_cp());
								//System.out.println("\t\t@ d"+data_id);
								//Data x = db.search(data_id);								
								/*System.out.println("\t\t@-- "+x.getData_id()+" | "
										+x.getData_zipfProbability()+" | "
										+x.getData_cumulativeZipfProbability()+" | "
										+x.getData_normalisedCumulativeZipfProbability());*/								
							}
						}
					}
					break;
			
			case 1:					
					data_id = db.getDb_data_numbers()+1;
					
					// Generate Primary and Foreign Keys
					ArrayList<Integer> linkSet = db.getLinkedTables(table);					
					
					// Generate Partition Id
					int target_partition = data_id % db.getDb_dbs().getDbs_nodes().size();
					Partition partition = table.getPartition(target_partition+1);
					
					// Create a new Data Row Object
					Data data = new Data(table, data_id, partition.getPartition_id(), partition.getPartition_globalId(), partition.getPartition_nodeId(), false);				
					data.setData_pk(partition.getPartition_table_id());
					data.setData_size(DBMSSimulator.TPCC_DATA_ROW_SIZE[partition.getPartition_table_id()-1]);
					
					// Put an entry into the Partition Data lookup table and add in the Data object into the Partition Data Set
					partition.getPartition_dataLookupTable().put(data.getData_id(), partition.getPartition_globalId());
					partition.getPartition_dataSet().add(data);
					partition.updatePartitionLoad();
					
					// Increment Data counts at Node and Database level
					db.getDb_dbs().getDbs_node(partition.getPartition_nodeId()).incNode_totalData();					
					db.setDb_data_numbers(data_id);
					
					trDataSet.add(data_id);
					
					//System.out.println("@ Inserting d"+data_id+" into "+partition.getPartition_label());
					break;
					
			case -1:
					double rand = DBMSSimulator.randomDataGenerator.nextUniform(0, 1, false);				
					data_id = db.getRandomData(rand, table);
					
					Data _data = db.search(data_id);
					Partition _partition = table.getPartition(_data.getData_localPartitionId());
					
					// Remove the entry from the Partition Data lookup table and remove the Data object from the Partition Data Set
					_partition.getPartition_dataLookupTable().put(_data.getData_id(), _partition.getPartition_globalId());
					_partition.getPartition_dataSet().remove(_data);
					_partition.updatePartitionLoad();						
					
					// Remove the data id from the workload transactions
					workload.removeDataFromTransactions(data_id, workload.getTransactionListForSearchedData(data_id));
					
					// Decrement Data counts at Node and Database level
					db.getDb_dbs().getDbs_node(_partition.getPartition_nodeId()).decNode_totalData();
					int data_counts = db.getDb_data_numbers();
					db.setDb_data_numbers(--data_counts);
										
					//System.out.println("@ Deleting d"+data_id+" from "+_partition.getPartition_label());
					break;
			}
			
			table.updateTableLoad();
		}
		
		return trDataSet;
	}
}