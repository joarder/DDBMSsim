/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.Partition;
import jkamal.ddbmssim.db.Table;
import jkamal.ddbmssim.main.DBMSSimulator;

public class TransactionGenerator {	
	private HashMap<Integer, ArrayList<Integer>> _cache;
	private ArrayList<Integer> _district_cache;
	
	public TransactionGenerator() {
		this._cache = new HashMap<Integer, ArrayList<Integer>>();
		this._district_cache = new ArrayList<Integer>();
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
			for(int j = 0; j < prop[i]; j++) { ////System.out.println(">> "+prop[i]);
				++global_tr_id;
				DBMSSimulator.incGlobal_tr_id();
				// Gather Data objects from a Transaction
				trDataSet = this.getTransactionalDataSet(db, i, workload);				
				// Create a new Transaction												
				transaction = new Transaction(global_tr_id, trDataSet);				
				transaction.setTr_ranking(i+1);
				workload.incWrl_totalTransaction();
				++new_tr;
				
				//System.out.println(">> T"+global_tr_id+"|"+trDataSet.size()+"|i("+i+")");
				
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
		ArrayList<Integer> dataList = null;
		ArrayList<Integer> _cache_items = null;
		int data_id = -1;
		int _w_rank, _i_rank = 0;
		int _w = 0, _i = 0, _d = 0, _s = 0, _c = 0, _o = 0, _no = 0, _ol = 0, index = 0;
		
		for(Table table : db.getDb_tables()) {			
			int data_nums = DBMSSimulator.TPCC_TRANSACTION_DATA_DIST[i][table.getTbl_id()-1];
			int action = DBMSSimulator.TPCC_TRANSACTIONAL_CHANGE[i][table.getTbl_id()-1];
			
			//System.out.println("\t<"+table.getTbl_name()+">| data = "+data_nums+"| action = "+action);//+"|min = "+table.getTbl_min_cp()+"|max = "+table.getTbl_max_cp());
			
			switch(action) {
			case 0:						
					if(table.getTbl_partitions().size() == 1 && data_nums != 0) { // e.g. for <Warehouse> table; W=1
						for(Partition partition : table.getTbl_partitions()) {
							for(Data data : partition.getPartition_dataSet()) {
								trDataSet.add(data.getData_id());
								
								////System.out.println("\t\t* d"+data.getData_id());
							}
						}
					} else {
						for(int d = 0; d < data_nums; d++) {
							//================new
							switch(table.getTbl_name()) {
								case "Warehouse":
									_w_rank = DBMSSimulator.randomDataGenerator.nextZipf(table.getTbl_data_count(), 2.0);
									dataList = table.getTableData(_w_rank);
									_w = dataList.get(0);
									
									//System.out.println("\t\t--> W("+_w+")");
									break;
									
								case "Item":									
									_i_rank = DBMSSimulator.randomDataGenerator.nextZipf(table.getTbl_data_count(), 2.5);
									dataList = table.getTableData(_i_rank);
									_i = dataList.get(0);
									
									//System.out.println("\t\t--> I("+_i+")");
									break;
									
								case "District":
									if(i <= 1) {
										keyList = new ArrayList<Integer>();
										keyList.add(_w);
										dataList = table.getTableData(keyList);
										_d = dataList.get(1);
									} else {
										index = DBMSSimulator.random.nextInt(_cache.size());
										_d = this._district_cache.get(index);
										//System.out.println("index="+index+"|_d="+_d+"|cache size="+_cache.size());
										
										while(!this._cache.containsKey(_d)) {
											index = DBMSSimulator.random.nextInt(_cache.size());
											_d = this._district_cache.get(index);
										}
										
										//System.out.println("index="+index+"|_d="+_d+"|cache size="+_cache.size());
										_cache_items = this._cache.get(_d);
										_c = _cache_items.get(0);
										_o = _cache_items.get(1);
										_no = _cache_items.get(2);
										_ol = _cache_items.get(3);
										_s = _cache_items.get(4);
										
										//System.out.println("\t\t>> Retrieving cached data <D("+_d+")|C("+_c+")|O("+_o+")|NO("+_no+")|OL("+_ol+")|S("+_s+")>");
										
										dataList = new ArrayList<Integer>();
										dataList.add(_d);
									}
									
									//System.out.println("\t\t--> D("+_d+") for W("+_w+")");
									break;
									
								case "Stock":
									if(i <= 1) {
										keyList = new ArrayList<Integer>();
										keyList.add(_w);
										keyList.add(_i);
										dataList = table.getTableData(keyList);
										_s = dataList.get(1);
									} else {
										dataList = new ArrayList<Integer>();
										dataList.add(_s);
									}
									
									//System.out.println("\t\t--> S("+_s+") for W("+_w+") and I("+_i+")");
									break;
									
								case "Customer": // District Table
									if(i <= 1) {
										keyList = new ArrayList<Integer>();
										keyList.add(_d);
										dataList = table.getTableData(keyList);
										_c = dataList.get(1);										
									} else {																													
										index = DBMSSimulator.random.nextInt(_cache.size());
										_d = this._district_cache.get(index);
										_cache_items = this._cache.get(_d);
										_c = _cache_items.get(0);
										_o = _cache_items.get(1);
										_no = _cache_items.get(2);
										_ol = _cache_items.get(3);
										_s = _cache_items.get(4);
										
										//System.out.println("\t\t>> Retrieving cached data <D("+_d+")|C("+_c+")|O("+_o+")|NO("+_no+")|OL("+_ol+")|S("+_s+")>");
										
										dataList = new ArrayList<Integer>();
										dataList.add(_c);
									}
									
									//System.out.println("\t\t--> C("+_c+") for D("+_d+") -- "+dataList.get(0));										
									break;
									
								case "History": // Customer Table
									// Nothing to do
									break;
									
								case "Orders":
									if(i <= 1) {
										keyList = new ArrayList<Integer>();
										keyList.add(_c);
										dataList = table.getTableData(keyList);
										_o = dataList.get(1);
									} else {
										int d_id = table.getTbl_data_id_map().get(_o);										
										dataList = new ArrayList<Integer>();										
										dataList.add(d_id);
										dataList.add(_o);										
									}
									
									//System.out.println("\t\t--> O("+_o+") for C("+_c+") -- "+dataList.get(0));
									break;
									
								case "New-Order": // Customer Table (the last 1/3 values from the Order table)					
									// Nothing to do
									break;
									
								case "Order-Line": // Stock Table (10 most popular values from the Stock table)
									keyList = new ArrayList<Integer>();
									keyList.add(_s);
									keyList.add(_o);										
									
									dataList = table.getTableData(keyList);
									_ol = dataList.get(1);	
									
									//System.out.println("\t\t--> OL("+_ol+") for S("+_s+") and O("+_o+") -- "+dataList.get(0));
									break;
							}
														
							if(trDataList.contains(dataList.get(0)) && d > 0) {
								--d;
							} else {
								trDataList.add(dataList.get(0));
								trDataSet.add(dataList.get(0));								
							}
						}
					}
					break;
			
			case 1:					
					// Create a new Data object
					data_id = db.getDb_data_numbers() + 1;					
					Data data = db.createNewDataObject(table, data_id);
					
					switch(table.getTbl_name()) {
						case("History"):
							data.getData_foreign_key().put(3, _d); // 3: District Table
							data.getData_foreign_key().put(5, _c); // 5: Customer Table
						
							table.getTbl_data_map_d().put(_d, _c, table.getTbl_data_count());
							table.getTbl_data_id_map().put(table.getTbl_data_count(), data_id);
							break;
						
						case("Orders"):
							//System.out.println("\t\t>> Inserting O("+table.getTbl_data_count()+") for C("+_c+"|D-"+_d+") with global data_id ["+data_id+"]");
							_o = table.getTbl_data_count();
							
							data.setData_primary_key(_o);							
							data.getData_foreign_key().put(5, _c); // 5: Customer Table
							
							table.getTbl_data_map_s().put(_c, data.getData_primary_key());
							table.getTbl_data_id_map().put(data.getData_primary_key(), data_id);							
							
							// Also put an entry in the New-Order and Order-Line Table														
							// New-Order
							Table t_no = db.getTable(8);
							int no_data_id = data_id;
							++no_data_id;							
														
							Data no_data = db.createNewDataObject(t_no, no_data_id);							
							_no = t_no.getTbl_data_count();
							//System.out.println("\t\t>> Inserting NO("+_no+") for O("+_o+") with global data_id ["+no_data_id+"]");
							
							no_data.setData_primary_key(_no);							
							no_data.getData_foreign_key().put(7, _o); // 7: Orders Table
						
							t_no.getTbl_data_map_s().put(_o, no_data.getData_primary_key());
							t_no.getTbl_data_id_map().put(no_data.getData_primary_key(), no_data_id);
							
							// Order-Line
							Table t_ol = db.getTable(9); 
							int ol_data_id = no_data_id;
							++ol_data_id;													
														
							Data ol_data = db.createNewDataObject(t_ol, ol_data_id);							
							_ol = t_ol.getTbl_data_count();
							//System.out.println("\t\t>> Inserting OL("+_ol+") for O("+_o+") and S("+_s+") with global data_id ["+ol_data_id+"]");
							
							ol_data.setData_primary_key(_ol);								
							ol_data.getData_foreign_key().put(4, _s); // 4: Stock Table
							ol_data.getData_foreign_key().put(7, _o); // 7: Order Table
							
							//System.out.println(">>-- _o="+_o+"|_s="+_s+"|Id="+t_ol.getTbl_data_count());
							t_ol.getTbl_data_map_d().put(_s, _o, ol_data.getData_primary_key());
							t_ol.getTbl_data_id_map().put(ol_data.getData_primary_key(), ol_data_id);
							
							// Caching
							_cache_items = new ArrayList<Integer>();
							_cache_items.add(_c);
							_cache_items.add(_o);
							_cache_items.add(_no);
							_cache_items.add(_ol);
							_cache_items.add(_s);
							_district_cache.add(_d);
							_cache.put(_d, _cache_items);							
							
							//System.out.println("\t\t>> Caching D-"+_d+"|C-"+_c+"|O-"+_o+"|NO-"+_no+"|OL-"+_ol+"|S-"+_s);							
							break;													
					}			
					
					trDataSet.add(data_id);					
					break;
					
			case -1:					
					data_id = table.getTbl_data_id_map().get(_no);					
					Data _data = db.search(data_id);					
					Partition _partition = table.getPartition(_data.getData_localPartitionId());
					
					// Remove the entry from the Partition Data lookup table and remove the Data object from the Partition Data Set
					_partition.getPartition_dataLookupTable().put(_data.getData_id(), _partition.getPartition_globalId());
					_partition.getPartition_dataSet().remove(_data);
					_partition.updatePartitionLoad();						
					
					// Remove from table
					table.getTbl_data_map_s().remove(_o);
					table.getTbl_data_id_map().remove(_no);
					
					// Remove cache entry
					this._district_cache.remove((Object)_d);
					this._cache.remove(_d);
					//System.out.println("\t\t@ Removed D("+_d+") from cache | index ("+index+")"+"|cache size="+_cache.size());					
					
					// Remove the data id from the workload transactions
					workload.removeDataFromTransactions(data_id, workload.getTransactionListForSearchedData(data_id));
					
					// Decrement Data counts at Node and Database level
					db.getDb_dbs().getDbs_node(_partition.getPartition_nodeId()).decNode_totalData();
					int data_counts = db.getDb_data_numbers();
					db.setDb_data_numbers(--data_counts);
										
					//System.out.println("\t\t@ Deleting d"+data_id+" from "+_partition.getPartition_label());
					break;
			}
			
			table.updateTableLoad();
		}
		
		return trDataSet;
	}
}