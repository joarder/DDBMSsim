/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.bootstrap;

import java.util.ArrayList;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.Node;
import jkamal.ddbmssim.db.Partition;
import jkamal.ddbmssim.db.Table;
import jkamal.ddbmssim.main.DBMSSimulator;

public class Bootstrapping {
	private int getBase(int x) { 
		int value = Math.abs(x);
		
		if (value == 0) 
			return 1; 
		else 
			return (int) (1 + Math.floor((Math.log(value)/Math.log(10.0d)))); 
	} 
	
	// Populate Database with Tables and Partitions
	private void populateTpccDatabase(Database db) {
		int table_id = 1;
		int partition_id = 1;
		int global_partition_id = 1;
		int partition_nums = 0;
		int[] table_data = new int[DBMSSimulator.TPCC_TABLE_TYPE.length];
		String[] table_names = {"Warehouse", "Item", "District", "Stock", "Customer", "History", "Orders", "New-Order", "Order-Line"};
		
		for(table_id = 1; table_id <= DBMSSimulator.TPCC_TABLE_TYPE.length; table_id++) {
			Table table = new Table(table_id, DBMSSimulator.TPCC_TABLE_TYPE[table_id-1], db.getDb_id(), table_names[table_id-1]);
			db.getDb_tables().add(table);
			
			//System.out.println("[ACT] Creating <"+table.getTbl_name()+"> Table T"+table.getTbl_id()+" ...");
			
			// Determine the number of Data rows to be populated for each individual table
			switch(table.getTbl_name()) {
				case "Warehouse":
					table_data[table.getTbl_id()-1] = DBMSSimulator.TPCC_WAREHOUSE;
					break;
				case "Item":
					table_data[table.getTbl_id()-1] = (int) (100000 * DBMSSimulator.TPCC_Scale);					
					break;
				case "District":
					table_data[table.getTbl_id()-1] = 10 * DBMSSimulator.TPCC_WAREHOUSE;
					break;
				case "Stock":
					table_data[table.getTbl_id()-1] = (int) (100000 * DBMSSimulator.TPCC_WAREHOUSE * DBMSSimulator.TPCC_Scale);
					break;
				case "Customer": // District Table
					table_data[table.getTbl_id()-1] = (int) (3000 * table_data[2] * DBMSSimulator.TPCC_Scale);
					break;
				case "History": // Customer Table
					table_data[table.getTbl_id()-1] = table_data[4]; 
					break;
				case "Orders":
					table_data[table.getTbl_id()-1] = (int) (30000 * DBMSSimulator.TPCC_WAREHOUSE * DBMSSimulator.TPCC_Scale);
					break;
				case "New-Order": // Customer Table (the last 1/3 values from the Order table)					
					table_data[table.getTbl_id()-1] =  (int) (Math.pow(10.0d, (this.getBase(table_data[6]) -1)) - Math.pow(10.0d, (this.getBase(table_data[6]) -2)));					
					break;
				case "Order-Line": // Stock Table (10 most popular values from the Stock table)
					table_data[table.getTbl_id()-1] =  table_data[6] * 10;
					break;
			}
			
			// Determine the number of Partitions for each individual table
			if(table_data[table.getTbl_id() - 1] < db.getDb_dbs().getDbs_nodes().size())				
				partition_nums = table_data[table.getTbl_id() - 1];
			else
				partition_nums = db.getDb_dbs().getDbs_nodes().size();
			
			// Create individual Partitions
			for(int p = 1; p <= partition_nums; p++) {
				Node node = db.getDb_dbs().getDbs_node(p);
				// Creating a new Partition
				Partition partition = new Partition(global_partition_id, partition_id, table_id, node.getNode_id());
				// Adding Partition to the Table
				table.getTbl_partitions().add(partition);
				// Adding the partition id into the table-partition map at the db level
				db.getDb_table_partition_map().put(partition.getPartition_globalId(), table.getTbl_id());
				// Adding partition to the Node
				node.getNode_partitions().add(partition.getPartition_globalId());		
				
				System.out.println("[ACT] Creating Partition "+partition.getPartition_label()
						+" under <"+table.getTbl_name()+"> Table and placing it into node N"+node.getNode_id());														
				
				++partition_id;
				++global_partition_id;			
			}				
			
			partition_id = 1;
		}
		
		db.setDb_partitions(global_partition_id - 1);				
				
		// Populating Data into Table Partitions
		ArrayList<Integer> linkedTables = null;
		int data_id = DBMSSimulator.getGlobal_tr_id();		
		
		for(Table table : db.getDb_tables()) {					
			//System.out.println(">-- <"+table.getTbl_name()+"> | data = "+table.getTbl_data_count());			
			
			// Get the table dependencies and associations for the non-primary tables
			linkedTables = db.getLinkedTables(table);			
			int[] f_key = new int[linkedTables.size()];
			
			boolean[] f_key_state = new boolean[linkedTables.size()];
			for(int i = 0; i < f_key_state.length; i++)
				f_key_state[i] = true;
			
			boolean first_time = true;
			
			for(int d = 1; d <= table_data[table.getTbl_id() - 1]; d++) {
				++data_id;
				DBMSSimulator.incGlobal_data_id();
				
				Data data = db.insertData(table.getTbl_id(), data_id);
				table.getTbl_data_pid_map().put(d, data_id);
				table.getTbl_pid_data_map().put(data_id, d);
				
				if(table.getTbl_type() != 2) { // Primary Tables					
					data.setData_primary_key(d);					
					// No foreign key for the Primary tables i.e. Warehouse and Item tables
					
					if(table.getTbl_type() == 1) { // Secondary Tables
						for(int i = 0; i < f_key.length; i++) {																					
							if(linkedTables.size() > 1) { // having multiple foreign keys
								
								if(i == 0) {									
									if(f_key[i+1] == table_data[linkedTables.get(i+1) - 1]) {
										f_key_state[i] = true;
										first_time = true;										
									}
								}
								
								if(f_key[i] == table_data[linkedTables.get(i) - 1]) {	
									if(i == 0 && f_key[i+1] < table_data[linkedTables.get(i+1) - 1]) {
										// Do nothing
									} else {									
										f_key[i] = 0;									
										f_key_state[i] = true;
									}									
								} else if(f_key[i] < table_data[linkedTables.get(i) - 1]) {
									if(i > 0)
										f_key_state[i] = true; 
									else if(f_key[i] < table_data[linkedTables.get(i) - 1] && i == 0 && !(first_time))
										f_key_state[i] = false;
								}
							} else { // having a single foreign key
								if(f_key[i] == table_data[linkedTables.get(i) - 1]) {
									f_key[i] = 0;
									f_key_state[i] = true;
								}
							}						
							
							// Generating foreign keys
							if(f_key_state[i] == true) {								
								int tmp = f_key[i];
								++tmp;
								f_key[i] = tmp;
								
								if(linkedTables.size() > 1)
									f_key_state[i] = false;
							}														
							
							data.getData_foreign_key().put(linkedTables.get(i), f_key[i]);
							table.getTbl_data_map_s().put(f_key[i], d);
							
							if(first_time)
								first_time = false;
						}						
					}					
				} else {
					if(table.getTbl_name() == "Order-Line")
						data.setData_primary_key(d);
					
					ArrayList<Integer> f_keys = new ArrayList<Integer>();
					for(int i = 0; i < f_key.length; i++) {														
						if(f_key[i] == table_data[linkedTables.get(i) - 1])
							f_key[i] = 0;
						
						int tmp = f_key[i];
						++tmp;
						f_key[i] = tmp;
						
						data.getData_foreign_key().put(linkedTables.get(i), f_key[i]);
						
						f_keys.add(f_key[i]);
					}
					
					table.getTbl_data_map_d().put(f_keys.get(0), f_keys.get(1), d);	
				}
				
				//System.out.println("\t\t @-- "+data.getData_id()+"|pk("+data.getData_primary_key()+"|fk("+data.getData_foreign_key()+")");				
			} //--end for()
			
			table.updateTableLoad();
		}
		
		db.updateDb_data_numbers();
		db.getDb_dbs().updateNodeLoad();
	}	
	
	// Start bootstrapping process
	public void bootstrapping(Database db) {
		switch(db.getDb_name()) {
		case "tpcc":
			this.populateTpccDatabase(db);
			break;
		}
	}			
}
