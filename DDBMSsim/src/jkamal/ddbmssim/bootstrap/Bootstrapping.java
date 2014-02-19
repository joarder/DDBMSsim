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
			
			System.out.println("[ACT] Creating <"+table.getTbl_name()+"> Table T"+table.getTbl_id()+" ...");
			
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
				Partition partition = new Partition(global_partition_id, partition_id, table_id, node.getNode_id(), db.getDb_partitionSize());
				// Adding Partition to the Table
				table.getTbl_partitions().add(partition);				
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
		int data_id = 1;
		
		for(Table table : db.getDb_tables()) {			
			table.setTbl_data_count(table_data[table.getTbl_id() - 1]);			
			System.out.println(">-- <"+table.getTbl_name()+"> | data = "+table.getTbl_data_count());//+table_data[table.getTbl_id() - 1]);			
			
			// Get the table dependencies and associations for the non-primary tables
			linkedTables = db.getLinkedTables(table.getTbl_id());			
			int[] f_key = new int[linkedTables.size()];												
			
			for(int d = 1; d <= table_data[table.getTbl_id() - 1]; d++) {
				Data data = this.createNewDataObject(db, table, data_id);
				++data_id;
				
				if(table.getTbl_type() != 2) { // Primary Tables					
					data.getData_primary_key().put(table.getTbl_id(), d);
					// No foreign key for the Primary tables i.e. Warehouse and Item tables
					
					if(table.getTbl_type() == 1) { // Secondary Tables
						for(int i = 0; i < f_key.length; i++) {														
							if(f_key[i] == table_data[linkedTables.get(i) - 1])
								f_key[i] = 0;
							
							int tmp = f_key[i];
							++tmp;
							f_key[i] = tmp;
							
							data.getData_foreign_key().put(linkedTables.get(i), f_key[i]);
						}						
					}					
					
				} else {
					for(int i = 0; i < f_key.length; i++) {														
						if(f_key[i] == table_data[linkedTables.get(i) - 1])
							f_key[i] = 0;
						
						int tmp = f_key[i];
						++tmp;
						f_key[i] = tmp;
						
						data.getData_foreign_key().put(linkedTables.get(i), f_key[i]);
					}
				}
				
				System.out.println("\t\t @-- "+data.getData_id()+"|pk("+data.getData_primary_key()+"|fk("+data.getData_foreign_key()+")");				
			} //--end for()
			
			table.setTbl_data_count(data_id - 1);
			table.updateTableLoad();
		}
		
		db.setDb_data_numbers(data_id - 1);
		db.getDb_dbs().updateNodeLoad();
	}
	
	// Create a new Data object and attach it with the designated Table and Partition within a given Database
	private Data createNewDataObject(Database db, Table table, int data_id) {
		// Generate Partition Id
		int target_partition = (data_id % db.getDb_dbs().getDbs_nodes().size());
		Partition partition = table.getPartition(target_partition + 1);
		
		// Create a new Data Row Object
		Data data = new Data(data_id, partition.getPartition_id(), partition.getPartition_globalId(), table.getTbl_id(), partition.getPartition_nodeId(), false, table.getTbl_type());				
		data.setData_pk(table.getTbl_id());				
		data.setData_size(DBMSSimulator.TPCC_DATA_ROW_SIZE[partition.getPartition_table_id() - 1]);				
		
		// Put an entry into the Partition Data lookup table and add in the Data object into the Partition Data Set
		partition.getPartition_dataLookupTable().put(data.getData_id(), partition.getPartition_globalId());
		partition.getPartition_dataSet().add(data);
		partition.updatePartitionLoad();
		
		// Increment Node Data count by 1
		db.getDb_dbs().getDbs_node(partition.getPartition_nodeId()).incNode_totalData();		
		
		return data;
	}
	
	private void insertForeignKey(ArrayList<Integer> linkedTables, int[] table_data, int[] f_key, Data data) {
		for(int i = 0; i < f_key.length; i++) {														
			if(f_key[i] == table_data[linkedTables.get(i) - 1])
				f_key[i] = 0;
			
			int tmp = f_key[i];
			++tmp;
			f_key[i] = tmp;
			
			data.getData_foreign_key().put(linkedTables.get(i), f_key[i]);
		}
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
