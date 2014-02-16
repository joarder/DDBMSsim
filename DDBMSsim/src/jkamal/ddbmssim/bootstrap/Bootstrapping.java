/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.bootstrap;

import java.util.LinkedList;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.Node;
import jkamal.ddbmssim.db.Partition;
import jkamal.ddbmssim.db.Table;
import jkamal.ddbmssim.main.DBMSSimulator;

public class Bootstrapping {
	// Populate Database with Tables and Partitions
	private void populateTpccDatabase(Database db) {
		int table_id = 1;
		int partition_id = 1;
		int global_partition_id = 1;
		int partition_nums = 0;
		int[] table_data = new int[DBMSSimulator.TPCC_TABLE_TYPE.length];
		String[] table_names = {
				"Warehouse",
				"Item",
				"District",
				"Stock",
				"Customer",
				"History",
				"Order",
				"New-Order",				
				"Order-Line"
			};
		
		for(table_id = 1; table_id <= DBMSSimulator.TPCC_TABLE_TYPE.length; table_id++) {
			Table table = new Table(table_id, DBMSSimulator.TPCC_TABLE_TYPE[(table_id - 1)], db.getDb_id(), table_names[(table_id - 1)]);
			db.getDb_tables().add(table);
			
			System.out.println("[ACT] Creating <"+table.getTbl_name()+"> Table T"+table.getTbl_id()+" ...");
			
			// Determine the number of Data rows to be populated for each individual table
			switch(table.getTbl_name()) {
				case "Warehouse":
					table_data[table.getTbl_id() - 1] = DBMSSimulator.TPCC_WAREHOUSE;
					break;
				case "Item":
					table_data[table.getTbl_id() - 1] = (int) (DBMSSimulator.TPCC_ITEM * DBMSSimulator.TPCC_Scale);
					break;
				case "District":
					table_data[table.getTbl_id() - 1] = (int) (10 * DBMSSimulator.TPCC_WAREHOUSE * DBMSSimulator.TPCC_Scale);
					break;
				case "Stock":
					table_data[table.getTbl_id() - 1] = (int) (100000 * DBMSSimulator.TPCC_WAREHOUSE * DBMSSimulator.TPCC_Scale);
					break;
				case "Customer":
					table_data[table.getTbl_id() - 1] = (int) (3000 * table_data[2] * DBMSSimulator.TPCC_Scale); // District Table
					break;
				case "History":
					table_data[table.getTbl_id() - 1] = (int) (1 * table_data[4] * DBMSSimulator.TPCC_Scale); // Customer Table
					break;
				case "Order":
					table_data[table.getTbl_id() - 1] = (int) (300000 * DBMSSimulator.TPCC_WAREHOUSE * DBMSSimulator.TPCC_Scale);
					break;
				case "New-Order":
					table_data[table.getTbl_id() - 1] = (int) (1 * table_data[4] * DBMSSimulator.TPCC_Scale); // Customer Table
					break;
				case "Order-Line":
					table_data[table.getTbl_id() - 1] = (int) (3 * table_data[3] * DBMSSimulator.TPCC_Scale); // Stock Table
					break;
			}
			
			
			
			
			/*if(table.getTbl_type() == 0) {
				if(table.getTbl_name() == "Warehouse") {
					table_data[table.getTbl_id() - 1] = DBMSSimulator.TPCC_WAREHOUSE * DBMSSimulator.TPCC_TABLE_DATA[table.getTbl_id() - 1];
					//System.out.println("@-- Table <"+table.getTbl_name()+"> | data = "+table_data[table.getTbl_id() - 1]);
				} else {
					table_data[table.getTbl_id() - 1] = (int) (DBMSSimulator.TPCC_Scale * DBMSSimulator.TPCC_TABLE_DATA[table.getTbl_id() - 1]);
					//System.out.println("@-- Table <"+table.getTbl_name()+"> | data = "+table_data[table.getTbl_id() - 1]);
				}
			} else if(table.getTbl_name() == "District") {
				table_data[table.getTbl_id() - 1] = DBMSSimulator.TPCC_WAREHOUSE * DBMSSimulator.TPCC_TABLE_DATA[table.getTbl_id() - 1];
				//System.out.println("@-- Table <"+table.getTbl_name()+"> | data = "+table_data[table.getTbl_id() - 1]);
			} else {
				table_data[table.getTbl_id() - 1] = (int) (DBMSSimulator.TPCC_WAREHOUSE * DBMSSimulator.TPCC_Scale * DBMSSimulator.TPCC_TABLE_DATA[table.getTbl_id() - 1]);
				//System.out.println("@-- Table <"+table.getTbl_name()+"> | data = "+table_data[table.getTbl_id() - 1]);
			}*/
			
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
		LinkedList<Integer> linkSet = null;
		int table_data_id = 0;
		int table_foreign_key = 0;
		int foreign_table = 0;
		int table_link = 0;
		int data_id = 1;
		
		for(Table table : db.getDb_tables()) {
			table_data_id = 1;
			System.out.println(">-- <"+table.getTbl_name()+"> | data = "+table_data[table.getTbl_id() - 1]);
			
			// Get the table dependencies and associations
			if(table.getTbl_type() != 0)
				linkSet = db.getSchemaLinkSet(table.getTbl_id());
			
			for(int d = 0; d < table_data[table.getTbl_id() - 1]; d++) {
				
				if(table_link == 0) {
					foreign_table = linkSet.getFirst() + 1;
					table_link = table_data[foreign_table - 1];
					table_foreign_key = 1;
					System.out.println("\t-- Foreign Table <"+(foreign_table)+"> | data = "+table_link);
				}
				
				// Generate Partition Id
				int target_partition = (data_id % db.getDb_dbs().getDbs_nodes().size());
				Partition partition = table.getPartition(target_partition + 1);
				
				// Create a new Data Row Object
				Data data = new Data(data_id, partition.getPartition_id(), partition.getPartition_globalId(), table.getTbl_id(), partition.getPartition_nodeId(), false);				
				data.setData_pk(table.getTbl_id());
				data.getData_foreign_key().put(foreign_table, table_foreign_key);
				data.setData_size(DBMSSimulator.TPCC_DATA_ROW_SIZE[partition.getPartition_table_id() - 1]);
				
				// Put an entry into the Partition Data lookup table and add in the Data object into the Partition Data Set
				partition.getPartition_dataLookupTable().put(data.getData_id(), partition.getPartition_globalId());
				partition.getPartition_dataSet().add(data);
				partition.updatePartitionLoad();
				
				// Increment Node Data count by 1
				db.getDb_dbs().getDbs_node(partition.getPartition_nodeId()).incNode_totalData();
				System.out.println("\t\t @-- "+data.getData_id()+"|fk("+data.getData_foreign_key()+")");
				++data_id;
				++table_foreign_key;
				++table_data_id;
				--table_link;
			}
			
			table.setTbl_data_count(data_id - 1);
			table.updateTableLoad();
		}
		
		db.setDb_data_numbers(data_id - 1);
		db.getDb_dbs().updateNodeLoad();
	}	
	
	public void bootstrapping(Database db) {
		switch(db.getDb_name()) {
		case "tpcc":
			this.populateTpccDatabase(db);
			break;
		}
	}			
}
