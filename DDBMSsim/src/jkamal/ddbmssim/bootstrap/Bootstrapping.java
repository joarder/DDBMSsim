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
		String[] table_names = {
			"Customer",
			"District",
			"History",
			"Item",
			"New-Order",
			"Order",
			"Order-Line",
			"Stock",
			"Warehouse"
		};
		
		int table_id = 1;
		int partition_id = 1;
		int global_partition_id = 1;
		int partition_nums = 0;
		
		for(table_id = 1; table_id <= DBMSSimulator.TPCC_TABLE_TYPE.length; table_id++) {
			Table table = new Table(table_id, DBMSSimulator.TPCC_TABLE_TYPE[(table_id - 1)], db.getDb_id(), table_names[(table_id - 1)]);
			db.getDb_tables().add(table);
			
			System.out.println("[ACT] Creating <"+table.getTbl_name()+"> Table T"+table.getTbl_id()+" ...");
			
			if(DBMSSimulator.TPCC_TABLE_DATA[table.getTbl_id()-1] < db.getDb_dbs().getDbs_nodes().size())				
				partition_nums = DBMSSimulator.TPCC_TABLE_DATA[table.getTbl_id()-1];
			else
				partition_nums = db.getDb_dbs().getDbs_nodes().size();
			
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
		int data_id = 1;
		for(Table table : db.getDb_tables()) {
			// Generate Primary and Foreign Keys
			LinkedList<Integer> linkSet = db.getSchemaLinkSet(table.getTbl_id());
			String[] keys = db.getTableKeys(table.getTbl_type(), table.getTbl_id(), data_id, linkSet);
			
			for(int d = 0; d < DBMSSimulator.TPCC_TABLE_DATA[table.getTbl_id()-1]; d++) {												
				// Generate Partition Id
				int target_partition = data_id % db.getDb_dbs().getDbs_nodes().size();
				Partition partition = table.getPartition(target_partition + 1);
				
				// Create a new Data Row Object
				Data data = new Data(data_id, keys[0], keys[1], partition.getPartition_id(), partition.getPartition_globalId(), table.getTbl_id(), partition.getPartition_nodeId(), false);				
				data.setData_pk(table.getTbl_id());
				data.setData_size(DBMSSimulator.TPCC_DATA_ROW_SIZE[partition.getPartition_table_id() - 1]);
				
				// Put an entry into the Partition Data lookup table and add in the Data object into the Partition Data Set
				partition.getPartition_dataLookupTable().put(data.getData_id(), partition.getPartition_globalId());
				partition.getPartition_dataSet().add(data);
				partition.updatePartitionLoad();
				
				// Increment Node Data count by 1
				db.getDb_dbs().getDbs_node(partition.getPartition_nodeId()).incNode_totalData();
				System.out.println("@ "+data.toString()+"|pk("+data.getData_primary_key()+")|fk("+data.getData_foreign_key()+")");
				++data_id;
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
