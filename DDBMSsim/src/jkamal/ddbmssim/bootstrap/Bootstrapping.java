/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.bootstrap;

import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.Node;
import jkamal.ddbmssim.db.Partition;
import jkamal.ddbmssim.db.Table;
import jkamal.ddbmssim.main.DBMSSimulator;

public class Bootstrapping {
	// Populate Database with Tables and Partitions
	private void populateDatabase(Database db) {		
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
		
		for(table_id = 1; table_id <= DBMSSimulator.TPCC_TABLE.length; table_id++) {
			Table table = new Table(table_id, db.getDb_id(), table_names[(table_id - 1)]);
			db.getDb_tables().add(table);
			System.out.println("[ACT] Creating <"+table.getTbl_name()+"> Table T"+table.getTbl_id()+" ...");
			
			
			for(Node node : db.getDb_dbs().getDbs_nodes()) {
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
		for(Table tbl : db.getDb_tables()) {			
			for(int d = 0; d < DBMSSimulator.TPCC_TABLE[tbl.getTbl_id()-1]; d++) {
				int target_partition = data_id % db.getDb_dbs().getDbs_nodes().size();
				Partition partition = tbl.getPartition(target_partition+1);
				
				Data data = new Data(data_id, partition.getPartition_globalId(), partition.getPartition_nodeId(), false);				
				data.setData_pk(partition.getPartition_table_id());
				data.setData_size(DBMSSimulator.DATA_ROW_SIZE[partition.getPartition_table_id()-1]);

				
				// Put an entry into the Partition Data lookup table and add in the Data object into the Partition Data Set
				partition.getPartition_dataLookupTable().put(data.getData_id(), partition.getPartition_globalId());
				partition.getPartition_dataSet().add(data);
				partition.updatePartitionLoad();
				// Increment Node Data count by 1
				db.getDb_dbs().getDbs_node(partition.getPartition_nodeId()).incNode_totalData();
				
				++data_id;
			}
		}
	}
	
	/*private void tpcc(DatabaseServer dbs, Database db, int DATA_OBJECTS) {
		//Table table;
		Partition partition;
		ArrayList<Data> dataList;
		Data data;												
		int node_id = 1; //1
		int partition_nums = (int) Math.ceil((double) DATA_OBJECTS/(db.getDb_partitionSize() * 0.8));
		int data_id = 1; //0
		int partition_data_numbers = 0;
		int global_data = 0;
		int pos = 0;
		
		int pk = 1;
		int primary_key = 1;
		int pk_range = DBMSSimulator.PK_ARRAY[pk-1];
		//int pk_range = (int) (Math.ceil(DATA_OBJECTS * pk_array[pk-1]) / 100);		
		
		// i -- partition
		for(int partition_id = 1; partition_id <= partition_nums; partition_id++) {	
			if(node_id > dbs.getDbs_nodes().size())
				node_id = 1;
			
			// Create a new Partition and attach it to the Database			
			partition = new Partition(partition_id, String.valueOf(partition_id), node_id, db.getDb_partitionSize());
			
			db.getDb_partitionSize().add(partition);
			partition_data_numbers = (int) ((int)(partition.getPartition_capacity())*0.8);						
			
			System.out.print("[ACT] Creating Partition "+partition.getPartition_label());			
			
			// Create an ArrayList for placing into the Routing Table for each i-th Partition entry
			dataList = new ArrayList<Data>();																											
			for(int k = 1; k <= partition_data_numbers && data_id <= DATA_OBJECTS; k++) {
				// Create a new Data Item within the Partition
				data = new Data(data_id, partition_id, node_id, false);
				partition.getPartition_dataSet().add(data);
				
				// Assigning Primary Key for each Row (Data)				
				if(pk_range == 0) {
					++pk;
					++pos;
					primary_key = 1;
					//pk_range = (int)((int) DATA_OBJECTS * pk_array[pk-1]);
					pk_range = DBMSSimulator.PK_ARRAY[pk-1];
				}				
				
				data.setData_pk(primary_key);
				data.setData_size(DBMSSimulator.DATA_ROW_SIZE[pos]);
				--pk_range;
				
				// Put an entry into the Partition Data lookup table
				partition.getPartition_dataLookupTable().put(data.getData_id(), partition.getPartition_globalId());
								
				dataList.add(data);				
				++data_id;
				++global_data;
				++primary_key;
			} // end -- for
			
			System.out.print(" with "+dataList.size()+" Data objects");
			
			// Calculate current load
			partition.getCurrentLoad();
			
			// Adding partition to the Node
			dbs.getDbs_node(node_id).getNode_partitions().add(partition.getPartition_globalId());
			dbs.getDbs_node(node_id).incNode_totalData(global_data);

			System.out.print(" and placing it into node N"+partition.getPartition_nodeId());
			System.out.println();
			
			++node_id;
		} // end -- for	
			
		// Add node-partitions map entries
		for(Node n : dbs.getDbs_nodes()) {
			Set<Integer> partitionSet = new TreeSet<Integer>();			
			for(Partition p : n.getNode_partitions()) {
				partitionSet.add(p.getPartition_globalId());
			}
			
			db.getDb_nodes().put(n.getNode_id(), partitionSet);
		}
		
		System.out.println("[MSG] Total Data Items: "+global_data);
		System.out.println("[MSG] Total Partitions: "+db.getDb_partitionSize());	
	}*/
	
	public void bootstrapping(Database db) {
		switch(db.getDb_name()) {
		case "tpcc":
			this.populateDatabase(db);
			break;
		}
	}			
}
