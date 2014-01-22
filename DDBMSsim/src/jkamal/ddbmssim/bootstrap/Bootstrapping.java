/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.bootstrap;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.DatabaseServer;
import jkamal.ddbmssim.db.Node;
import jkamal.ddbmssim.db.Partition;
import jkamal.ddbmssim.main.DBMSSimulator;

public class Bootstrapping {
	public Bootstrapping() {}
	
	public void rangePartitioning() {}
	public void hashPartitioning() {}
	public void saltPartitioning() {}
	public void consistentHashPartitioning() {}	
	
	// Synthetic Data Generation
	// Options: Range, Salting, Hash (Random), Consistent-Hash (Random)
	public void bootstrapping(DatabaseServer dbs, Database db, int DATA_OBJECTS) {		
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
		int pk_range = DBMSSimulator.PK_ARRAY[pk-1];
		//int pk_range = (int) (Math.ceil(DATA_OBJECTS * pk_array[pk-1]) / 100);		
		
		// i -- partition
		for(int partition_id = 1; partition_id <= partition_nums; partition_id++) {	
			if(node_id > dbs.getDbs_nodes().size())
				node_id = 1;
			
			// Create a new Partition and attach it to the Database			
			partition = new Partition(partition_id, String.valueOf(partition_id), node_id, db.getDb_partitionSize());
			
			db.getDb_partitions().add(partition);
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
					//pk_range = (int)((int) DATA_OBJECTS * pk_array[pk-1]);
					pk_range = DBMSSimulator.PK_ARRAY[pk-1];
				}				
				
				data.setData_pk(("pk-"+pk));
				data.setData_size(DBMSSimulator.DATA_ROW_SIZE[pos]);
				--pk_range;
				
				// Put an entry into the Partition Data lookup table
				partition.getPartition_dataLookupTable().put(data.getData_id(), partition.getPartition_id());
								
				dataList.add(data);				
				++data_id;
				++global_data;
			} // end -- for
			
			System.out.print(" with "+dataList.size()+" Data objects");
			
			// Calculate current load
			partition.getCurrentLoad();
			
			// Adding partition to the Node
			dbs.getDbs_node(node_id).getNode_partitions().add(partition);
			dbs.getDbs_node(node_id).incNode_totalData(global_data);

			System.out.print(" and placing it into node N"+partition.getPartition_nodeId());
			System.out.println();
			
			++node_id;
		} // end -- for	
			
		// Add node-partitions map entries
		for(Node n : dbs.getDbs_nodes()) {
			Set<Integer> partitionSet = new TreeSet<Integer>();			
			for(Partition p : n.getNode_partitions()) {
				partitionSet.add(p.getPartition_id());
			}
			
			db.getDb_nodes().put(n.getNode_id(), partitionSet);
		}
		
		System.out.println("[MSG] Total Data Items: "+global_data);
		System.out.println("[MSG] Total Partitions: "+db.getDb_partitions().size());
	}
}
