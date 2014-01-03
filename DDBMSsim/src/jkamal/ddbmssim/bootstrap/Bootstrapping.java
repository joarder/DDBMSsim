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
		int partition_nums = (int) Math.ceil((double) DATA_OBJECTS/(db.getDb_partition_size() * 0.8));
		int data_id = 1; //0
		int data_nums = 0;
		int global_data = 0;
		
		// TPC-C Database table (9) row counts in different scale
		//double[] pk_array = {0.19, 1.92, 1.92, 19.2, 57.69, 5.76, 5.76, 1.73, 5.76};
		int[] pk_array = {1, 1, 1, 10, 30, 3, 3, 3, 1}; // 53
		//int[] pk_array = {1, 10, 10, 100, 300, 30, 30, 30, 9}; // 520
		//int[] pk_array = {10, 100, 100, 1000, 3000, 300, 300, 300, 90}; //5,200
		int pk = 1;
		//int pk_range = (int) (Math.ceil(DATA_OBJECTS * pk_array[pk-1]) / 100);
		int pk_range = pk_array[pk-1];
		
		// i -- partition
		for(int partition_id = 1; partition_id <= partition_nums; partition_id++) {	
			if(node_id > dbs.getDbs_nodes().size())
				node_id = 1;
			
			// Create a new Partition and attach it to the Database			
			partition = new Partition(partition_id, String.valueOf(partition_id), node_id, db.getDb_partition_size());
			
			db.getDb_partitions().add(partition);
			data_nums = (int) ((int)(partition.getPartition_capacity())*0.8);						
			
			System.out.print("[ACT] Creating Partition "+partition.getPartition_label());			
			
			// Create an ArrayList for placing into the Routing Table for each i-th Partition entry
			dataList = new ArrayList<Data>();																											
			for(int k = 1; k <= data_nums && data_id <= DATA_OBJECTS; k++) {
				// Create a new Data Item within the Partition
				data = new Data(data_id, partition_id, node_id, false);
				partition.getPartition_dataSet().add(data);
				
				// Assigning Primary Key for each Row (Data)				
				if(pk_range == 0) {
					++pk;
					//pk_range = (int)((int) DATA_OBJECTS * pk_array[pk-1]);
					pk_range = pk_array[pk-1];
				}				
				
				data.setData_pk(("pk-"+pk));
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
