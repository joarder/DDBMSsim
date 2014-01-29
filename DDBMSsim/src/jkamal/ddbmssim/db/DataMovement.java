/**
 * @author Joarder Kamal
 * 
 * Perform Data Movement after analysing Workload using HyperGraph Partitioning 
 */

package jkamal.ddbmssim.db;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import jkamal.ddbmssim.util.Matrix;
import jkamal.ddbmssim.util.MatrixElement;
import jkamal.ddbmssim.workload.MappingTable;
import jkamal.ddbmssim.workload.Transaction;
import jkamal.ddbmssim.workload.Workload;

public class DataMovement {
	private int intra_node_data_movements;
	private int inter_node_data_movements;
	
	public DataMovement() {}

	private int getIntra_node_data_movements() {
		return intra_node_data_movements;
	}

	private void setIntra_node_data_movements(int intra_node_data_movements) {
		this.intra_node_data_movements = intra_node_data_movements;
	}
	
	private void incIntra_node_data_movements() {
		int intra = this.getIntra_node_data_movements();
		this.setIntra_node_data_movements(++intra);
	}

	private int getInter_node_data_movements() {
		return inter_node_data_movements;
	}

	private void setInter_node_data_movements(int inter_node_data_movements) {
		this.inter_node_data_movements = inter_node_data_movements;
	}
	
	private void incInter_node_data_movements() {
		int inter = this.getInter_node_data_movements();
		this.setInter_node_data_movements(++inter);
	}

	private void setEnvironment(Database db, Workload workload, String type) {
		workload.calculateDistributedTransactions();
		this.setIntra_node_data_movements(0);
		this.setInter_node_data_movements(0);		
				
		for(Node node : db.getDb_dbs().getDbs_nodes()) {
			node.setNode_inflow(0);
			node.setNode_outflow(0);
		}
		
		for(Partition partition : db.getDb_partitions()) {
			partition.setPartition_inflow(0);
			partition.setPartition_outflow(0);
		}
	}
	
	private void wrappingUp(boolean movement, String message, Database db, Workload workload, String type) {
		workload.setWrl_hasDataMoved(true);					
		workload.setMessage(message);				
		workload.calculateDistributedTransactions();		
		workload.show(db, type);
	}
	
	public void performDataMovement(Database db, Workload workload, String strategy) {
		switch(strategy) {
		case "bs":
			this.baseStrategy(db, workload, strategy);
			break;
		case "s1":
			this.strategy1(db, workload, strategy);
			break;
		case "s2":
			this.strategy2(db, workload, strategy);
			break;
		}
	}
	
	private void baseStrategy(Database db, Workload workload, String type) {
		this.setEnvironment(db, workload, type);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(db, workload, type);
		System.out.println("[ACT] Generating Data Movement Mapping Matrix ...\n" +
				"      (Row :: Pre-Partition Id, Col :: Cluster Id, Elements: Data Occurance Counts)");
		mapping.print();
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();		
		for(int col = 0; col < mapping.getN(); col++) {				
			keyMap.put(col, col); // which cluster will go to which partition
			//System.out.println("-#-Entry("+col+") [ACT] C"+col+"|P"+col);
		}
		
		// Perform Actual Data Movement
		this.move(db, workload, keyMap, type);		
		this.wrappingUp(true, "bs", db, workload, type);
	}
	
	private void strategy1(Database db, Workload workload, String type) {
		this.setEnvironment(db, workload, type);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(db, workload, type);
		System.out.println("[ACT] Generating Data Movement Mapping Matrix ...\n" +
				"   [Row :: Pre-Partition Id, Col :: Cluster Id, Elements: Data Occurance Counts]");
		mapping.print();
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();
		MatrixElement colMax;
		for(int col = 1; col < mapping.getN(); col++) {
			colMax = mapping.findColMax(col);
			keyMap.put(colMax.getCol_pos(), colMax.getRow_pos()); // which cluster will go to which partition
			//System.out.println("-#-Col("+col+") [ACT] C"+(colMax.getCol_pos())+"|P"+(colMax.getRow_pos()));
		}
		
		// Perform Actual Data Movement
		this.move(db, workload, keyMap, type);
		this.wrappingUp(true, "s1", db, workload, type);		
	}
	
	private void strategy2(Database db, Workload workload, String type) {	
		this.setEnvironment(db, workload, type);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(db, workload, type);
		System.out.println("[ACT] Generating Data Movement Mapping Matrix ...\n   [Row :: Pre-Partition Id, Col :: Cluster Id, Elements: Data Occurance Counts]");
		mapping.print();
				
		// Step-1 :: Max Movement Matrix Formation
		MatrixElement max;
		int diagonal_pos = 1;		
		
		for(int m = 1; m < mapping.getM(); m++) {
			max = mapping.findMax(diagonal_pos);
			//System.out.println("[ACT] Max: "+max.getCounts()+", Col: "+(max.getCol_pos()+1)+", Row: "+(max.getRow_pos()+1));
			
			// Row/Col swap with diagonal Row/Col
			if(max.getCounts() != 0) {
				mapping.swap_row(max.getRow_pos(), diagonal_pos);
				mapping.swap_col(max.getCol_pos(), diagonal_pos);
			}			
			
			++diagonal_pos;
		}		

		// @debug
		System.out.println("[ACT] Creating Movement Matrix after Sub Matrix Max calculation ...");
		mapping.print();
		
		// Step-2 :: PID Conversion		
		// Create the PID conversion Key Map
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>(); 
		for(int row = 1; row < mapping.getM(); row++) {
			keyMap.put((int)mapping.getMatrix()[0][row].getCounts(), (int)mapping.getMatrix()[row][0].getCounts());
			//System.out.println("-#-Row("+row+" [ACT] C"+(int)mapping.getMatrix()[0][row].getCounts()+"|P"+(int)mapping.getMatrix()[row][0].getCounts());
		}
	
		// Perform Actual Data Movement	
		this.move(db, workload, keyMap, type);
		this.wrappingUp(true, "s2", db, workload, type);
	}
	
	private void updateData(Data data, int dst_partition_id, int dst_node_id, boolean roaming) {		
		data.setData_partitionId(dst_partition_id);					
		data.setData_nodeId(dst_node_id);
		
		if(roaming)
			data.setData_isRoaming(true);
		else
			data.setData_isRoaming(false);
	}
	
	private void updatePartition(Database db, Data data, int current_partition_id, int dst_partition_id) {                                        
        Partition current_partition = db.getPartition(current_partition_id);
        Partition dst_partition = db.getPartition(dst_partition_id);
        Partition home_partition = db.getPartition(data.getData_homePartitionId());
        
        // Actual Movement
        dst_partition.getPartition_dataSet().add(data);                
        current_partition.getPartition_dataSet().remove(data);
        
        // Update Lookup Table
        home_partition.getPartition_dataLookupTable().remove(data.getData_id());
        home_partition.getPartition_dataLookupTable().put(data.getData_id(), dst_partition_id);                                                
	}
	
	private void updateMovementCounts(Database db, int dst_node_id, int current_node_id, int dst_partition_id, int current_partition_id) {
		if(dst_node_id != current_node_id) {
			this.incInter_node_data_movements();
			
			db.getDb_dbs().getDbs_node(dst_node_id).incNode_totalData();
			db.getDb_dbs().getDbs_node(current_node_id).decNode_totalData();
			
			db.getDb_dbs().getDbs_node(dst_node_id).incNode_inflow();
			db.getDb_dbs().getDbs_node(current_node_id).incNode_outflow();
			
		} else {
			this.incIntra_node_data_movements();
			
			db.getPartition(dst_partition_id).incPartition_inflow();
			db.getPartition(current_partition_id).incPartition_outflow();
		}
	}		
	
	// Perform Actual Data Movement
	private void move(Database db, Workload workload, Map<Integer, Integer> keyMap, String type) {
		Partition home_partition = null;
		Partition current_partition = null;
		Partition dst_partition = null;
		int home_partition_id = -1;
		int current_partition_id = -1;
		int dst_partition_id = -1;		
		int current_node_id = -1;		
		int dst_node_id = -1;		
		
		Set<Integer> dataSet = new TreeSet<Integer>();
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {		
				for(Integer data_id : transaction.getTr_dataSet()) {					
					Data data = db.search(data_id);
					
					if(!dataSet.contains(data.getData_id())) {
						dataSet.add(data.getData_id());
						
						home_partition_id = data.getData_homePartitionId();
						home_partition = db.getPartition(data.getData_homePartitionId());																		
						
						current_partition_id = data.getData_partitionId();									
						current_partition = db.getPartition(current_partition_id);
						current_node_id = data.getData_nodeId();			
						
						switch(type) {
						case "hgr":
							dst_partition_id = keyMap.get(data.getData_hmetisClusterId());
							data.setData_hmetisClusterId(-1);
							break;
						case "chg":
							dst_partition_id = keyMap.get(data.getData_chmetisClusterId());
							data.setData_chmetisClusterId(-1);
							break;
						case "gr":
							dst_partition_id = keyMap.get(data.getData_metisClusterId());
							data.setData_metisClusterId(-1);
							break;
						}
						
						dst_partition = db.getPartition(dst_partition_id);
						dst_node_id = dst_partition.getPartition_nodeId();												
						
						if(dst_partition_id != current_partition_id) { // Data needs to be moved					
							if(data.isData_isRoaming()) { // Data is already Roaming
								if(dst_partition_id == home_partition_id) {
									this.updateData(data, dst_partition_id, dst_node_id, false);
									this.updatePartition(db, data, current_partition_id, dst_partition_id);
									this.updateMovementCounts(db, dst_node_id, current_node_id, dst_partition_id, current_partition_id);
									
									current_partition.decPartition_foreign_data();
									home_partition.decPartition_roaming_data();
									
								} else if(dst_partition_id == current_partition_id) {									
									// Nothing to do									
								} else {
									this.updateData(data, dst_partition_id, dst_node_id, true);
									this.updatePartition(db, data, current_partition_id, dst_partition_id);
									this.updateMovementCounts(db, dst_node_id, current_node_id, dst_partition_id, current_partition_id);
									
									dst_partition.incPartition_foreign_data();
									current_partition.decPartition_foreign_data();
									
								}
							} else {
								this.updateData(data, dst_partition_id, dst_node_id, true);
								this.updatePartition(db, data, current_partition_id, dst_partition_id);
								this.updateMovementCounts(db, dst_node_id, current_node_id, dst_partition_id, current_partition_id);
								
								dst_partition.incPartition_foreign_data();								
								home_partition.incPartition_roaming_data();
							}
						}
					}									
				} // end -- for()-Data
				// Resetting Transaction frequency
				transaction.setTr_frequency(1);
			} // end -- for()-Transaction
		} // end -- for()-Transaction-Type		
		
		switch(type) {
		case "hgr":
			workload.setWrl_hg_intraNodeDataMovements(this.getIntra_node_data_movements());
			workload.setWrl_hg_interNodeDataMovements(this.getInter_node_data_movements());
			break;
		case "chg":
			workload.setWrl_chg_intraNodeDataMovements(this.getIntra_node_data_movements());
			workload.setWrl_chg_interNodeDataMovements(this.getInter_node_data_movements());
			break;
		case "gr":
			workload.setWrl_gr_intraNodeDataMovements(this.getIntra_node_data_movements());
			workload.setWrl_gr_interNodeDataMovements(this.getInter_node_data_movements());
			break;
		}
		
	}
}