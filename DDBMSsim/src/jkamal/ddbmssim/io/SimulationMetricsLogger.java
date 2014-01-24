/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.DatabaseServer;
import jkamal.ddbmssim.db.Node;
import jkamal.ddbmssim.db.Partition;
import jkamal.ddbmssim.workload.Transaction;
import jkamal.ddbmssim.workload.Workload;

public class SimulationMetricsLogger {	
	private String db_logger;
	private String workload_logger;
	private String partition_logger;
	private boolean has_data_moved;
	private Map<Integer, String> partitionsBeforeDM;
	
	public SimulationMetricsLogger() {
		this.setDb_logger("db-log.txt");
		this.setWorkload_logger("workload-log.txt");
		this.setPartition_logger("partition-log.txt");
		this.setData_hasMoved(false);
		this.setPartitionsBeforeDM(new TreeMap<Integer, String>());
	}
	
	public String getDb_logger() {
		return db_logger;
	}

	public void setDb_logger(String db_logger) {
		this.db_logger = db_logger;
	}

	public String getWorkload_logger() {
		return workload_logger;
	}

	public void setWorkload_logger(String workload_logger) {
		this.workload_logger = workload_logger;
	}

	public String getPartition_logger() {
		return partition_logger;
	}

	public void setPartition_logger(String partition_logger) {
		this.partition_logger = partition_logger;
	}

	public boolean isData_movement() {
		return has_data_moved;
	}

	public void setData_hasMoved(boolean data_movement) {
		this.has_data_moved = data_movement;
	}

	public Map<Integer, String> getPartitionsBeforeDM() {
		return partitionsBeforeDM;
	}

	public void setPartitionsBeforeDM(Map<Integer, String> partitionsBeforeDM) {
		this.partitionsBeforeDM = partitionsBeforeDM;
	}

	public PrintWriter getWriter(String dir, String trace) {		
		File logFile = new File(dir+"\\"+trace+".txt");
		PrintWriter prWriter = null;
		
		try {
			if(!logFile.exists())
				logFile.createNewFile();
			
			try {
				prWriter = new PrintWriter(new BufferedWriter(new FileWriter(logFile)));				
				
			} catch(IOException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}
		
		return prWriter;
	}
	
	public void log(DatabaseServer dbs, Database db, PrintWriter prWriter) {
		int space = -1;
		
		try {							
			prWriter.print(Integer.toString(dbs.getDbs_nodes().size())+" ");		
			prWriter.println();
			
			space = dbs.getDbs_nodes().size();
			for(Node node : dbs.getDbs_nodes()) { 
				prWriter.print(Integer.toString(node.getNode_partitions().size()));
				
				if(space != 1)
					prWriter.print(" ");
				
				--space;
			}
			
			prWriter.println();			
			
			for(Partition partition : db.getDb_partitions()) {				
				space = db.getDb_partitions().size();
				
				prWriter.print(Integer.toString(partition.getPartition_dataSet().size())+" ");
				prWriter.print(Integer.toString(partition.getPartition_roaming_data())+" "); //getRoaming_dataObjects().size()
				prWriter.print(Integer.toString(partition.getPartition_foreign_data())); //getForeign_dataObjects().size()
				
				if(space != 1)
					prWriter.print(" ");
			
				--space;									
			}	
			
			prWriter.println();
		} finally {
			prWriter.flush();
			prWriter.close();
		} 
	}
	
	public void logDb(Database db, Workload workload, PrintWriter writer) {
		int partitions = db.getDb_partitions().size();		
		
		for(int i = 1; i <= partitions; i++) {
			Partition partition = db.getPartition(i);
			
			for(Data data : partition.getPartition_dataSet()) {
				if(!this.has_data_moved)
					writer.print("bf ");
				else 
					writer.print("af ");
				
				this.logData(workload, partition, data, writer);
				
				if(data.getData_transactions_involved().size() != 0) {					
					for(Integer transaction_id : data.getData_transactions_involved()) {
						Transaction transaction = workload.getTransaction(transaction_id);
						
						if(transaction != null) {
							writer.print("T"+transaction.getTr_id()+" ");						
							writer.print(transaction.getTr_weight()+" ");
							writer.print(transaction.getTr_ranking()+" ");
							writer.print(transaction.getTr_frequency()+" ");
						} else {
							//System.out.println("@debug >> "+data.toString()+" | T"+transaction_id);
						}
					}
				}
				
				writer.println();
			}
		}			
	}
	
	private void logData(Workload workload, Partition partition, Data data, PrintWriter writer) {
		writer.print("W"+workload.getWrl_id()+" ");
		writer.print("D"+data.getData_id()+" ");
		writer.print("N"+data.getData_nodeId()+" ");
		writer.print("P"+partition.getPartition_id()+" ");		
	}
	
	public void logWorkload(Database db, Workload workload, PrintWriter writer, String type) {
		switch(type) {
		case "hgr":
			if(!this.isData_movement()) {
				writer.print(workload.getWrl_id()+" ");			
				writer.print(workload.getMessage()+" ");				
				writer.print(workload.getWrl_distributedTransactions()+" ");																		
			} else {					
				writer.print(workload.getMessage()+" ");								
				writer.print(workload.getWrl_distributedTransactions()+" ");
				writer.print(workload.getWrl_hg_intraNodeDataMovements()+" ");
				writer.print(workload.getWrl_hg_interNodeDataMovements()+" ");				
				writer.println();
			}
			break;
			
		case "chg":
			if(!this.isData_movement()) {
				writer.print(workload.getWrl_id()+" ");			
				writer.print(workload.getMessage()+" ");				
				writer.print(workload.getWrl_distributedTransactions()+" ");																		
			} else {					
				writer.print(workload.getMessage()+" ");				
				writer.print(workload.getWrl_distributedTransactions()+" ");
				writer.print(workload.getWrl_chg_intraNodeDataMovements()+" ");
				writer.print(workload.getWrl_chg_interNodeDataMovements()+" ");				
				writer.println();
			}
			break;	
			
		case "gr":
			if(!this.isData_movement()) {
				writer.print(workload.getWrl_id()+" ");			
				writer.print(workload.getMessage()+" ");				
				writer.print(workload.getWrl_distributedTransactions()+" ");																		
			} else {					
				writer.print(workload.getMessage()+" ");				
				writer.print(workload.getWrl_distributedTransactions()+" ");
				writer.print(workload.getWrl_gr_intraNodeDataMovements()+" ");
				writer.print(workload.getWrl_gr_interNodeDataMovements()+" ");				
				writer.println();
			}
			break;
		}			
	}
	
	public void logPartition(Database db, Workload workload, PrintWriter prWriter) {		
		for(Partition partition : db.getDb_partitions()) {
			if(this.isData_movement())// && workload.getWrl_id() == 0)
				this.writePartitionLog(workload, partition, prWriter);
			//else
				//this.writePartitionLog(workload, partition, prWriter);
		}
	}
	
	private void writePartitionLog(Workload workload,Partition partition, PrintWriter prWriter) {
		prWriter.print(workload.getWrl_id()+" ");
		prWriter.print(workload.getMessage()+" ");
		prWriter.print(partition.getPartition_id()+" ");
		prWriter.print(partition.getPartition_nodeId()+" ");
		prWriter.print(partition.getPartition_current_load()+" ");
		prWriter.print(partition.getPartition_dataSet().size()+" ");
		prWriter.print(partition.getPartition_roaming_data()+" ");
		prWriter.print(partition.getPartition_foreign_data()+" ");
		prWriter.print(partition.getPartition_inflow()+" ");
		prWriter.print(partition.getPartition_outflow());
		prWriter.println();
	} 
	
	public void logNode(Database db, Workload workload, PrintWriter prWriter) {
		//int i = 0;
		for(Node node : db.getDb_dbs().getDbs_nodes()) {
			if(this.isData_movement()) //{
				this.writeNodeLog(workload, node, prWriter);
				//i = 1;
			//} else
				//this.writeNodeLog(workload, node, prWriter);
		}
	}
	
	private void writeNodeLog(Workload workload, Node node, PrintWriter prWriter) {
		prWriter.print(workload.getWrl_id()+" ");
		prWriter.print(workload.getMessage()+" ");
		prWriter.print(node.getNode_id()+" ");
		prWriter.print(node.getNode_total_data()+" ");
		prWriter.print(node.getNode_inflow()+" ");
		prWriter.print(node.getNode_outflow());
		prWriter.println();
	}
	
	public void logTransactionProp(Workload workload, PrintWriter prWriter) {
		prWriter.print(workload.getWrl_transactionTypes()+" ");
		
		int space = workload.getWrl_transactionProportions().length;
		for(double prop : workload.getWrl_transactionProportions()) {
			prWriter.print(Integer.toString((int)Math.round(prop)));
			--space;
			
			if(space != 0)
				prWriter.print(" ");
		}
		
		prWriter.println();	
	}
}