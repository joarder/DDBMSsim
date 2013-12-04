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
	private boolean data_movement;
	private Map<Integer, String> partitionsBeforeDM;
	
	public SimulationMetricsLogger() {
		this.setDb_logger("db-log.txt");
		this.setWorkload_logger("workload-log.txt");
		this.setPartition_logger("partition-log.txt");
		this.setData_movement(false);
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
		return data_movement;
	}

	public void setData_movement(boolean data_movement) {
		this.data_movement = data_movement;
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
				if(!this.data_movement)
					writer.print("bf ");
				else 
					writer.print("af ");
				
				this.logData(workload, partition, data, writer);
				
				if(data.getData_transaction_involved().size() != 0) {					
					for(Integer transaction_id : data.getData_transaction_involved()) {
						Transaction transaction = workload.getTransaction(transaction_id);

						writer.print("T"+transaction.getTr_id()+" ");						
						writer.print(transaction.getTr_weight()+" ");
						writer.print(transaction.getTr_ranking()+" ");
						writer.print(transaction.getTr_frequency()+" ");							
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
		
		writer.print(data.getData_weight()+" ");
		writer.print(data.getData_ranking()+" ");
		writer.print(data.getData_frequency()+" ");		
	}
	
	public void logWorkload(Database db, Workload workload, PrintWriter writer) {		
		if(!this.isData_movement()) {
			writer.print(workload.getWrl_id()+" ");			
			writer.print(workload.getMessage()+" ");
			
			writer.print(workload.getWrl_DtNumbers()+" ");
			writer.print(workload.getWrl_DtImpact()+" ");																		
		} else {					
			//writer.print(workload.getWrl_data_movement_strategy()+" ");
			writer.print(workload.getMessage()+" ");
			
			writer.print(workload.getWrl_DtNumbers()+" ");
			writer.print(workload.getWrl_DtImpact()+" ");								
			
			writer.print(workload.getWrl_totalDataObjects()+" ");
			writer.print(workload.getWrl_intraNodeDataMovements()+" ");
			writer.print(workload.getWrl_percentageIntraNodeDataMovement()+" ");
			writer.print(workload.getWrl_interNodeDataMovements()+" ");
			writer.print(workload.getWrl_percentageInterNodeDataMovement());
			
			writer.println();			
		}				
	}
	
	public void logPartition(Database db, Workload workload, PrintWriter prWriter) {		
		for(Partition partition : db.getDb_partitions()) {
			if(!this.data_movement)
				prWriter.print("bf ");
			else 
				prWriter.print("af ");
			
			prWriter.print("W"+workload.getWrl_id()+" ");
			prWriter.print(workload.getMessage()+" ");
			prWriter.print("P"+partition.getPartition_id()+" ");
			prWriter.print("N"+partition.getPartition_nodeId()+" ");
			prWriter.print("-L "+partition.getPartition_current_load()+" ");
			prWriter.print("-H "+partition.getPartition_dataSet().size()+" ");
			prWriter.print("-R "+partition.getPartition_roaming_data()+" ");
			prWriter.print("-F "+partition.getPartition_foreign_data());
			prWriter.println();
		}
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