/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.db.Node;
import jkamal.ddbmssim.db.Partition;
import jkamal.ddbmssim.db.Table;
import jkamal.ddbmssim.main.DBMSSimulator;
import jkamal.ddbmssim.workload.Transaction;
import jkamal.ddbmssim.workload.Workload;

public class SimulationMetricsLogger {
	private File file_name;
	private Timings timing;
	private boolean has_data_moved;
	private Map<Integer, String> partitionsBeforeDM;
	
	public SimulationMetricsLogger() {
		this.setData_hasMoved(false);
		this.setPartitionsBeforeDM(new TreeMap<Integer, String>());
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

	public File getFile_name() {
		return file_name;
	}

	public void setFile_name(File file_name) {
		this.file_name = file_name;
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
		
		this.setFile_name(logFile);
		
		return prWriter;
	}
	
	
	public int traceWorkload(Database db, Workload workload, int serial, PrintWriter writer) {			
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				
				Iterator<Integer> data =  transaction.getTr_dataSet().iterator();
				
				++serial;
				writer.print(serial+" "+serial+" "+transaction.getTr_dataSet().size()+" ");
				
				while(data.hasNext()) {
					writer.print(db.getData(data.next()).getData_id());
					
					if(data.hasNext())
						writer.print(" ");
					else
						writer.println();
				}
			}
		}
		
		writer.flush();		
		
		return serial;
	}
	
	public void logWorkload(Workload workload, PrintWriter writer, String partitioner){
		
		if(!this.isData_movement()) {
			writer.print(workload.getWrl_id()+" ");			
			writer.print(partitioner+" ");				
			writer.print(workload.getWrl_distributedTransactions()+" ");
			
			for(int i = 0; i < workload.getWrl_dt_nums_typewise().length; i++)
				writer.print(workload.getWrl_dt_nums_typewise()[i]+" ");
			
			writer.print(workload.getWrl_meanDTI()+" ");
		} else {					
			writer.print(workload.getMessage()+" ");				
			writer.print(workload.getWrl_distributedTransactions()+" ");
			
			for(int i = 0; i < workload.getWrl_dt_nums_typewise().length; i++)
				writer.print(workload.getWrl_dt_nums_typewise()[i]+" ");
			
			writer.print(workload.getWrl_meanDTI()+" ");
			
			switch(partitioner) {
				case "hgr":
					writer.print(workload.getWrl_hgr_intraNodeDataMovements()+" ");
					writer.print(workload.getWrl_hgr_interNodeDataMovements()+" ");
					break;
					
				case "chg":
					writer.print(workload.getWrl_chg_intraNodeDataMovements()+" ");
					writer.print(workload.getWrl_chg_interNodeDataMovements()+" ");
					break;
				
				case "gr":
					writer.print(workload.getWrl_gr_intraNodeDataMovements()+" ");
					writer.print(workload.getWrl_gr_interNodeDataMovements()+" ");
					break;
			}
			
			if(workload.getWrl_id() != DBMSSimulator.SIMULATION_RUNS)
				writer.println();
		}
		
		writer.flush();		
	}
	
	public void logPartition(Database db, Workload workload, PrintWriter writer) {
		if(this.isData_movement()) {		
		
			DescriptiveStatistics _data = new DescriptiveStatistics();
			//DescriptiveStatistics _roaming_data = new DescriptiveStatistics();
			//DescriptiveStatistics _foreign_data = new DescriptiveStatistics();
			DescriptiveStatistics _inflow = new DescriptiveStatistics();
			DescriptiveStatistics _outflow = new DescriptiveStatistics();
			
			for(Table table : db.getDb_tables()) {
				
				if(table.getTbl_name() == "Orders" || table.getTbl_name() == "New-Order" || table.getTbl_name() == "Order-Line" || table.getTbl_name() == "History") {
					for(Partition partition : table.getTbl_partitions()) {				
						//if(this.isData_movement()) {
							//this.writePartitionLog(workload, partition, prWriter);
						
							_data.addValue(partition.getPartition_dataSet().size());
							//_roaming_data.addValue(partition.getPartition_roaming_data());
							//_foreign_data.addValue(partition.getPartition_foreign_data());
							_inflow.addValue(partition.getPartition_inflow());
							_outflow.addValue(partition.getPartition_outflow());				
						//}
					}
				}
			}
			
			writer.print(workload.getWrl_id()+" ");
			
			this.writeStats(writer, _data, db.getDb_partitions());
			//this.writeStats(prWriter, _roaming_data);
			//this.writeStats(prWriter, _foreign_data);
			this.writeStats(writer, _inflow, db.getDb_partitions());
			this.writeStats(writer, _outflow, db.getDb_partitions());		
			
			if(workload.getWrl_id() != DBMSSimulator.SIMULATION_RUNS)
				writer.println();
			
			writer.flush();
		}
	} 
	
	public void logNode(Database db, Workload workload, PrintWriter writer) {
		if(this.isData_movement()) {
			DescriptiveStatistics _data = new DescriptiveStatistics();
			DescriptiveStatistics _inflow = new DescriptiveStatistics();
			DescriptiveStatistics _outflow = new DescriptiveStatistics();
			
			for(Node node : db.getDb_dbs().getDbs_nodes()) {
				//if(this.isData_movement()) //{
					//this.writeNodeLog(workload, node, prWriter);
				
				_data.addValue(node.getNode_total_data());
				_inflow.addValue(node.getNode_inflow());
				_outflow.addValue(node.getNode_outflow());
				
			}
			
			writer.print(workload.getWrl_id()+" ");			
			
			this.writeStats(writer, _data, db.getDb_dbs().getDbs_nodes().size());
			this.writeStats(writer, _inflow, db.getDb_dbs().getDbs_nodes().size());
			this.writeStats(writer, _outflow, db.getDb_dbs().getDbs_nodes().size());
			
			if(workload.getWrl_id() != DBMSSimulator.SIMULATION_RUNS)
				writer.println();
			
			writer.flush();
		}
	}
	
	private void writeStats(PrintWriter writer, DescriptiveStatistics stats, int n) {
		writer.print(stats.getMin()+" ");				// Min
		writer.print(stats.getPercentile(25)+" "); 		// Q1
		writer.print(stats.getPercentile(50)+" "); 		// Median
		writer.print(stats.getPercentile(75)+" "); 		// Q3
		writer.print(stats.getMax()+" ");				// Max
		writer.print(stats.getMean()+" ");				// Mean
		writer.print(stats.getStandardDeviation()+" "); // SD
	}
	
	/*private void writePartitionLog(Workload workload, Partition partition, PrintWriter prWriter) {
		prWriter.print(workload.getWrl_id()+" ");
		prWriter.print(workload.getMessage()+" ");
		prWriter.print(partition.getPartition_globalId()+" ");
		prWriter.print(partition.getPartition_nodeId()+" ");
		prWriter.print(partition.getPartition_current_load()+" ");
		prWriter.print(partition.getPartition_dataSet().size()+" ");
		prWriter.print(partition.getPartition_roaming_data()+" ");
		prWriter.print(partition.getPartition_foreign_data()+" ");
		prWriter.print(partition.getPartition_inflow()+" ");
		prWriter.print(partition.getPartition_outflow());
		prWriter.println();
	}
	
	private void writeNodeLog(Workload workload, Node node, PrintWriter prWriter) {
		prWriter.print(workload.getWrl_id()+" ");
		prWriter.print(workload.getMessage()+" ");
		prWriter.print(node.getNode_id()+" ");
		prWriter.print(node.getNode_total_data()+" ");
		prWriter.print(node.getNode_inflow()+" ");
		prWriter.print(node.getNode_outflow());
		prWriter.println();
	}*/
	
	public void logTransactionProp(Workload workload, PrintWriter writer) {
		writer.print(workload.getWrl_transactionTypes()+" ");
		
		int space = workload.getWrl_transactionProportions().length;
		for(double prop : workload.getWrl_transactionProportions()) {
			writer.print(Integer.toString((int)Math.round(prop)));
			--space;
			
			if(space != 0)
				writer.print(" ");
		}
		
		writer.println();		
		writer.flush();
	}
	
	public void logTimings(PrintWriter writer, String state){
		switch(state){
			case "start":
				this.timing = new Timings(System.nanoTime());
				break;
			case "stop":
				writer.print((System.nanoTime() - this.timing.getStart_time())+" ");					
				writer.flush();
				break;
			case"newline":
				writer.println();		
				writer.flush();
				break;
			case "skip":
				writer.print(Integer.toString(0)+" ");					
				writer.flush();
				break;
			default:
				break;
		}
	}
}