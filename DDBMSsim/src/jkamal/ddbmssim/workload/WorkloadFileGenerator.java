/**
 * @author Joarder Kamal
 */

package jkamal.ddbmssim.workload;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import jkamal.ddbmssim.db.Data;
import jkamal.ddbmssim.db.Database;
import jkamal.ddbmssim.main.DBMSSimulator;
import org.apache.commons.lang3.StringUtils;

public class WorkloadFileGenerator {
	
	// Assigns Shadow HMetis Data Id for Hypergraph partitioning
	public void assignShadowDataId(Database db, Workload workload) {
		// Cleanup
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {		
				for(Integer data_id : transaction.getTr_dataSet()) {
					Data data = db.search(data_id);
					
					if(data.isData_hasShadowId()) {					
						data.setData_shadowId(-1);
						data.setData_hasShadowId(false);
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
		
		
		int shadow_id = 1;		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				for(Integer data_id : transaction.getTr_dataSet()) {
					Data data = db.search(data_id);
					
					if(!data.isData_hasShadowId()) {
						workload.getWrl_dataId_shadowId_map().put(data.getData_id(), shadow_id);
						
						data.setData_shadowId(shadow_id);
						data.setData_hasShadowId(true);								
						++shadow_id;					
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
		
		workload.setWrl_totalDataObjects(shadow_id - 1);
	}
	
	public boolean generateWorkloadFile(Database db, Workload workload, String partitioner) throws IOException {
		boolean empty = false;
		
		switch(partitioner) {
		case "hgr":
			empty = this.generateHGraphWorkloadFile(db, workload);
			if(empty == false) 
				this.generateHGraphFixFile(db, workload);
			break;
			
		case "chg":
			empty = this.generateCHGraphWorkloadFile(db, workload);
			if(empty == false) 
				this.generateCHGraphFixFile(db, workload);
			break;
			
		case "gr":
			empty = this.generateGraphWorkloadFile(db, workload);
			break;
		}
		
		return empty;
	}
	
	// Generates Workload File for Hypergraph partitioning
	private boolean generateHGraphWorkloadFile(Database db, Workload workload) {
		File workloadFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_hGraphWorkloadFile());
		
		Data trData = null;
		int hyper_edges = workload.getWrl_totalTransactions();		
		int vertices = workload.getWrl_totalDataObjects();
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;						
		
		if(hyper_edges <= 1 ) {
			System.out.println("[ALM] Only "+hyper_edges+" hyperedges have been present in the workload");
			System.out.println("[ACT] Simulation will be aborted for this run ...");
			return true;
		} else {		
			try {
				workloadFile.createNewFile();
				Writer writer = null;
				try {
					writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
					writer.write(hyper_edges+" "+vertices+" "+hasTransactionWeight+""+hasDataWeight+"\n");
					//System.out.println(hyper_edges+" "+vertices+" "+hasTransactionWeight+""+hasDataWeight);
					for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
						for(Transaction transaction : entry.getValue()) {
							if(transaction.getTr_class() != "green") {
								writer.write(transaction.getTr_frequency()+" ");
								//System.out.println("@@ "+transaction.getTr_frequency()+"* ");
								Iterator<Integer> data =  transaction.getTr_dataSet().iterator();
								while(data.hasNext()) {
									trData = db.search(data.next());
									
									writer.write(Integer.toString(trData.getData_shadowId()));							
									
									if(data.hasNext())
										writer.write(" "); 
								} // end -- while() loop
								
								writer.write("\n");						
							} // end -- if()-Transaction Class
						} // end -- for()-Transaction
					} // end -- for()-Transaction-Types
	
					// Writing Data Weight
					Set<Integer> uniqueDataSet = new TreeSet<Integer>();
					int newline = 0;
					
					for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
						for(Transaction transaction : entry.getValue()) {
							if(transaction.getTr_class() != "green") {
								
								Iterator<Integer> data =  transaction.getTr_dataSet().iterator();							
								
								while(data.hasNext()) {
									trData = db.search(data.next());								
									
									if(!uniqueDataSet.contains(trData.getData_shadowId())) {
										++newline;
										
										writer.write(Integer.toString(trData.getData_weight()));	
										//System.out.println("@@ "+trData.getData_weight());
										//writer.write(Integer.toString(1));
										
										if(newline != vertices)
											writer.write("\n");										
										
										uniqueDataSet.add(trData.getData_shadowId());
									}
								}							
							}
						}
					}
					
				} catch(IOException e) {
					e.printStackTrace();
				}finally {
					writer.close();
				}
			} catch (IOException e) {		
				e.printStackTrace();
			}
			
			System.out.println("[OUT] Workload file generation for hypergraph partitioning has been completed.");
			return false;
		}
	}
	
	// Generates Fix Files (Determines whether a Data is movable from its current Partition or not) 
	// for Hypergraph partitioning
	private void generateHGraphFixFile(Database db, Workload workload) {
		File fixFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_hGraphFixFile());
		
		Data trData = null;
		
		try {
			fixFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fixFile), "utf-8"));
				
				Set<Integer> uniqueDataSet = new TreeSet<Integer>();
				int newline = 0;
				
				for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
					for(Transaction transaction : entry.getValue()) {
						if(transaction.getTr_class() != "green") {							
							Iterator<Integer> data =  transaction.getTr_dataSet().iterator();							
							
							while(data.hasNext()) {
								trData = db.search(data.next());								
								
								if(!uniqueDataSet.contains(trData.getData_shadowId())) {
									++newline;
									
									if(trData.isData_isMoveable())									
										writer.write(Integer.toString(trData.getData_globalPartitionId()));
									else
										writer.write(Integer.toString(-1));
									
									if(newline != workload.getWrl_totalDataObjects())
										writer.write("\n");										
									
									uniqueDataSet.add(trData.getData_shadowId());
								}
							}
						}
					}					
				}
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}		
	}
	
	// Generates Workload File for Graph partitioning
	private boolean generateGraphWorkloadFile(Database db, Workload workload) throws IOException {
		File workloadFile = new File(DBMSSimulator.METIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_graphWorkloadFile());
		
		Data trData = null;
		Data trInvolvedData = null;
		Set<Integer> dataIdSet = null;
		Set<Integer> dataSet = new TreeSet<Integer>();
		String content = "";
		int edges = 0; // Total number of edges need to be determined
		int vertices = workload.getWrl_totalDataObjects();		
		int hasVertexWeight = 1;
		int hasEdgeWeight = 1;
		int new_line = vertices;
		
		//System.out.println("@ tr edges = "+workload.getWrl_totalTransactions());
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_class() != "green") {
					
					Iterator<Integer> data_id_itr =  transaction.getTr_dataSet().iterator();
					while(data_id_itr.hasNext()) {
						trData = db.search(data_id_itr.next());
											
						if(!dataSet.contains(trData.getData_id())) {
							dataSet.add(trData.getData_id());							
							dataIdSet = new TreeSet<Integer>();
							
							String str = Integer.toString(trData.getData_weight())+" ";							
							//System.out.println("@ "+trData.toString()+" | size = "+trData.getData_transactions_involved().size());							
							
							Set<Integer> trSet = workload.getWrl_dataTransactionsInvolved().get(trData.getData_id());							
							if(trSet.size() != 0) {					
								for(Integer transaction_id : trSet) {
									Transaction tr = workload.getTransaction(transaction_id);
									
									if(tr != null) {
										for(int trInvolvedDataId : tr.getTr_dataSet()) {
											trInvolvedData = db.search(trInvolvedDataId);													
											
											if(!dataIdSet.contains(trInvolvedDataId) && trInvolvedData.getData_id() != trData.getData_id()) {
												str += Integer.toString(trInvolvedData.getData_shadowId())+" ";							
												str += tr.getTr_frequency()+" ";
												
												++edges;
												//System.out.println("@ edges = "+edges);
												dataIdSet.add(trInvolvedData.getData_id());
											}
										}
									} else {
										System.out.println("@ Null transaction has been found !!! "+transaction_id);
									}
								}															
							} else {
								System.out.println("@ No involved transactions !!! "+trData.toString()+" | "+trData.getData_shadowId());
							}
							
							content += StringUtils.stripEnd(str, null);
							
							--new_line;
							
							if(new_line != 0)
								content += "\n";
						}																
					} // end -- while() loop																				
				} // end -- if()-Transaction Class
			} // end -- for()-Transaction
		} // end -- for()-Transaction-Types
		
		//System.out.println("@ final edges = "+edges);
		
		/*if(edges/2 <= 1) {
			System.out.println("[ALM] Only "+edges+" graph edges have been present in the workload");
			System.out.println("[ACT] Simulation will be aborted for this run ...");
			System.out.println("@ edges/2 = "+edges/2);
			return true;
		} else {*/		
			try {
				workloadFile.createNewFile();
				Writer writer = null;			
	
				try {
					writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
					writer.write(vertices+" "+(edges/2)+" "+hasVertexWeight+""+hasEdgeWeight+"\n"+content);
					//writer.write(vertices+" "+(edges/2)+"\n"+content);
				} catch(IOException e) {
					e.printStackTrace();
				}finally {
					writer.close();
				}
			} catch (IOException e) {		
				e.printStackTrace();
			}
			
			//System.out.println("[OUT] Workload file generation for graph partitioning has been completed.");
			return false;
		//}
	}
	
	private int simpleHash(int x, int nums) {
		//System.out.println("@debug >> x = "+x+" nums = "+nums);
		return (x % nums);
	} 
	
	// Generates Workload File for Compressed Hyper-graph partitioning
	private boolean generateCHGraphWorkloadFile(Database db, Workload workload) {
		Map<Integer, Set<Integer>> virtual_vertexMap = new TreeMap<Integer, Set<Integer>>();
		Map<Integer, Integer> virtual_vertexToHashMap = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> virtual_vertexWeightToHashMap = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> virtual_vertexWeightMap = new TreeMap<Integer, Integer>();
		Set<Integer> virtual_vertex_dataSet = null;		
		int virtual_vertex_id = 0;
		
		Data trData = null;
		Set<Integer> dataSet = new TreeSet<Integer>();
		Set<Integer> virtual_dataSet = new TreeSet<Integer>();		
		
		//System.out.println("@debug >> Red = "+workload.getWrl_tr_red()+" | Orange = "+workload.getWrl_tr_orange()
				//+" | div = "+workload.getWrl_totalDataObjects()/2);

		System.out.println("[ACT] Starting workload compression with CR = 0.5 ...");
		System.out.println("[ACT] Creating virtual data objects ...");
		// Creating Virtual Nodes
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_class() != "green") {
					Iterator<Integer> data_id_itr1 =  transaction.getTr_dataSet().iterator();
					while(data_id_itr1.hasNext()) {												
						trData = db.search(data_id_itr1.next());
						
						if(!dataSet.contains(trData.getData_id())) {
							dataSet.add(trData.getData_id());							
							int i = simpleHash(trData.getData_pk(), (workload.getWrl_totalDataObjects()/2));							
							//System.out.println("@ "+trData.toString()+" | hashed at ("+i+")");
							
							if(!virtual_dataSet.contains(i+1)) {
								virtual_dataSet.add(i+1);
								
								virtual_vertex_dataSet = new TreeSet<Integer>();								
								virtual_vertex_dataSet.add(trData.getData_id());								
								
								virtual_vertexMap.put(i+1, virtual_vertex_dataSet);
								virtual_vertexWeightMap.put(i+1, trData.getData_weight());
																
								++virtual_vertex_id;
							} else {																
								virtual_vertexMap.get(i+1).add(trData.getData_id());
								
								int weight = virtual_vertexWeightMap.get(i+1) + trData.getData_weight();
								virtual_vertexWeightMap.remove(i+1);
								virtual_vertexWeightMap.put(i+1, weight);
							}							
														
							virtual_vertexToHashMap.put(virtual_vertex_id, i+1);
							virtual_vertexWeightToHashMap.put(virtual_vertex_id, i+1);
							
							trData.setData_virtual_node_id(virtual_vertex_id);
							
							
							//System.out.println("@ vid = "+virtual_vertex_id+" (i+1) = "+(i+1)+" | "+trData.toString());
							//System.out.println("@debug >> "+trData.toString()+" | hashed at ("+(i+1)+") | vid = "+trData.getData_virtual_node_id());
							//System.out.println(trData.getData_id()+" | "+trData.getData_pk()+" | "+(i+1));
						}
					}
				}
			}
		}
		
		System.out.println("[OUT] Total "+virtual_vertexMap.size()+" virtual data objects have been created.");
		
//====== Check -- Found OK
		/*
		for(Entry<Integer, Set<Integer>> entry : virtual_vertexMap.entrySet()) {
			System.out.print(" v' = "+entry.getKey()+" {");
			
			for(Integer j : entry.getValue()) {
				System.out.print(j+", ");
			}
			
			System.out.print("}\n");
		}
		//Check -- Found OK
		for(Entry<Integer, Integer> entry : virtual_vertexWeightMap.entrySet())
			System.out.println(" v' = "+entry.getKey()+" | weight = "+entry.getValue());
		
		System.out.println("--------------");
		*/
//======
		
		System.out.println("[ACT] Creating virtual transactions ...");
		// Creating Virtual Edges
		Map<Integer, Set<Integer>> virtual_edgeMap = new TreeMap<Integer, Set<Integer>>();
		Map<Integer, Integer> virtual_edgeWeightMap = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> virtual_vertexTracker = new TreeMap<Integer, Integer>();
		Set<Integer> virtual_edge_vertexSet = null;		
		Set<Integer> edgeSet = new TreeSet<Integer>();
		int virtual_edge_id = 1;
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_class() != "green") {	
					
					Iterator<Integer> data_id_itr2 =  transaction.getTr_dataSet().iterator();
					while(data_id_itr2.hasNext()) {												
						trData = db.search(data_id_itr2.next());
							virtual_vertex_id = trData.getData_virtual_node_id();							
							
							// Virtual Edge - Vertices
							if(!edgeSet.contains(virtual_edge_id)) {								
								edgeSet.add(virtual_edge_id);
								
								virtual_edge_vertexSet = new TreeSet<Integer>();
								virtual_edge_vertexSet.add(virtual_vertex_id);
								virtual_edgeMap.put(virtual_edge_id, virtual_edge_vertexSet);
																
								//System.out.println("@debug >> v-eid= "+virtual_edge_id+" | v-vid = "+virtual_vertex_id);
							} else {
								virtual_edgeMap.get(virtual_edge_id).add(virtual_vertex_id);
								//System.out.println("@debug >> v-eid= "+virtual_edge_id+" | v-vid = "+virtual_vertex_id);
							}
							
							virtual_edgeWeightMap.put(virtual_edge_id, 1);
							
							if(!virtual_vertexTracker.containsKey(virtual_vertex_id))
								virtual_vertexTracker.put(virtual_vertex_id, 1);
							else {
								int vertex_frequency = virtual_vertexTracker.get(virtual_vertex_id);
								virtual_vertexTracker.put(virtual_vertex_id, ++vertex_frequency);
							}
					}
				}
				
				++virtual_edge_id;
			}
		}	
		
		System.out.println("[OUT] Total "+virtual_edgeMap.size()+" virtual transactions have been created.");
		
//====== Check -- Found OK
		/*
		for(Entry<Integer, Set<Integer>> entry : virtual_edgeMap.entrySet()) {
			System.out.print(" e' = "+entry.getKey()+"{");
			
			for(Integer j : entry.getValue()) {
				System.out.print(j+", ");
			}
			
			System.out.print("}\n");
		}
		//Check -- Found OK
		for(Entry<Integer, Integer> entry : virtual_edgeWeightMap.entrySet())
			System.out.println(" e' = "+entry.getKey()+" | weight = "+entry.getValue());
		
		System.out.println("--------------");
		*/
//======		
		
		System.out.println("[ACT] Starting (compressed) workload sampling to remove virtual transactions having a single virtual node ...");		
		// Identify the virtual edges which have only one virtual node associated with them
		Set<Integer> removable_virtualEdgeSet = new TreeSet<Integer>();
		for(Entry<Integer, Set<Integer>> entry : virtual_edgeMap.entrySet()) {
			if(entry.getValue().size() == 1)
				removable_virtualEdgeSet.add(entry.getKey());
		}
		
		System.out.println("[OUT] Total "+removable_virtualEdgeSet.size()+" virtual transactions have been removed from the compressed workload.");
		// Remove the virtual edges from the virtual edge map which have been identified in the previous step
		for(Integer i : removable_virtualEdgeSet) {
			// Decrease the frequency of the virtual vertices which were involved with this (ith) virtual edge
			for(Integer j : virtual_edgeMap.get(i)) {
				int vertex_frequency = virtual_vertexTracker.get(j);
				virtual_vertexTracker.put(j, --vertex_frequency);
				//System.out.println("@ Dec freq for "+j+" to "+vertex_frequency);
			}
			
			virtual_edgeMap.remove(i);
			virtual_edgeWeightMap.remove(i);
		}
				
//======Check -- Found OK
		/*
		for(Entry<Integer, Set<Integer>> entry : virtual_edgeMap.entrySet()) {
			System.out.print(" e' = "+entry.getKey()+"{");
			
			for(Integer j : entry.getValue()) {
				System.out.print(j+", ");
			}
			
			System.out.print("}\n");
		}
		//Check -- Found OK
		for(Entry<Integer, Integer> entry : virtual_edgeWeightMap.entrySet())
			System.out.println(" e' = "+entry.getKey()+" | weight = "+entry.getValue());
		
		System.out.println("@ Total "+virtual_edgeMap.size()+" e' are now left.");
		System.out.println("--------------");
		*/
//======
		
		System.out.println("[ACT] Starting workload sampling to remove duplicate virtual transactions ...");
		// Identify the virtual edges which have the same set of virtual nodes associated with them		
		removable_virtualEdgeSet = new TreeSet<Integer>();		
		
		for(Entry<Integer, Set<Integer>> entry : virtual_edgeMap.entrySet()) {
			for(Entry<Integer, Set<Integer>> entry1 : virtual_edgeMap.entrySet()) {
				if(entry.getKey() != entry1.getKey() && !removable_virtualEdgeSet.contains(entry.getKey())) {
					if(entry.getValue().equals(entry1.getValue())) {
						
						removable_virtualEdgeSet.add(entry1.getKey());
					
						//System.out.println("@ >> e' = "+entry.getKey()+" | e* = "+entry1.getKey());
						if(virtual_edgeWeightMap.containsKey(entry.getKey())) {								
							int weight = virtual_edgeWeightMap.get(entry.getKey());							
							virtual_edgeWeightMap.remove(entry.getKey());
							virtual_edgeWeightMap.put(entry.getKey(), ++weight);
						} else {
							virtual_edgeWeightMap.put(entry.getKey(), 1);
						}
					}
				}
			}
		}
		
		System.out.println("[OUT] Total "+removable_virtualEdgeSet.size()+" duplicate virtual transactions have been removed from the workload.");
		// Remove the virtual edges from the virtual edge map which have been identified in the previous step
		for(Integer i : removable_virtualEdgeSet) {
			// Decrease the frequency of the virtual vertices which were involved with this (ith) virtual edge
			for(Integer j : virtual_edgeMap.get(i)) {
				int vertex_frequency = virtual_vertexTracker.get(j);
				virtual_vertexTracker.put(j, --vertex_frequency);
				//System.out.println("> Dec freq for "+j+" to "+vertex_frequency);
			}
			
			virtual_edgeMap.remove(i);
			virtual_edgeWeightMap.remove(i);
		}																			

//======Check -- Found OK
		/*
		for(Entry<Integer, Set<Integer>> entry : virtual_edgeMap.entrySet()) {
			System.out.print(" e' = "+entry.getKey()+"{");
			
			for(Integer j : entry.getValue()) {
				System.out.print(j+", ");
			}
			
			System.out.print("}\n");
		}
		//Check -- Found OK
		for(Entry<Integer, Integer> entry : virtual_edgeWeightMap.entrySet())
			System.out.println(" e' = "+entry.getKey()+" | weight = "+entry.getValue());
		*/
//======	
		
		// Delete the virtual nodes which does not have any appearance in any virtual edge
		System.out.println("[ACT] Adjusting the virtual data object list ... ");//+virtual_vertexTracker.entrySet().size());
		
		int count = 0;
		for(Entry<Integer, Integer> entry : virtual_vertexTracker.entrySet()) {
			if(entry.getValue() == 0) {
				//System.out.println("@ 0 value found for "+entry.getKey());
				virtual_vertexToHashMap.values().remove(entry.getKey());
				virtual_vertexMap.remove(entry.getKey());
				virtual_vertexWeightMap.remove(entry.getKey());
				++count;
			}
		}
		
		if(count != 0)
			System.out.println("[OUT] Total "+virtual_vertexMap.size()+" virtual data objects are now existed after adjustment.");
		else
			System.out.println("[OUT] No adjustments were required.");		
		
		// Creating Compressed Hyper-graph Workload File
		System.out.println("[MSG] Total "+virtual_edgeMap.size()+" virtual transactions have been identified for partitioning.");
		System.out.println("[ACT] Generating workload file for compressed hypergraph partitioning ...");
		
		File workloadFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_chGraphWorkloadFile());
				
		int compressed_hyper_edges = virtual_edgeMap.size();
		int compressed_vertices = virtual_vertexMap.size();
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;						
		
		if(compressed_hyper_edges <= 1) { 
			System.out.println("[ALM] Only "+compressed_hyper_edges+" compressed hyperedges have been present in the compressed workload");
			System.out.println("[ACT] Simulation will be aborted for this run ...");
			return true;
		} else {		
			try {
				workloadFile.createNewFile();
				Writer writer = null;
				try {
					writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
					writer.write(compressed_hyper_edges+" "+compressed_vertices+" "+hasTransactionWeight+""+hasDataWeight+"\n");
					
					for(Entry<Integer, Set<Integer>> entry : virtual_edgeMap.entrySet()) {
						writer.write(virtual_edgeWeightMap.get(entry.getKey())+" ");
								
						Iterator<Integer> virtual_id_itr =  entry.getValue().iterator();
						while(virtual_id_itr.hasNext()) {						
							writer.write(Integer.toString(virtual_id_itr.next()));							
							
							if(virtual_id_itr.hasNext())
								writer.write(" "); 
						}
						
						writer.write("\n");		
					}
	
					// Writing Data Weight
					int newline = 0;
					
					for(Entry<Integer, Integer> entry : virtual_vertexWeightMap.entrySet()) {
							//writer.write(Integer.toString(entry.getValue()));
							writer.write(Integer.toString(1));
							
							if(newline != (virtual_vertexWeightMap.size()-1))
								writer.write("\n");
							
							++newline;
					}
					
				} catch(IOException e) {
					e.printStackTrace();
				}finally {
					writer.close();
				}
			} catch (IOException e) {		
				e.printStackTrace();
			}	
			
			System.out.println("[OUT] Workload file generation for compressed hypergraph partitioning has been completed.");
			return false;
		}
	}
	
	// Generate Fix File for Compressed Hyper-graph partitioning 
	private void generateCHGraphFixFile(Database db, Workload workload) {
		File fixFile = new File(DBMSSimulator.hMETIS_DIR_LOCATION+"\\"
				+workload.getWrl_id()+"-"+db.getDb_name()+"-"+workload.getWrl_chGraphFixFile());
		
		Data trData = null;
		
		try {
			fixFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fixFile), "utf-8"));
				
				Set<Integer> uniqueDataSet = new TreeSet<Integer>();
				int newline = 0;
				
				for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
					for(Transaction transaction : entry.getValue()) {
						if(transaction.getTr_class() != "green") {							
							Iterator<Integer> data =  transaction.getTr_dataSet().iterator();							
							
							while(data.hasNext()) {
								trData = db.search(data.next());								
								
								if(!uniqueDataSet.contains(trData.getData_shadowId())) {
									++newline;
									
									if(trData.isData_isMoveable())									
										writer.write(Integer.toString(trData.getData_globalPartitionId()));
									else
										writer.write(Integer.toString(-1));
									
									if(newline != workload.getWrl_totalDataObjects())
										writer.write("\n");										
									
									uniqueDataSet.add(trData.getData_shadowId());
								}
							}
						}
					}					
				}
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}		
	}
}