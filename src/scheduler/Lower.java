package scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Lower extends PartialScheduler {
	
	private Integer latency_estimate;

	// 1. Integer: Time step, 2. Integer: group, 3. The amount of resources of that group
	private	Map<Integer, Map<Integer, Integer>> res_count = new HashMap<Integer, Map<Integer, Integer>>();
	
	//Generate resources type
	private	Map<Integer, Set<String>> res_available = new HashMap<Integer, Set<String>>();
	// 1. Integer: group, Integer: number of resources belongs to this group.
	private Map<RT, Integer> type_group_mapping = new HashMap<RT, Integer>();
			
	
	public Lower(Map<Integer, Set<String>> res_available, Map<RT, Integer> type_group_mapping) {
		this.res_available = res_available;
		this.type_group_mapping = type_group_mapping;
		this.latency_estimate = 0;
	}
	
	@Override
	public Schedule schedule(final Graph sg, final RC rc, final Schedule sched, final Integer curr_node_number, final Schedule asap, final Schedule alap) { 		


		Schedule schedule = new Schedule();
		schedule = sched.clone();
		int init_curr_node = curr_node_number;
		
		//Check the resources used with respect to partial schedule sched
		if(curr_node_number != 0) {
			for (Node nd : sched.nodes()) {
				// Get the time slot of the node inside the schedule 
				Interval ii = sched.slot(nd);
				int group = type_group_mapping.get(nd.getRT());
				for(int k = ii.lbound.intValue(); k <= ii.ubound.intValue(); k++) {
					if(res_count.containsKey(k)) {
						//Update the coressponding group by 1 
						res_count.get(k).put(group, res_count.get(k).get(group)  + 1);
						
					}
					else {
						Map<Integer, Integer> res_count_temp = new HashMap<Integer, Integer>();
						for(Integer i : res_available.keySet()) {
							if(i.equals(type_group_mapping.get(nd.getRT()))) {
								res_count_temp.put(i, 1);
							}
							else
								res_count_temp.put(i, 0);
						}
						res_count.put(k, res_count_temp);
					}
						
				}
				
			}
		}
		
		//Check the resources used with respect to ASAP
		for (Node nd : asap.nodes()) {
			if(nd.returnnumber() > curr_node_number){
				Interval ii = asap.slot(nd);
				int group = type_group_mapping.get(nd.getRT());
				for(int k = ii.lbound.intValue(); k <= ii.ubound.intValue(); k++) {
					if(res_count.containsKey(k)) {
						//Update the coressponding group by 1 
						res_count.get(k).put(group, res_count.get(k).get(group)  + 1);
						
					}
					else {
						Map<Integer, Integer> res_count_temp = new HashMap<Integer, Integer>();
						for(Integer i : res_available.keySet()) {
							if(i.equals(type_group_mapping.get(nd.getRT()))) {
								res_count_temp.put(i, 1);
							}
							else
								res_count_temp.put(i, 0);
						}
						res_count.put(k, res_count_temp);
					}
						
				}
				
			}
		}
				
		//Here should check the size is from 0 or 1 
		for(int i = curr_node_number + 1; i <= sg.size(); i++) {
			
			Node curr_nd = sg.get_node(i);	//get node from the graph from a Treemap
		
			Integer curr_lbound = asap.slot(curr_nd).lbound;	//k<--ASAP(xi)
			Integer group = type_group_mapping.get(curr_nd.getRT());
			
			//Remove the resources used of the current node in ASAP to move the node.
			for(int step = asap.slot(curr_nd).lbound; step <= asap.slot(curr_nd).ubound; step++){
				res_count.get(step).put(group, res_count.get(step).get(group)  - 1);
			}
			
			while(true) {
				int available = 0;
				for(int k = curr_lbound; k < curr_lbound + curr_nd.getDelay(); k++) {
					available = 0;
					if(!res_count.containsKey(k)) {
						
					}
					else if(res_count.get(k).get(group) >= res_available.get(group).size()) {
						break;
					}
					available = 1;
				}
				if(available == 1)
					break;
				else curr_lbound++;
			}
				
				
			//After the previous loop, we have the start time step for curr_nd
			//Now assign resource to operator -> Here the simple method is implemented 
			//Later can improve by considering the weight between resources
			Interval ii = new Interval(curr_lbound,curr_lbound + curr_nd.getDelay()-1);
			schedule.add(curr_nd, ii);
			for(int k = curr_lbound; k < curr_lbound + curr_nd.getDelay(); k++) {
				//New version
				if(res_count.containsKey(k)) {
					//Update the coressponding group by 1 
					res_count.get(k).put(group, res_count.get(k).get(group)  + 1);
					
				}
				else {
					Map<Integer, Integer> res_count_temp = new HashMap<Integer, Integer>();
					for(Integer gr : res_available.keySet()) {
						if(gr.equals(type_group_mapping.get(curr_nd.getRT()))) {
							res_count_temp.put(gr, 1);
						}
						else
							res_count_temp.put(gr, 0);
					}
					res_count.put(k, res_count_temp);
				}
			}
		
		}
		
		//Optimize the lower-bound forward
		for(int i = init_curr_node + 1; i <= sg.size(); i++) {
			Node curr_nd = sg.get_node(i);
			Integer group = type_group_mapping.get(curr_nd.getRT());
			Integer curr_lbound = asap.slot(curr_nd).lbound;
			
			for(int step = schedule.slot(curr_nd).lbound; step <= schedule.slot(curr_nd).ubound; step++){
				res_count.get(step).put(group, res_count.get(step).get(group)  - 1);
			}
	
			for(int k = curr_lbound; k <= schedule.max(); k++) {
				int available = 0;
				int gap_detect = 0;
				int break_all = 0;
				for(int j = k; j < k + curr_nd.getDelay(); j++) {
					available = 0;
					if(!res_count.containsKey(j)) {
						
					}
					else if(res_count.get(j).get(group) >= res_available.get(group).size()) {
						
						if(gap_detect != 0 && (j - gap_detect) < schedule.slot(curr_nd).lbound) {
							//Get all the nodes available at time step j to chose candidate for change position
							//Find a node has the same type, but has larger ASAP value
							for(Node conflict_nd : schedule.nodes(j)) {
								if(asap.slot(conflict_nd).lbound > curr_lbound && conflict_nd.getRT().equals(curr_nd.getRT())) {
									//Change position
									//Remove resources used of conflict_nd
									for(int step = schedule.slot(conflict_nd).lbound; step <= schedule.slot(conflict_nd).ubound; step++){
										res_count.get(step).put(group, res_count.get(step).get(group)  - 1);
									}
									//Copy the confict_node to the position of current node
									Interval ii = new Interval(schedule.slot(curr_nd).lbound, schedule.slot(curr_nd).lbound + conflict_nd.getDelay()-1);
									schedule.add(conflict_nd, ii);
									//Update resources used for conflict node
									for(int step = schedule.slot(conflict_nd).lbound; step <= schedule.slot(conflict_nd).ubound; step++){
										res_count.get(step).put(group, res_count.get(step).get(group)  + 1);
									}
									//Replace current node at the first step, where the gap is detected
									curr_lbound = j - gap_detect;
									ii = new Interval(curr_lbound,curr_lbound + curr_nd.getDelay()-1);
									schedule.add(curr_nd, ii);
									break_all = 1;
									break;
								}
							}
						}
						
						break;
					}
					gap_detect++;
					available = 1;
				}
				
				if(break_all == 1) break;
				
				if(available == 1 && k < schedule.slot(curr_nd).lbound){
					curr_lbound = k;
					Interval ii = new Interval(curr_lbound,curr_lbound + curr_nd.getDelay()-1);
					schedule.add(curr_nd, ii);
					break;
				}
			}
			
			for(int k = schedule.slot(curr_nd).lbound; k <= schedule.slot(curr_nd).ubound; k++) { 
				if(res_count.containsKey(k)) {
                    //Update the coressponding group by 1 
                    res_count.get(k).put(group, res_count.get(k).get(group)  + 1);

                }
                else {
                    Map<Integer, Integer> res_count_temp = new HashMap<Integer, Integer>();
                    for(Integer gr : res_available.keySet()) {
                        if(gr.equals(type_group_mapping.get(curr_nd.getRT()))) {
                            res_count_temp.put(gr, 1);
                        }
                        else
                            res_count_temp.put(gr, 0);
                    }
                    res_count.put(k, res_count_temp);
                }
			}
			
		}
		
		return schedule;
	}
	public Integer getLatency() {
		return latency_estimate;
	}
}
