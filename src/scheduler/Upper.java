package scheduler;

import java.util.*;
	
public class Upper extends PartialScheduler {
	private	Map<Integer, Map<Integer, Integer>> res_count = new HashMap<Integer, Map<Integer, Integer>>();
	
	//Generate resources type
	private	Map<Integer, Set<String>> res_available = new HashMap<Integer, Set<String>>();
			// 1. Integer: group, Integer: number of resources belongs to this group.
	private Map<RT, Integer> type_group_mapping = new HashMap<RT, Integer>();
			
	
	public Upper(Map<Integer, Set<String>> res_available, Map<RT, Integer> type_group_mapping) {
		this.res_available = res_available;
		this.type_group_mapping = type_group_mapping;
	}
	
	
	public Schedule schedule(final Graph sg, final RC rc, final Schedule sched, final Integer curr_node_number, final Schedule asap, final Schedule alap){
		

		Schedule schedule = sched.clone();
	
		
		if(curr_node_number != 0) {
			for (Node nd : sched.nodes()) {
				Interval ii = sched.slot(nd);
				for(int k = ii.lbound.intValue(); k <= ii.ubound.intValue(); k++) {
					if(res_count.containsKey(k)) {
						res_count.get(k).put(type_group_mapping.get(nd.getRT()), res_count.get(k).get(type_group_mapping.get(nd.getRT()))  + 1);
						
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
			
			Node curr_nd = sg.get_node(i);
			
			Integer curr_lbound = asap.slot(curr_nd).lbound;
			
			//When schedule a node, all its predecessors must be already finished
			//Check if lbound of the current node is larger than ubound of its predecessors
		
			if(!curr_nd.root()) {
				for(Node pre_nd : curr_nd.predecessors()) {
					while(curr_lbound <= schedule.slot(pre_nd).ubound){
						//Increase by 1 to avoid curr_lbound > pre_ubound
						curr_lbound = curr_lbound + 1;
					}
				}
			}
			//Now we will check if the resource for this node is available at this
			//time step. If not -> move to the next time step.
			Integer group = type_group_mapping.get(curr_nd.getRT());
			
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
		return schedule;
	}
}
