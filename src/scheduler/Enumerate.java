package scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Enumerate extends PartialScheduler{
	private Schedule best_schedule;
	private Integer iteration;
	private int best_latency;
	private Schedule asap_update;
	private Schedule init_asap;
	private long start_time;

	//1: group, 2: Set resources belong to that group.
	private	Map<Integer, Set<String>> res_available = new HashMap<Integer, Set<String>>();
	// 1. Integer: group, Integer: number of resources belongs to this group.
	private Map<RT, Integer> type_group_mapping = new HashMap<RT, Integer>();		
	//1. Integer: time step, Map: 1. Integer: group, 2. Integer: the number of the operators in the group
	private	Map<Integer, Map<Integer, Integer>> res_count = new HashMap<Integer, Map<Integer, Integer>>();
	
	public Enumerate(Schedule alap, Schedule asap, Map<Integer, Set<String>> res_available, Map<RT, Integer> type_group_mapping, long start_time) {
		best_schedule = new Schedule();

		best_schedule = alap;
		best_latency = Integer.MAX_VALUE;
		this.res_available = res_available;
		this.type_group_mapping = type_group_mapping;
		this.iteration = 0;
		this.asap_update = new Schedule();
		this.init_asap = asap;
		this.start_time = start_time;
	}
	@Override
	public Schedule schedule(Graph sg, RC rc, Schedule sched, Integer curr_node_number, Schedule asap, Schedule alap){
		
		Schedule schedule = sched.clone();
		
		if(curr_node_number == sg.size()+1) {
		
			if(best_latency > schedule.max()) {
				best_schedule = schedule;
			}
		}
		else {
			
			Node curr_nd = sg.get_node(curr_node_number);
			for(int step = asap.slot(curr_nd).lbound; step <= alap.slot(curr_nd).lbound; step++) {
				Schedule saved_asap = asap.clone_asap();  

				Integer group = type_group_mapping.get(curr_nd.getRT());
				int resources_available = 0;
				for(int k = step; k < step + curr_nd.getDelay(); k++) {
					resources_available = 0;
					if(!res_count.containsKey(k)) {
						//If the time step are not in res_count then this time step is free
					}
					else if(res_count.get(k).get(group) >= res_available.get(group).size()) {
						break;
					}
					resources_available = 1;
				}
				//If the resouces_available == 1, the the resources is available  
				if(resources_available == 1) {
					
					Lower lower_bound_estimate = new Lower(res_available, type_group_mapping);
					Schedule s_lower = lower_bound_estimate.schedule(sg, rc,  sched,  curr_node_number - 1, asap, alap);
					Integer l = s_lower.max()+1;
					
					Upper upper_bound_estimate = new Upper(res_available, type_group_mapping);
					Schedule s_upper = upper_bound_estimate.schedule(sg, rc,  sched,  curr_node_number - 1, asap, alap);
					//Because latency in the schedule count from 0, but we need the result count from 1, so we add 1 
					Integer u = s_upper.max()+1;
					
					if(u < best_latency) {
						best_schedule = s_upper.clone();
						best_latency = u;
					}
					long endTime=System.currentTimeMillis();
					if(endTime-start_time > 300000) {	//120000
						System.out.println("running time： "+(endTime-start_time)+"ms");
						System.out.printf("the number of itegrations %d \n", iteration);
						System.out.printf("best latency: %d \n", best_latency);
						System.exit(0);
					}
					
					
					if(l < best_latency) {
						//Schedule the current node to the schedule S(xi)
						Interval ii = new Interval(step, step + curr_nd.getDelay() - 1);
						schedule.add(curr_nd, ii);
						
						//increase ResourceUsed()
						for(int k = step; k < step + curr_nd.getDelay(); k++) {
							if(res_count.containsKey(k)) {
								//Increase the resources used of this node at this time step
								res_count.get(k).put(group, res_count.get(k).get(group)  + 1);
							}
							else {
								//Generate the Map<group, number of resources used> for all group
								// and add to time step k
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
	
						asap_update = asap.clone_asap();
						//ASAP update should be written here and call enumerate again.
						updateASAP(curr_nd, schedule.slot(curr_nd).lbound, saved_asap, sg);
						Schedule asap_new = asap_update.clone_asap();
						
						iteration++;
						
						// Put new updated ASAP to the next function call
						schedule(sg, rc, schedule, curr_node_number+1, asap_new, alap);
						
						//Decrement ResourceUsed()
						for(int i = step; i < step + curr_nd.getDelay(); i++){
							res_count.get(i).put(type_group_mapping.get(curr_nd.getRT()), res_count.get(i).get(type_group_mapping.get(curr_nd.getRT()))  - 1);
						}
						// Delete the schedule of the current node to try new position 
						schedule.remove(curr_nd);
					}
				}
				
				asap = saved_asap.clone_asap();
			}
		}
		
		
		return best_schedule;
	}
	// Current node: xi, nd:xj
	private void updateASAP(Node curr_node, Integer step, Schedule asap_ref, Graph sg) {
		for( int i = curr_node.returnnumber() + 1; i <= sg.size(); i++ ) {
			// nd is the node j as in the paper
			Node nd = sg.get_node(i);
			//If nd is the successor of current node
			if(curr_node.successors().contains(nd)) {
				//Update the new position of nd if data dependency is wrong
				//The slot().lbound is not clear -> should be rethink again 
				int lowerbound = Math.max(asap_update.slot(nd).lbound, step + curr_node.getDelay());
				
				Interval ii = new Interval(lowerbound, lowerbound+nd.getDelay()-1);
				asap_update.add(nd, ii);
				asap_update.draw("schedules/updateASAP_2.dot");
			}
		}
	}
	
	public Integer getIteration() {
		return iteration;
	}
}
