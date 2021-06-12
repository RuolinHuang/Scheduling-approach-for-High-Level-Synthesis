package scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Main {

	public static void main(String[] args) {
		//running time
		
		RC rc = null;
		if (args.length>1){
			rc = new RC();
			rc.parse(args[1]);
		}
		
		Dot_reader dr = new Dot_reader(false);
		if (args.length < 1) {
			System.err.printf("Usage: scheduler dotfile%n");
			System.exit(-1);
		}else {
			System.out.println("Scheduling "+args[0]);
			System.out.println();
		}
		
		Graph g = dr.parse(args[0]);
		
		long startTime=System.currentTimeMillis();
		Scheduler s = new ASAP();
		Schedule sched = s.schedule(g);
		s = new ALAP();
		Schedule alap = s.schedule(g);	

		Numbering  number = new Numbering();
		g = number.Numbering(g, alap, sched);
		
		s = new ASAP();
		Schedule asap = s.schedule(g);
		
		//asap.draw("schedules/ASAP_" + args[0].substring(args[0].lastIndexOf("/")+1));
		
		
		//1: group, 2: Set resources belong to that group.
		Map<Integer, Set<String>> res_available = new HashMap<Integer, Set<String>>();
		// 1. RT: Operator type, 2. Integer: group
		Map<RT, Integer> type_group_mapping = new HashMap<RT, Integer>();
		
		int group = 0;
		
		for(String res : rc.getAllRes().keySet()) {
			if(res_available.isEmpty()) {
				Set<String> temp = new HashSet<String>();
				temp.add(res);
				res_available.put(0, temp);
				group++;
			}
			else {
				int group_temp = group;
				int set_found = 0;
				for(int i = 0; i < group_temp; i++) {
					RT rt = rc.getAllRes().get(res).iterator().next();
					if(rc.getAllRes().get(res_available.get(i).iterator().next()).contains(rt)){
							res_available.get(i).add(res);
							set_found = 1;
							break;
					}
				}
				if(set_found == 0) {
					Set<String> temp = new HashSet<String>();
					temp.add(res);
					res_available.put(group, temp);
					group++;
				}
				set_found = 0;
			}
		}
		
		for(Integer i : res_available.keySet()) {
			for(RT rt : rc.getAllRes().get(res_available.get(i).iterator().next())) {
				if(!type_group_mapping.containsKey(rt))
					type_group_mapping.put(rt,i);
			}
			
		}
		
		Upper ListScheduling = new Upper(res_available, type_group_mapping);
		Schedule List_schedule = new Schedule();
		Schedule ListSchedule = ListScheduling.schedule(g, rc,  List_schedule, 0, asap, alap);
		Integer L_max = ListSchedule.max()+1;
		
		System.out.printf("List scheduling latency: %d \n",L_max);
		
		s = new ALAP(L_max);
		Schedule alap_new = s.schedule(g);
 		//alap.draw("schedules/ALAP_" + args[0].substring(args[0].lastIndexOf("/")+1));
		Schedule init_schedule = new Schedule();
		Enumerate bulb = new Enumerate(alap, asap, res_available, type_group_mapping, startTime);
		
		
		Schedule result = bulb.schedule(g, rc, init_schedule, 1, asap, alap_new);
		long endTime=System.currentTimeMillis();
		System.out.println("running time： "+(endTime-startTime)+"\n ms"); 
		System.out.println("best latency： " + (result.max()+1));
		System.out.printf("the number of itegrations %d \n",bulb.getIteration());
		//result.draw("schedules/BULB_" + args[0].substring(args[0].lastIndexOf("/")+1));
		
		
		
	}
}
