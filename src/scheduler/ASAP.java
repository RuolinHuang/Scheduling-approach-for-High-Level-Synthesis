package scheduler;

import java.util.HashMap;
import java.util.Map;

public class ASAP extends Scheduler {
	
	public Schedule schedule(final Graph sg) {
		Map<Node, Interval> queue = new HashMap<Node, Interval>();
		Map<Node, Interval> qq;
		Map<Node, Interval> minslot = new HashMap<Node, Interval>();
		Schedule schedule = new Schedule();
	
		Graph g = sg;
		
		//add all node in the map
		for (Node nd : g) {
			if (nd.root())	//if the 
				queue.put(nd, new Interval(0, nd.getDelay() - 1)); //the time which the node start and finish
		}
		
		//check the finished map
		if(queue.size() == 0)
			System.out.println("No root in Graph found. Empty or cyclic graph");

		while (queue.size() > 0) {
			qq = new HashMap<Node, Interval>();

			for (Node nd : queue.keySet()) {	//get all nodes
				Interval slot = queue.get(nd);	//get interval
				schedule.add(nd, slot);		//schedule the nodes
	
				for (Node l : nd.successors()) { 	//l is successor of nd
					g.handle(nd, l);	//what does it mean?
	
					Interval ii = minslot.get(l);
					if (ii == null)
						minslot.put(l, new Interval(slot.ubound + 1,
									       slot.ubound + l.getDelay()));
					else if (ii.lbound.compareTo(slot.ubound) <= 0)		//a<=b node1 start first than node 3
						minslot.put(l, new Interval(slot.ubound + 1,
									       slot.ubound + l.getDelay()));

					if (!l.top())
						continue;
					ii = minslot.get(l);
					if ((queue.get(l) == null)) {
						if (qq.get(l) == null)
							qq.put(l, ii);
						else if (qq.get(l).lbound <= slot.ubound)
							qq.put(l, ii);
					} else if (queue.get(l).lbound <= slot.ubound)
						qq.put(l, ii);
				}
			}
			queue = qq;
		}
		g.reset();
	
		return schedule;
	}
}
