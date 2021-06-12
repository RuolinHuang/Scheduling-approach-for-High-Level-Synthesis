package scheduler;

public abstract class PartialScheduler {	
	/**
	 * Use the graph given to create a schedule. 
	 * @param sg - the dependency graph
	 * @return a schedule for the given graph
	 */
	public abstract Schedule schedule(final Graph sg, final RC rc, final Schedule sched, final Integer curr_node_number, Schedule asap, Schedule alap);
}
