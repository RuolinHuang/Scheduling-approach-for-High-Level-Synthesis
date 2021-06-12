package scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

//1. timeslot 2. critical path
public class Numbering {

	
	public Graph Numbering(final Graph sg, Schedule sched_alap, Schedule sched_asap)
	{
		HashMap<Integer, Node> New_oder1 = new HashMap<>();
		TreeMap<Integer, Set<Node>> New_oder2 = new TreeMap<>();
		Graph g = sg;
		
		int slot = 0, count=1;
		int critical_path=0;
		while(slot<=sched_alap.max()) {
		if (sched_alap.nodes(slot)!=null)
		{
			Set<Node> Node_set = sched_alap.nodes(slot);
			for(Node ng : Node_set)
			{
				critical_path = Integer.MAX_VALUE - sched_asap.slot(ng).lbound;//g.critical_path(ng);
				//critical_path = Integer.MAX_VALUE - g.critical_path(ng);
				if(New_oder2.get(critical_path)==null)	//avoid the same key
				{
					//sorting automatically
					Set<Node> Same_number = new HashSet<Node>();	//create new set
					Same_number.add(ng);
					New_oder2.put(critical_path, Same_number);		
				}
				else
				{
					New_oder2.get(critical_path).add(ng);	//add new node to the set
				}
			//sorting automatically!!
			}
			while(New_oder2.isEmpty()!=true)
			{		
				Set<Node> Same_number = New_oder2.pollFirstEntry().getValue();	//return the first entry and delete it	
				
				Iterator<Node> i = Same_number.iterator();
				while(i.hasNext())
				{
					Node temp = i.next();
					if(g.get(temp).returnnumber()==0)
					{
						g.get(temp).addnumber(count);		//find this node in graph, then add number to this node
						g.add_numbering(count, temp);
						count++;
					}
				}
				//count++;
			}
		}
		slot++;	//node for each slot
		}
		return g;
	}
	
	/*
	public int critical_path(Node node)
	{
		int critical_path = 0;
		while(node.allSuccessors()!=null)
		{
			HashMap<Node, Integer> map_successor = node.allSuccessors();
			if(map_successor.keySet().iterator().hasNext()==true)
			{
				Node successor = map_successor.keySet().iterator().next();
				critical_path++;	//longest path, smallest number, for the sorting in Tree
				// map_successor.get(successor);	//+=edge weight
				node = successor;	//go to the next node
			}
			else
				break;
		}	
		return critical_path;
	}
	*/
}


