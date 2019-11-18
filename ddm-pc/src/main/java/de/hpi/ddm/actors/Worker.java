package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang3.SerializationUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.HashSet;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class TaskMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private String[] line;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HashMessage implements Serializable {
		private static final long serialVersionUID = 6560207097470814017L;
		private String character;
		//private List<String> passwordChars;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class PasswordChars implements Serializable {
		private static final long serialVersionUID = 8163558040091664272L;
		private List<String> passwordChars;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintsHashesMessage implements Serializable {
		private static final long serialVersionUID = -7425329313563534330L;
		private List<String> allHints;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CrackedHintsMessage implements Serializable {
		private static final long serialVersionUID = -719107220168245254L;
		private Hashtable<String,String> crackedHints;
	}


	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	private List<String> hintsHashes;
	private Hashtable<String,String> crackedHints;
	private List<String> passwordChars;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
		this.hintsHashes = new ArrayList<String>();
		this.crackedHints = new Hashtable<String,String>();
		this.passwordChars = new ArrayList<String>();
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(TaskMessage.class, this::handle)
				.match(HashMessage.class, this::handle)
				.match(PasswordChars.class, this::handle)
				.match(HintsHashesMessage.class, this::handle)
				.match(CrackedHintsMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}
	
	private void handle(PasswordChars message) {
		this.passwordChars = new ArrayList<String>(message.getPasswordChars());
	}

	private void handle(HashMessage message) {
		String cc = message.getCharacter();
		this.log().info("Hashing without letter " + cc);
		//List<String> passwordChars = new ArrayList<String>(message.getPasswordChars());
		List<String> passwordChars = this.passwordChars;
		this.log().info("PasswordChars: " + passwordChars.toString());
		char[] tmpChars = new char[passwordChars.size()-1]; // heapPermutations needs a char array
		int ii = 0;
		for (String ct : passwordChars) {
			if (!ct.equals(cc)) {
				tmpChars[ii] = ct.toCharArray()[0]; // One letter String to char
				ii++;
			}
		}
		List<String> permutations = new ArrayList<String>();
		this.heapPermutation(tmpChars, tmpChars.length, permutations); // permutations returned by reference

		String phash = new String("");
		Hashtable<String,String> output = new Hashtable<String,String>();
		ii=0;
		for (String perm : permutations) {
			phash = this.hash(perm);
			if (this.hintsHashes.contains(phash)) {
				//this.log().info("Matching hashes: " + phash + " for " + perm);
				output.put(phash,perm);
			}
			
		}
		this.sender().tell(new Master.HintMessage(output), this.self());
	}
	
	private void handle(HintsHashesMessage message) {
		//this.hintsHashes = message.getAllHints();
		List<String> tmp = new ArrayList<String>(message.getAllHints());
		//this.log().info("Received hashes " + tmp.toString());
		for (String ee : tmp) {
			this.hintsHashes.add(ee);
		}
	}
	
	private void handle(CrackedHintsMessage message) {
		//this.log().info("Worker receiving cracked hints");
		//this.crackedHints = message.getCrackedHints();
		Hashtable<String,String> tmpCrackedHints = message.getCrackedHints();
		for (String key : tmpCrackedHints.keySet()) {
			this.crackedHints.put(key, tmpCrackedHints.get(key));
		}
	}
	
	private void handle(TaskMessage message) {
		String[] task = message.getLine();
		int id = Integer.parseInt(task[0]);
		String name = task[1];
		char[] passwordChars = task[2].toCharArray();
		int passwordLength = Integer.parseInt(task[3]);
		String passwordHash = task[4];
		
		//this.log().info("Cracking " + name + ": " + passwordHash);
		
		List<String> hintsHashes = new ArrayList<String>();
		for (int ii=5; ii < task.length; ii++) {
			hintsHashes.add(task[ii]);
			//this.log().info(task[ii]);
		}
		HashSet<Character> notLetters = new HashSet<Character>();
		for (String hh : hintsHashes) {
			if (this.crackedHints.containsKey(hh)) {
				notLetters.add(this.findMissingLetter(passwordChars, this.crackedHints.get(hh).toCharArray()));
			}
		}
		
		// Here be cracking!
		// With a subset of letters only
		String finalLetters = new String("");
		String phash = new String("");
		for (char cc : passwordChars) {
			if (!notLetters.contains(cc)) {
				finalLetters += cc;
			}
		}
		//this.log().info("Removed from hint letters " + name + " : " + notLetters);
		//this.log().info("Final letters " + name + " : " + finalLetters);
		if (finalLetters.length() > 3) {
			String[] output = {name, "crashed"};
			this.sender().tell(new Master.ResultMessage(output), this.self());
			return;
		}
		for (String candidate : this.generateAllKLength(finalLetters.toCharArray(), passwordLength)) {
			phash = this.hash(candidate);
			if (phash.equals(passwordHash)) {
				String[] output = {name, candidate};
				this.sender().tell(new Master.ResultMessage(output), this.self());
				return;
			}
		}
		
		
		String[] output = {name, "randomPassword"};
		this.sender().tell(new Master.ResultMessage(output), this.self());
	}
	
	private char findMissingLetter(char[] alphabet, char[] target) {
		boolean found = false;
		for (char ii : alphabet) {
			for (char jj : target) {
				if (ii == jj) {
					found = true;
					break;
				}
			}
			if (!found) {
				return(ii);
			}
			found = false;
		}
		return "".charAt(0); // Buggy but shouldn't be called anyway
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}
	
	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
	

	// https://www.geeksforgeeks.org/print-all-combinations-of-given-length/
	private List<String> generateAllKLength(char[] set, int k) 
	{ 
	    int n = set.length;  
	    return generateAllKLengthRec(set, "", n, k); 
	}
	
	private List<String> generateAllKLengthRec(char[] set, String prefix, int n, int k) { 
		// Base case: k is 0,
		// print prefix 
		List<String> output= new ArrayList<String>();
		if (k == 0) {
			output.add(prefix);
			return output;
		}
		// One by one add all characters  
		// from set and recursively  
		// call for k equals to k-1
		for (int i = 0; i < n; ++i) { 
			// Next character of input added 
			String newPrefix = prefix + set[i];  
			// k is decreased, because  
			// we have added a new character 
			output.addAll(this.generateAllKLengthRec(set, newPrefix, n, k - 1));  
		}
		return(output);
	}
	
	
}