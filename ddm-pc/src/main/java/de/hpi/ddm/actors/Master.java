package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Hashtable;
import java.util.HashSet;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.configuration.ConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "master";

	public static Props props(final ActorRef reader, final ActorRef collector) {
		return Props.create(Master.class, () -> new Master(reader, collector));
	}

	public Master(final ActorRef reader, final ActorRef collector) {
		this.reader = reader;
		this.collector = collector;
		this.workers = new ArrayList<>();
		this.freeWorkers = new ArrayList<>();

		this.toCrack = new ArrayList<String[]>();
		this.allHints = new HashSet<String>();
		//this.allHints = new ArrayList<String>();
	}

	////////////////////
	// Actor Messages //
	////////////////////

	@Data
	public static class StartMessage implements Serializable {
		private static final long serialVersionUID = -50374816448627600L;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class ResultMessage implements Serializable {
		private static final long serialVersionUID = -4884396984570239244L;
		private String[] result;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BatchMessage implements Serializable {
		private static final long serialVersionUID = 8343040942748609598L;
		private List<String[]> lines;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class HintMessage implements Serializable {
		private static final long serialVersionUID = -822966750270011344L;
		private Hashtable<String, String> crackedHints;
	}

	@Data
	public static class RegistrationMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef reader;
	private final ActorRef collector;
	private final List<ActorRef> workers;
	private final List<ActorRef> freeWorkers;

	private long startTime;
	private List<String[]> toCrack;
	private HashSet<String> allHints;
//	private List<String> allHints;
	private Hashtable<String, String> crackedHints;
	private List<String> passwordChars;
	private List<String> toProcessChars;
	private int toProcessID;
	private boolean dataLoaded;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(StartMessage.class, this::handle)
				.match(BatchMessage.class, this::handle)
				.match(ResultMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.match(HintMessage.class, this::handle)
				.match(RegistrationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	protected void handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();
		
		this.dataLoaded = false;
		this.passwordChars = new ArrayList<String>();
		this.toProcessChars = new ArrayList<String>();
		this.toProcessID = 0;
		
		this.crackedHints = new Hashtable<String, String>();
		this.allHints = new HashSet<String>();
		this.toCrack = new ArrayList<String[]>();
		
		this.reader.tell(new Reader.ReadMessage(), this.self()); // Start the reader
	}
	
	protected void handle(BatchMessage message) {
		// Load all password and hints
		// Ask the workers to crack the hashes once all the data are loaded
		
		if (message.getLines().isEmpty()) {
			this.dataLoaded = true;
			this.distribute();
			return;
		}
		
		for (String[] line : message.getLines()) {
			toCrack.add(line);
			for (int ii=5; ii < line.length; ii++) {
				allHints.add(line[ii]);
			}
		}
		// This only need to be done once but has a negligible cost so...
		this.passwordChars = Arrays.asList(toCrack.get(0)[2].split(""));
		this.toProcessChars = Arrays.asList(toCrack.get(0)[2].split(""));
		
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void distribute() {
		// Main "thought" routine of the master, distribute the jobs and controls that intermediate steps are completed
		List<ActorRef> notFree = new ArrayList<ActorRef>();
		for (ActorRef worker : this.freeWorkers) {
			if (!this.dataLoaded) {
				return;
			} else if (this.dataLoaded && this.toProcessID < this.passwordChars.size()) {
				// Crack the hints first
				String nextChar = this.toProcessChars.get(this.toProcessID);
				this.toProcessID++;
				this.sendHintsHashes(worker);
				/* Convert the List<String> (one character strings) into a String to send to the workers, as the original List<String>
				cannot be deserialized by Kryos for unknown reasons*/
				String post = new String("");
				for (String cc: this.passwordChars) {
					post += cc;
				}
				worker.tell(new Worker.PasswordCharsMessage(post), this.self());
				worker.tell(new Worker.HashMessage(nextChar), this.self());
				notFree.add(worker);
			} else if (!this.toCrack.isEmpty() && this.crackedHints.size() == this.allHints.size()) {
				// Crack the passwords once the hints have been cracked
				this.sendCrackedHints(worker);
				worker.tell(new Worker.TaskMessage(this.toCrack.get(0)), this.self());
				this.toCrack.remove(0);
				notFree.add(worker);
			}
		}
		for (ActorRef worker : notFree) {
			this.freeWorkers.remove(worker);
		}
	}
	
	protected void sendHintsHashes(ActorRef worker) {
		int ii = 1;
		List<String> tmp = new ArrayList<String>();
		for (String hh : this.allHints) {
			tmp.add(hh);
			if (ii % ConfigurationSingleton.get().getBufferSize() == 0) {
				worker.tell(new Worker.HintsHashesMessage(tmp), this.self());
				tmp = new ArrayList<String>(); // Not clear() as it passes by reference if on the same JVM
			}
			ii++;
		}
		worker.tell(new Worker.HintsHashesMessage(tmp), this.self());
	}
	
	protected void sendCrackedHints(ActorRef worker) {
		int ii = 0;
		Hashtable<String,String> tmp = new Hashtable<String,String>();
		for (String key : this.crackedHints.keySet()) {
			tmp.put(key, this.crackedHints.get(key));
			if (ii % ConfigurationSingleton.get().getBufferSize() == 0) {
				worker.tell(new Worker.CrackedHintsMessage(tmp), this.self());
				tmp = new Hashtable<String, String>(); // Not clear() as it passes by reference if on the same JVM
			}
			ii++;
		}
		worker.tell(new Worker.CrackedHintsMessage(tmp), this.self());
	}
	
	protected void handle(HintMessage message) {
		Hashtable<String,String> hintsCracks = message.getCrackedHints();
		for (String key : hintsCracks.keySet()) {
			this.crackedHints.put(key, hintsCracks.get(key));
		}
		
		this.freeWorkers.add(this.sender());
		this.distribute();
	}
	
	// Receive result from a worker, and give it a new task if there are some left
	protected void handle(ResultMessage message) {
		String name = message.getResult()[0];
		String password = message.getResult()[1];
		collector.tell(new Collector.CollectMessage(password), this.self());
		
		this.freeWorkers.add(this.sender());
		
		if (this.toCrack.isEmpty() && this.freeWorkers.size() == this.workers.size()) {
			this.terminate();
		}
		this.distribute();
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
		this.collector.tell(new Collector.PrintMessage(), this.self());
		this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		for (ActorRef worker : this.workers) {
			this.context().unwatch(worker);
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
		
		this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		
		long executionTime = System.currentTimeMillis() - this.startTime;
		this.log().info("Algorithm finished in {} ms", executionTime);
	}

	protected void handle(RegistrationMessage message) {
		this.context().watch(this.sender());
		this.workers.add(this.sender());
		this.freeWorkers.add(this.sender());
		this.distribute();
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.freeWorkers.remove(message.getActor());
	}
}
