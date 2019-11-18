package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Hashtable;

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

		this.toCrack = new ArrayList<>();
		this.allHints = new ArrayList<>();
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
	private List<String> allHints;
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
		this.reader.tell(new Reader.ReadMessage(), this.self());

		this.crackedHints = new Hashtable<String, String>();
		this.allHints = new ArrayList<String>();
		this.toCrack = new ArrayList<String[]>();
	}
	
	protected void handle(BatchMessage message) {
		// Load all password and hints
		// Ask the workers to crack the hashes once all the data are loaded
		
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		// The input file is read in batches for two reasons: /////////////////////////////////////////////////
		// 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
		// 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
		// TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
		///////////////////////////////////////////////////////////////////////////////////////////////////////
		if (message.getLines().isEmpty()) {
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.dataLoaded = true;
			//this.log().info("Hints to crack:" + allHints.toString());
			this.distribute();
			//this.terminate();
			return;
		}
		
		for (String[] line : message.getLines()) {
			//System.out.println(Arrays.toString(line));
			toCrack.add(line);
			for (int ii=5; ii < line.length; ii++) {
				allHints.add(line[ii]);
			}
		}
		this.passwordChars = Arrays.asList(toCrack.get(0)[2].split(""));
		this.toProcessChars = Arrays.asList(toCrack.get(0)[2].split(""));
		
		
		this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
		this.reader.tell(new Reader.ReadMessage(), this.self());
	}
	
	protected void distribute() {
		if (System.currentTimeMillis() - this.startTime > 240*1000) { // TODO Failsafe for testing
			this.log().info("Early shutdown");
			terminate();
		}
		List<ActorRef> notFree = new ArrayList<ActorRef>();
		for (ActorRef worker : this.freeWorkers) {
			if (this.dataLoaded && !(this.toProcessChars.isEmpty() || this.toProcessID >= this.passwordChars.size())) {
				String nextChar = this.toProcessChars.get(this.toProcessID);
				//String nextChar = this.toProcessChars.get(0);
				//this.log().info("Selected: " + nextChar);
				this.toProcessID++;
				//this.toProcessChars.remove(0);
				//worker.tell(new Worker.HintsHashesMessage(this.allHints), this.self());
				this.sendHintsHashes(worker);
				//worker.tell(new Worker.HashMessage(nextChar, this.passwordChars), this.self());
				worker.tell(new Worker.PasswordChars(this.passwordChars), this.self());
				worker.tell(new Worker.HashMessage(nextChar), this.self());
				notFree.add(worker);
			} else if (!this.toCrack.isEmpty() && this.crackedHints.size() == this.allHints.size()) { // Assumes all hints are unique TODO make unique allHints
				worker.tell(new Worker.CrackedHintsMessage(this.crackedHints), this.self());
				//worker.tell(new Worker.TaskMessage(this.toCrack.get(0)), this.self());
				this.sendCrackedHints(worker);
				this.toCrack.remove(0);
				notFree.add(worker);
			}
		}
		for (ActorRef worker : notFree) {
			this.freeWorkers.remove(worker);
		}
	}
	
	protected void sendHintsHashes(ActorRef worker) {
		int ii = 0;
		List<String> tmp = new ArrayList<String>();
		for (String hh : this.allHints) {
			tmp.add(hh);
			if (ii % ConfigurationSingleton.get().getBufferSize() == 0) {
				worker.tell(new Worker.HintsHashesMessage(tmp), this.self());
				tmp.clear();
			}
			ii++;
		}
	}
	
	protected void sendCrackedHints(ActorRef worker) {
		int ii = 0;
		Hashtable<String,String> tmp = new Hashtable<String,String>();
		for (String key : this.crackedHints.keySet()) {
			tmp.put(key, this.crackedHints.get(key));
			if (ii % ConfigurationSingleton.get().getBufferSize() == 0) {
				worker.tell(new Worker.CrackedHintsMessage(tmp), this.self());
				tmp.clear();
			}
			ii++;
		}
	}
	
	protected void handle(HintMessage message) {
		Hashtable<String,String> hintsCracks = message.getCrackedHints();
		//this.log().info("Getting cracked hints back (crackedHints : " + this.crackedHints.size() + ", allHints : " + this.allHints.size() + ")");
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
		//this.log().info("Cracked " + name + ": " + password);
		collector.tell(new Collector.CollectMessage("Cracked " + name + ": " + password), this.self());
		
		this.freeWorkers.add(this.sender());
		
		if (this.toCrack.isEmpty() && this.freeWorkers.size() == this.workers.size()) {
			this.log().info("Terminated because empty and done: " + String.valueOf(this.toCrack.size()));
			this.collector.tell(new Collector.PrintMessage(), this.self());
			this.terminate();
		}
		this.distribute();
	}
	
	protected void terminate() {
		this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
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
//		this.log().info("Registered {}", this.sender());
	}
	
	protected void handle(Terminated message) {
		this.context().unwatch(message.getActor());
		this.workers.remove(message.getActor());
		this.freeWorkers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
	}
}
