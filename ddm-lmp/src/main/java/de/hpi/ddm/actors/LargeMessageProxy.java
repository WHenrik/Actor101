package de.hpi.ddm.actors;

import java.io.Serializable;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.serialization.*;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private String manifest;
		private int serializerID;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming. - CHECK - HAVE TO TEST
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		
		// Serialize the object with the akka.serializable library
		Serialization serialization = SerializationExtension.get(this.getContext().getSystem());
		
		byte[] bytes = serialization.serialize(message.getMessage()).get();
		int serializerID = serialization.findSerializerFor(message.getMessage()).identifier();
		String manifest = Serializers.manifestFor(serialization.findSerializerFor(message.getMessage()), message.getMessage());

		receiverProxy.tell(new BytesMessage<>(bytes, manifest, serializerID, this.sender(), message.getReceiver()), this.self());
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content. -check
		Serialization serialization = SerializationExtension.get(this.getContext().getSystem());
				
		Object mes =  serialization.deserialize((byte[]) message.bytes, message.serializerID, message.manifest).get();

		message.getReceiver().tell(mes, message.getSender());
	}
}

//TODO: Include ByteBuffer ?
//https://doc.akka.io/docs/akka/current/remoting-artery.html#bytebuffer-based-serialization