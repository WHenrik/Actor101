package de.hpi.ddm.actors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.serialization.*;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.lang.Math;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	private ByteArrayOutputStream messageBuffer = new ByteArrayOutputStream();;
	
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

	//two ints were added
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private String manifest;
		private int serializerID;
		private ActorRef sender;
		private ActorRef receiver;
		private int messagecount;
		private int endcount;
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

		// Solution option:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).

		// Serialize the object with the akka.serializable library
		Serialization serialization = SerializationExtension.get(this.getContext().getSystem());
		
		byte[] bytes = serialization.serialize(message.getMessage()).get();
		int serializerID = serialization.findSerializerFor(message.getMessage()).identifier();
		String manifest = Serializers.manifestFor(serialization.findSerializerFor(message.getMessage()), message.getMessage());

		//Chunk byte array into array chunk of length 8096
		int distance = 8096;
		int endcount = (int) Math.ceil((double) bytes.length/distance);
		for(int messagecount=0;messagecount<endcount;messagecount+=1){
			byte[] byte_chunk= Arrays.copyOfRange(bytes, messagecount*distance, Math.min(bytes.length,(messagecount+1)*distance));
			receiverProxy.tell(new BytesMessage<byte[]>(byte_chunk, manifest, serializerID, this.sender(), message.getReceiver(),messagecount,endcount), this.self());
		}
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.

		//collects the array chunks and brings them together
		if(message.messagecount<message.endcount){
			byte[] test = (byte[]) message.bytes;
			try {
				messageBuffer.write(test);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		//whole message will be deserialized
		if(message.messagecount==(message.endcount)-1) {
			Serialization serialization = SerializationExtension.get(this.getContext().getSystem());
			Object mes = serialization.deserialize(messageBuffer.toByteArray(), message.serializerID, message.manifest).get();
			message.getReceiver().tell(mes, message.getSender());
			messageBuffer.reset();
		}
	}
}
