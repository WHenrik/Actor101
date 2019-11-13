package de.hpi.ddm.actors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.serialization.*;
import akka.actor.Props;
import akka.util.MessageBuffer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import sun.rmi.rmic.Util;

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
		
		// This will definitely fail in a distributed setting if the serialized message is large!
		// Solution options:
		// 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
		// 2. Serialize the object and send its bytes via Akka streaming.
		// 3. Send the object via Akka's http client-server component.
		// 4. Other ideas ...
		
		// Serialize the object with the akka.serializable library
		Serialization serialization = SerializationExtension.get(this.getContext().getSystem());
		
		byte[] bytes = serialization.serialize(message.getMessage()).get();
		int serializerID = serialization.findSerializerFor(message.getMessage()).identifier();
		String manifest = Serializers.manifestFor(serialization.findSerializerFor(message.getMessage()), message.getMessage());


		int distance = 8096;
		int endcount = (int) Math.ceil((double) bytes.length/distance);
		//System.out.println(bytes.length + "HAALAOOFDFDS");
		// chunk size to divide
		for(int messagecount=0;messagecount<endcount;messagecount+=1){
			byte[] byte_chunk= Arrays.copyOfRange(bytes, messagecount*distance, Math.min(bytes.length,(messagecount+1)*distance));
			receiverProxy.tell(new BytesMessage<byte[]>(byte_chunk, manifest, serializerID, this.sender(), message.getReceiver(),messagecount,endcount), this.self());
		}
		System.out.println("VORBEI!!!!!");

		//receiverProxy.tell(new BytesMessage<>(bytes, manifest, serializerID, this.sender(), message.getReceiver()), this.self());
	}

	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		System.out.println("1. "+ message.messagecount + "2. " + message.endcount);
		if(message.messagecount<message.endcount){
			byte[] test = (byte[]) message.bytes;
			try {
				messageBuffer.write(test);
			} catch (IOException e) {
				System.out.println("FEHLER!!!!");
				e.printStackTrace();
			}
		}
		if(message.messagecount==(message.endcount)-1) {
			Serialization serialization = SerializationExtension.get(this.getContext().getSystem());
			Object mes = serialization.deserialize(messageBuffer.toByteArray(), message.serializerID, message.manifest).get();
			message.getReceiver().tell(mes, message.getSender());
			messageBuffer.reset();
			System.out.println("AKKAMACHTSPAß");
		}
	}
}

//TODO: Include ByteBuffer ?
//https://doc.akka.io/docs/akka/current/remoting-artery.html#bytebuffer-based-serialization