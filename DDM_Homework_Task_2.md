DDM Homework Task 2

Our approach to solve the task

Sender

1. serialize the message

2. chunk byte array into array chunks of (at most) length 8096

3. send chunks of a message consecutively by sending objects including a serialized messagechunk, manifest, serializerID, sender, current chunk number and number of total expected chunks for this message

Receiver

1. gets message chunkwise, collects byte chunks and reassembles the message based on chunk number and number of expected chunks into one byte array

2. deserializes whole message

The serialization is done by kryo and the large messages are send via the dedicated artery's side channel.
