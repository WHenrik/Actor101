DDM Homework Task 2

Our approach to solve the task

Producer

1. serialize the message

2. chunk byte array into array chunks of length 8096

3. sending object including chunks, manifest, serializerID, sender, recent chunk number and number of expected chunks

Consumer

1. gets message and collecting byte chunks based on chunk number and number of expected chunks into one byte array

2. deserializing whole message