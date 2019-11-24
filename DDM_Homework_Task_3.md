DDM Homework Task 3

Our approach to solve the task 

Master

1. Loads the data and gathers the hints' hashes.
2. Sends the hints hashes, the password characters and the password length to the worker along with a non cracked
letter to generate the potential hints strings.
3. Collects the cracked hints from the workers in a hashtable. Sends the next letter if there are some left to be sent.
Controls that all hints have been cracked before progressing to 4.
4. Sends the cracked hints as an hashtable to the workers, along with a line of the password file.
5. Gather the cracked passwords from the workers in a hashtable and send the next uncracked and unsent line of the password
file. Waits until all password have been cracked before progressing to 6.
6. Sends the cracked name/password combinations to the Collector and terminates.

Worker

Processes 2 kind of jobs:

1. Hint cracking job: Generates all permutations from the password character minus the one letter sent, hash each permutation
and compare it to the list of hints' hashes. Collects all cracked hints in a hashtable and sends it back to the master once
all permutations' hashes have been compared.

2. Password cracking job: Extracts the hint hashes from the password file's line and get the decrypted hints using the hashtable.
Then excludes all the letters found from the hints from the password characters list and generates all combinations of this
reduced list to hash and compare against the password hash. Returns the password as soon as it is cracked to the Master.

