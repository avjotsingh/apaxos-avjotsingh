## Abstract
The objective of this project is to implement a variant of the Paxos consensus protocol. Paxos can achieve consensus with at least 2f + 1 nodes where f is the maximum number of concurrent faulty (crashed) nodes. As part of the project, you will create a basic (but not entirely realistic) distributed banking application using a group of servers. Each server will handle outgoing transactions for a specific account (client) locally and keep track of them in its own log. In cases where an account lacks sufficient funds for a transaction, a modified Paxos protocol will be initiated among all servers to retrieve the latest transactions. Once consensus is reached, each server will add a block of transactions to its datastore.


## Design
Each server is responsible for processing the outgoing transactions of its corresponding client and each client initially has 100 units in its account. _Figure 1_ presents the log of each server after a few transactions (the same set of transactions as the basic design). Client A has initiated two transactions (A,C,20) and (A,B,60). Client B has initiated a single transaction (B,A,70) and client C has initiated a single transaction (C,A,50). At this point, the servers are not aware of the transactions that are received on other servers.

<p align="center">
      <img src="https://github.com/user-attachments/assets/ad627873-38bb-48ef-83ff-59ec6a2ce3fc">
      <br>
      <em>Figure 1: First Snapshot</em>
</p>

Again, client A initiates a transaction (A,B,30). Server A checks the client’s account and realizes that client A has only 20 units in its account. Therefore, it cannot process the transaction locally. In the main design, in contrast to the basic design, Server A initiates a modified Paxos protocol itself to get the most up-to-date information from servers B, and C. Once consensus is established, each server gets transactions on (at least the majority of) all servers and as shown in _Figure 2_, appends a block of transactions to its datastore.

<p align="center">
      <img src="https://github.com/user-attachments/assets/f99f56c2-d076-4803-8ab4-5f50266cb52a">
      <br>
      <em>Figure 2: Second Snapshot</em>
</p>

Now, the balance of client A is 140 and A can easily initiate transaction (A,B,30). Similarly, clients B and C also initiate transactions (B,C,50) and (C,B,60) which are processed locally on servers B and C respectively, as shown in _Figure 3_.
<p align="center">
      <img src="https://github.com/user-attachments/assets/588c2fa1-3638-4e31-9e44-368cdcb6678f">
      <br>
      <em>Figure 3: Third Snapshot</em>
</p>

At this point client B sends transaction (B,A,60), however its balance is 40 and server B cannot process the transaction. As a result, server B will initiate a consensus protocol among all servers to get the most recent list of transactions. As shown in _Figure 4_, as a result of consensus, a block of transactions will be added to the datastore and server B can also execute the requested transaction.

<p align="center">
      <img src="https://github.com/user-attachments/assets/c3a4a26f-2946-4f85-a71d-8b74df3755f3">
      <br>
      <em>Figure 4: Fourth Snapshot</em>
</p>


## Modified Paxos Protocol
**Part I - Leader Election:** We change the leader election phase of Paxos so that the leader is aware of the local logs of the servers involved
in the consensus. A server S can contest for leader election only if it has a transaction _(S,S’,amt)_ and the balance of client S is less than amt. Now, server S needs to start consensus with other servers to retrieve their local transactions in order to process the transaction. Server S initiates the leader election by sending _⟨PREPARE, n⟩_ to all servers (note that the prepare message structure might need more elements). Upon receiving a prepare message from server S, each server S’ sends a promise message _⟨ACK,n,AcceptNum,AcceptVal,T(S′)⟩_ to S where T(S′) is a list of transactions that were processed locally on server S’ but have not been appended to the datastore. Two scenarios are possible here. (1) if there is a value (i.e., a set of transactions) that has been proposed and accepted (but might not decided due to leader failure) in the previous phase, server S’ uses _AcceptNum_ and _AcceptVal_ to inform the leader about the latest accepted value (set of transactions) and what ballot it was accepted in (and T(S′) would be empty). (2) if there is no such a value, server S’ uses T(S′) to inform server S about the list of transactions that were processed locally on server S’ but have not been appended to the datastore.
All other details remain similar to the Paxos protocol.

**Part II: - Normal Operation:** The server contesting leader election (here, server S) logs all promise messages. Once it receives f promise messages from different servers (plus itself becomes f + 1, a majority of the servers), it either proposes an accepted but not decided value from previous rounds or it creates a major transaction block MB combining its own set of local transactions and all the transaction lists received in promise messages. The leader then multicasts a accept message _⟨ACCEPT,n,MB⟩_ to all servers. Note that the leader might need to synchronize slow servers that send AcceptVal values that have already been decided (committed). Upon receiving a valid accept message from the leader, each server S′ sends an accepted message _⟨ACCEPTED,n,MB⟩_ to the leader which is followed by a decide phase of Paxos to persist the changes permanently. Every server must also delete its local transactions that are part of the main block MB from its log. As the server may have recorded new transactions (between sending its log to the leader in a promise message and receiving a commit message from the leader), it needs to carefully examine transactions in its log and remove only the outdated ones. One simple method for accomplishing this task is to assign an order (such as timestamp or sequence number) to local transactions when adding them to the log. _Figure 5_ demonstrates the normal case execution of Paxos protocol with 3 servers.

<p align="center">
      <img src="https://github.com/user-attachments/assets/ce14420d-7224-45c2-a481-8a76a3b63039">
      <br>
      <em>Figure 5: Normal case execution of Paxos</em>
</p>

**Part III - Node Failure:** This part mainly remains the same as the original Paxos protocol. Note that we should be able to synchronize servers that recovered from failures with other servers.


## Implementation details

**1. Catchup Mechanism:** After recovering from a failure, a server’s datastore may have outdated information compared to other servers that have processed new transaction blocks. As a result, the server must synchronize its state with others in order to process incoming transactions. There are various methods for implementing this synchronization mechanism. We have adopted a passive approach that entails the server waiting for a prepare message from a leader and notifying the leader that its state is outdated, after which the leader attempts to synchronize the server’s datastore. This process is somewhat similar to what leader nodes do in the Raft protocol. Including the current ballot number and the ballot number of the last committed block in the messages resolves many of the challenges.

**2. Data loss:** If a server crashes and its log is not stored persistently, any client transactions within the log will be lost. To avoid that, we store logs on persistent storage (SQLite DB), and ensure that servers do not send reply messages to clients if the client transaction has not been stored in persistent storage. This precaution is necessary because if the server sends a reply to the client while the transaction is in the log and then subsequently fails, the client transaction may never be executed despite being told by the server that it was successful.

**3. Datastore consistency:** This project, unlike Paxos, requires handling a sequence of values. Consequently, we need a mechanism to make sure that all servers have committed all transaction blocks and there is no gap in the datastore of different servers. An an example, consider the scenario presented in _Figure 6_. Server 1 is able to commit a major block B with ballot number <1,1> by agreement from a majority of servers (i.e., 1, 2, and 3). Once the block is committed, servers 1, 2 and 3 will reset their AcceptNum and AcceptVal values. Next, server 5 becomes the leader and successfully commits another block of transactions B′ with ballot number <2,5>. This time servers 3, 4 and 5 where involved in consensus.

<p align="center">
      <img src="https://github.com/user-attachments/assets/5bd38c7e-39d5-41aa-8287-88a77d5e4cfa">
      <br>
      <em>Figure 6: The need for resetting AcceptVal and AcceptNum</em>
</p>

Now, if we look at the datastore on different servers, server 1 and 2 maintain a single block B, server 3 maintains two blocks B and B′ and servers 4 and 5 maintain a single block B′, demonstrating an inconsistency in datastores.
To resolve this issue, servers need to keep each other informed about their last commit- ted blocks. One potential approach is for the leader to include the ballot number of its last committed block in its prepare message. Other servers will only accept a prepare message if the ballot number of the leader’s last committed block is greater than or equal to their own last committed block’s ballot number. This mechanism, along with the catch-up mechanism, should effectively address any issues related to failed or slow servers.

**4. Log consistency:** When a server receives a commit message from the leader, it needs to remove the corresponding transactions from its log. This involves checking which segments of its log have been incorporated into the proposed major block. It is crucial to conduct this check because (a) the server may have added new transactions to its log while consensus was being reached, and (b) in scenarios involving parallel proposers, the server might have sent different sets of transactions to different proposers. To address these challenges, incorporating unique sequence numbers into both the local log and final block can help maintain consistency and accurately identify which transactions should be removed. You should also be able to handle situations where a server sends some transactions to the leader in its promise message and fails before receiving the corresponding commit messages. In this situation and if the log has not been stored persistently, the server might receive a major block including its own local transactions while such transactions are not part of its current log. Another relevant scenario arises when a server sends transactions to the leader in its promise message but does not receive corresponding accept or commit messages, even though the leader has committed those transactions. In such cases, the server may forward these transactions from its log to the next leader. The leader should be capable of informing the server about previously committed messages (as discussed in the previous section on datastore consistency). As a result, the server will update its datastore and log to prevent duplicate entries. The leader can choose to either wait for a new promise message from the server or include only the new portion of the server’s log in its proposed block by verifying the sequence number of transactions. _Figure 7_ demonstrates an example of such a scenario. Server 1 is acting as the leader and includes all servers’ transactions, including the single transaction (E,B,40) from server 5, in its proposed block. However, server 5 does not receive the corresponding accept and commit messages. Next, server 3 attempts to become the leader by sending a prepare message with the ballot number <2,3>. In response, server 5 sends a promise message to server 3 because the ballot number is greater than the last ballot number that it has seen (<1,1>). Server 5 also includes its logged transactions in the message. Notably, this includes both transactions ((E,B,40) and (E,D,10)), even though transac- tion (E,B,40) has already been committed on servers 1, 2 and 3. To resolve this issue, the leader must inform server 5 about past committed blocks in its prepare messages, as discussed in the previous part, in order to prevent duplicate entries. In this example, if server 4, which is not aware of the last committed block, attempts to become the leader instead of server 3, the catch-up mechanism should be capable of updating the datastore of server 4. This means that proposer servers must include the ballot number of their last committed block, in addition to their current ballot number, in their prepare messages.

<p align="center">
      <img src="https://github.com/user-attachments/assets/cd0782b7-75e5-48f1-ab7f-03e5bafee95a">
      <br>
      <em>Figure 7: The need for updating log upon receiving commit messages for previous blocks</em>
</p>


## Setup instructions

### Install gRPC

Instructions to install gRPC on MacOS are given below:

For installation instructions on Linux or Windows, please follow the steps here: https://grpc.io/docs/languages/cpp/quickstart/

1. Choose a directory to hold locally installed packages
```
$ export MY_INSTALL_DIR=$HOME/.local
```

2. Ensure that the directory exists
```
$ mkdir -p $MY_INSTALL_DIR
```

3. Add the local `bin` folder to path variable
```
$ export PATH="$MY_INSTALL_DIR/bin:$PATH"
```

4. Install cmake
```
$ brew install cmake
```

5. Install other required tools
```
$ brew install autoconf automake libtool pkg-config
```

6. Clone the gRPC repo
```
$ git clone --recurse-submodules -b v1.66.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc
```

7. Build and install gRPC and Protocol Buffers
```
$ cd grpc
$ mkdir -p cmake/build
$ pushd cmake/build
$ cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
      ../..
$ make -j 4
$ make install
$ popd
```

### Clone the project repository

1. Clone the project repository which has the source code for running paxos.
```
$ git clone git@github.com:avjotsingh/apaxos-avjotsingh.git
```


### Build the project

1. Build the project. This should not take too long
```
$ cd apaxos-avjotsingh
$ mkdir build
$ cd ./build
$ cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ..
$ cmake --build .
```

2. Run the project
The driver program needs a CSV which has the following format
<p align="center">
      <img src="https://github.com/user-attachments/assets/27cb193a-3bc5-4ccb-a9c4-51203d412fff">
      <br>
      <em>Input CSV file format</em>
</p>
An example CSV file is available under the test directory

```
$ cd build
$ ./driver <csv_filepath>
```
