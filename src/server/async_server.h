#pragma once

#include <string.h>
#include <grpcpp/grpcpp.h>
#include "paxos.grpc.pb.h"
#include <shared_mutex>
#include "../types/transaction.h"
#include <chrono>

using grpc::Server;
using grpc::CompletionQueue;
using grpc::ServerCompletionQueue;

using paxos::Paxos;
using paxos::Proposal;
using paxos::Transaction;
using paxos::TransferReq;
using paxos::TransferRes;
using paxos::Balance;
using paxos::Logs;
using paxos::PrepareReq;
using paxos::PrepareRes;
using paxos::AcceptReq;
using paxos::AcceptRes;
using paxos::CommitReq;
using paxos::CommitRes;
using paxos::SuccessReq;
using paxos::SuccessRes;

// Server Implementation
class PaxosServer final {
public:
    PaxosServer(std::string serverName);
    void run(std::string targetAddress);
    ~PaxosServer();
    void HandleRPCs();

    void handlePrepareReply(PrepareRes& reply);
    void handleAcceptReply(AcceptRes& reply);
    void handleCommitReply(CommitRes& reply);
    void handleSuccessReply(SuccessRes& reply);
    
    bool processTransferCall(TransferReq* req, TransferRes* res);
    bool processGetBalanceCall(Balance* res);
    bool processGetLogsCall(Logs* logs);
    bool processGetDBLogsCall(Logs* logs);
    bool processPrepareCall(PrepareReq* req, PrepareRes* res);
    bool processAcceptCall(AcceptReq* req, AcceptRes* res);
    bool processCommitCall(CommitReq* req, CommitRes* res);
    bool processSuccessCall(SuccessReq* req, SuccessRes* res);


private:
    std::string serverName;

    // Completion Queue for incoming requests to the server
    std::unique_ptr<ServerCompletionQueue> requestCQ;

    // Completion Queue for responses to server's prepare, accept, and commit requests
    std::unique_ptr<CompletionQueue> responseCQ;

    Paxos::AsyncService service;
    std::unique_ptr<Server> server;

    // Stubs for sending prepare, accept, and commit RPCs to other servers  
    std::vector<std::unique_ptr<Paxos::Stub>> stubs_;

    int rpcTimeoutSeconds;

    enum ServerState { IDLE, PREPARE, PROPOSE, COMMIT, PROMISED, ACCEPTED };
    ServerState currentState_;
    
    int balance;
    int currentTransactionNum;
    std::vector<types::Transaction> localLogs;

    types::Proposal myProposal;

    /* For Prepare phase */
    // highest accept num and accept val received in prepare replies (should be reset before prepare phase)
    types::Proposal highestAcceptNum;
    std::vector<types::Transaction> highestAcceptVal;
    
    // logs from other servers received in prepare replies
    std::vector<types::Transaction> remoteLogs;

    /* For Accept phase */
    // accept num and accept val at current node
    types::Proposal acceptNum;
    std::vector<types::Transaction> acceptVal;

    /* For commit phase */
    int lastCommittedBlock;
    types::Proposal lastCommittedProposal;
    std::vector<types::TransactionBlock> committedBlocks;
    
    // variables for tracking the state of consensus
    const static int MAJORITY = 2;
    int prepareSuccesses;
    int prepareFailures;
    int acceptSuccesses;
    int acceptFailures;
    int commitSuccesses;
    int commitFailures;

    void copyProposal(Proposal* from, Proposal* to);
    int getServerIdFromName(std::string serverName);
    void sendPrepareToCluster(PrepareReq& request);
    void sendAcceptToCluster(AcceptReq& request);
    void sendCommitToCluster(CommitReq& request);

    void sendPrepareAsync(PrepareReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline);
    void sendAcceptAsync(AcceptReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline);
    void sendCommitAsync(CommitReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline);
    void sendSuccessAsync(SuccessReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline);

    void populateProposal(Proposal* to, types::Proposal from);
    void populateAcceptNum(Proposal* acceptNum);
    void populateLocalLogs(Logs* logs);
    void populateAcceptVal(Logs* logs);
    void replicateBlock(std::string serverName, int blockId);
};