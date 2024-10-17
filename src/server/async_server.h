#pragma once

#include <string.h>
#include <grpcpp/grpcpp.h>
#include "paxos.grpc.pb.h"
#include <shared_mutex>
#include "../types/transaction.h"
#include "../client/async_cluster_client.h"


using grpc::Server;
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

// Server Implementation
class PaxosServer final {
public:
    PaxosServer(std::string serverName);
    void run(std::string targetAddress);
    void HandleRPCs();
    ~PaxosServer();

    void processTransferCall(TransferReq& req, TransferRes& res);
    void processGetBalanceCall(Balance& res);
    void processGetLogsCall(Logs& logs);
    void processGetDBLogsCall(Logs& logs);
    void processPrepareCall(PrepareReq& req, PrepareRes& res);
    void processAcceptCall(AcceptReq& req, AcceptRes& res);
    void processCommitCall(CommitReq& req, CommitRes& res);

private:
    std::unique_ptr<ServerCompletionQueue> cq;
    Paxos::AsyncService service;
    std::unique_ptr<Server> server;
    PaxosClusterClient* clusterClient;
    
    std::string serverName;
    int balance;
    int currentProposalNum;
    int currentTransactionNum;
    std::shared_mutex currentStateMutex;

    types::Proposal acceptedProposal;
    std::vector<types::Transaction> acceptedLogs;
    int acceptedBlockId;
    std::shared_mutex acceptedStateMutex;

    std::vector<types::Transaction> localLogs;
    std::shared_mutex localLogsMutex;

    std::vector<types::TransactionBlock> committedBlocks;
    std::shared_mutex committedBlockMutex;


    void populateAcceptNum(Proposal* acceptNum);
    void populateLocalLogs(Logs* logs);
    void acceptLogs(std::vector<types::Transaction> logs);
    void commitLogs();
    void populateAcceptVal(Logs* logs);
};