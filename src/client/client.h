#pragma once

#include <string>
#include <grpcpp/grpcpp.h>
#include "paxos.grpc.pb.h"
#include "../types/transaction.h"

using grpc::Channel;
using grpc::ClientContext;

using paxos::Paxos;


class PaxosClient {
public:
    PaxosClient(std::string serverName, std::string targetAddress);
    bool TransferAmount(std::string receiver, int amount);
    int GetBalance();
    std::vector<types::Transaction> GetLogs();
    std::vector<types::Transaction> GetDBLogs();
    void Prepare();
    void Accept();
    void Commit();

private:
    std::string serverName;
    std::shared_ptr<Channel> channel;
    std::unique_ptr<Paxos::Stub> stub_;
};  