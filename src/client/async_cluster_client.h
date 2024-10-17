#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include "paxos.grpc.pb.h"

#include "../utils/utils.h"
#include "../types/request_types.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using paxos::Paxos;
using paxos::Proposal;
using paxos::PrepareReq;
using paxos::PrepareRes;
using paxos::AcceptReq;
using paxos::AcceptRes;
using paxos::CommitReq;
using paxos::CommitRes;


class PaxosClusterClient {
public:
    PaxosClusterClient(std::string hostServerName);
    void sendPrepareToCluster(PrepareReq request, PrepareRes* reply);
    void sendAcceptToCluster(AcceptReq request, AcceptRes* reply);
    void sendCommitToCluster(CommitReq request, CommitRes* reply);

private:
    // struct for keeping state and data information
    struct AsyncClientCall {
        types::RequestTypes callType;

        // Container for the data we expect from the server.
        PrepareRes prepareReply;
        AcceptRes acceptReply;
        CommitRes commitReply;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // Storage for the status of the RPC upon completion.
        Status status;

        std::unique_ptr<ClientAsyncResponseReader<PrepareRes>> prepareResponseReader;
        std::unique_ptr<ClientAsyncResponseReader<AcceptRes>> acceptResponseReader;
        std::unique_ptr<ClientAsyncResponseReader<CommitRes>> commitResponseReader;
    };

    // servers' exposed services.
    std::vector<std::unique_ptr<Paxos::Stub>> stubs_;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq_;

    // RPC timeout in seconds
    int rpcTimeoutSeconds = 5;

    void sendPrepareAsync(PrepareReq request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline);
    void sendAcceptAsync(AcceptReq request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline);
    void sendCommitAsync(CommitReq request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline);
};