#pragma once

#include <grpcpp/grpcpp.h>
#include "paxos.grpc.pb.h"
#include "async_server.h"
#include "../types/request_types.h"

using grpc::ServerAsyncResponseWriter;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using google::protobuf::Empty;

using paxos::Paxos;
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

class CallData {
public:
    CallData(Paxos::AsyncService* service, PaxosServer* server, ServerCompletionQueue* cq, types::RequestTypes type);
    void Proceed();

private:
    Paxos::AsyncService* service_;
    PaxosServer* server_;
    ServerCompletionQueue* cq_;
    ServerContext ctx_;

    // Different request and response types that server can expect to
    // receive and send to the client
    TransferReq transferReq;
    TransferRes transferRes;
    Empty getBalanceReq;
    Balance getBalanceRes;
    Empty getLogsReq;
    Logs getLogsRes;
    Empty getDBLogsReq;
    Logs getDBLogsRes;
    PrepareReq prepareReq;
    PrepareRes prepareRes;
    AcceptReq acceptReq;
    AcceptRes acceptRes;
    CommitReq commitReq;
    CommitRes commitRes;

    // The means to get back to the client.
    ServerAsyncResponseWriter<TransferRes> transferResponder;
    ServerAsyncResponseWriter<Balance> getBalanceResponder;
    ServerAsyncResponseWriter<Logs> getLogsResponder;
    ServerAsyncResponseWriter<Logs> getDBLogsResponder;
    ServerAsyncResponseWriter<PrepareRes> prepareResponder;
    ServerAsyncResponseWriter<AcceptRes> acceptResponder;
    ServerAsyncResponseWriter<CommitRes> commitResponder;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;  // The current serving state.
    types::RequestTypes callType;    
};
