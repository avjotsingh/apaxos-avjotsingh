#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/alarm.h>
#include "paxos.grpc.pb.h"
#include "../server/async_server.h"
#include "../types/request_types.h"

using grpc::ServerAsyncResponseWriter;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using google::protobuf::Empty;
using grpc::Alarm;

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
using paxos::SuccessReq;
using paxos::SuccessRes;

class CallData {
public:
    CallData(Paxos::AsyncService* service, PaxosServer* server, ServerCompletionQueue* cq, types::RequestTypes type);
    void Proceed();
    void Retry();

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
    SuccessReq successReq;
    SuccessRes successRes;

    // The means to get back to the client.
    ServerAsyncResponseWriter<TransferRes> transferResponder;
    ServerAsyncResponseWriter<Balance> getBalanceResponder;
    ServerAsyncResponseWriter<Logs> getLogsResponder;
    ServerAsyncResponseWriter<Logs> getDBLogsResponder;
    ServerAsyncResponseWriter<PrepareRes> prepareResponder;
    ServerAsyncResponseWriter<AcceptRes> acceptResponder;
    ServerAsyncResponseWriter<CommitRes> commitResponder;
    ServerAsyncResponseWriter<SuccessRes> successResponder;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS, RETRY, FINISH };
    CallStatus status_;  // The current serving state.
    types::RequestTypes callType;   

    int retry_count_;
    const static int maxRetryCount = 2;
    std::unique_ptr<grpc::Alarm> alarm_;  // Alarm to schedule retries.
    int randomBackoff(int minMs, int maxMs);

    const static int proposerMinBackoff = 5;
    const static int proposerMaxBackoff = 10;
    const static int acceptorMinBackoff = 20;
    const static int acceptorMaxBackoff = 30; 
};