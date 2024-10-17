#include "call_data.h"
#include "../types/transaction.h"
#include <shared_mutex>
#include <string.h>
#include "absl/log/check.h"

using grpc::Status;

CallData::CallData(Paxos::AsyncService* service, PaxosServer* server, ServerCompletionQueue* cq, types::RequestTypes type):
            transferResponder(&ctx_), 
            getBalanceResponder(&ctx_), 
            getLogsResponder(&ctx_), 
            getDBLogsResponder(&ctx_),
            prepareResponder(&ctx_),
            acceptResponder(&ctx_),
            commitResponder(&ctx_) {
        service_ = service;
        server_ = server;
        cq_ = cq;
        status_ = CREATE;
        callType = type;
        
        Proceed();
      }

void CallData::Proceed() {
    if (status_ == CREATE) {
        
        switch (callType) {
            case types::TRANSFER: 
                service_->RequestTransfer(&ctx_, &transferReq, &transferResponder, cq_, cq_, this);
                break;
            case types::GET_BALANCE:
                service_->RequestGetBalance(&ctx_, &getBalanceReq, &getBalanceResponder, cq_, cq_, this);
                break;
            case types::GET_LOGS:
                service_->RequestGetLogs(&ctx_, &getLogsReq, &getLogsResponder, cq_, cq_, this);
                break;
            case types::GET_DB_LOGS:
                service_->RequestGetDBLogs(&ctx_, &getDBLogsReq, &getDBLogsResponder, cq_, cq_, this);
                break;
            case types::PREPARE:
                service_->RequestPrepare(&ctx_, &prepareReq, &prepareResponder, cq_, cq_, this);
                break;
            case types::ACCEPT:
                service_->RequestAccept(&ctx_, &acceptReq, &acceptResponder, cq_, cq_, this);
                break;
            case types::COMMIT:
                service_->RequestCommit(&ctx_, &commitReq, &commitResponder, cq_, cq_, this);
                break;
        }
        status_ = PROCESS;

    } else if (status_ == PROCESS) {
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.
        new CallData(service_, server_, cq_, callType);

        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
        switch (callType) {
            case types::TRANSFER:
                server_->processTransferCall(transferReq, transferRes);
                transferResponder.Finish(transferRes, Status::OK, this);
                status_ = FINISH;
                break;
            case types::GET_BALANCE:
                server_->processGetBalanceCall(getBalanceRes);
                getBalanceResponder.Finish(getBalanceRes, Status::OK, this);
                status_ = FINISH;
                break;
            case types::GET_LOGS:
                server_->processGetLogsCall(getLogsRes);
                getLogsResponder.Finish(getLogsRes, Status::OK, this);
                status_ = FINISH;
                break;
            case types::GET_DB_LOGS:
                server_->processGetDBLogsCall(getDBLogsRes);
                getDBLogsResponder.Finish(getDBLogsRes, Status::OK, this);
                status_ = FINISH;
                break;
            case types::PREPARE:
                server_->processPrepareCall(prepareReq, prepareRes);
                prepareResponder.Finish(prepareRes, Status::OK, this);
                status_ = FINISH;
                break;
            case types::ACCEPT:
                server_->processAcceptCall(acceptReq, acceptRes);
                acceptResponder.Finish(acceptRes, Status::OK, this);
                status_ = FINISH;
                break;
            case types::COMMIT:
                server_->processCommitCall(commitReq, commitRes);
                commitResponder.Finish(commitRes, Status::OK, this);
                status_ = FINISH;
                break;
        }
    } else {
        CHECK_EQ(status_, FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
    }
}
