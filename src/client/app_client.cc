#include <iostream>
#include <string>

#include "app_client.h"
#include "../utils/utils.h"
#include "../types/request_types.h"
#include <grpc/support/time.h>
#include "../constants.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using paxos::Paxos;
using paxos::Proposal;
using paxos::Logs;
using paxos::Transaction;
using paxos::TransferReq;
using paxos::TransferRes;
using paxos::PrepareReq;
using paxos::PrepareRes;
using paxos::AcceptRes;
using paxos::AcceptRes;
using paxos::CommitReq;
using paxos::CommitRes;

    
AppClient::AppClient() {
    for (auto it = Constants::serverAddresses.begin(); it != Constants::serverAddresses.end(); it++) {
        std::string server = it->first;
        std::string targetAddress = it->second;
        stubs_.push_back(Paxos::NewStub(grpc::CreateChannel(it->first, grpc::InsecureChannelCredentials())));
    }
}

void AppClient::processTransactions(std::vector<types::Transaction> transactions) {
    int requestsIssued = 0;
    std::vector<bool> issued(transactions.size(), false);

    while (requestsIssued < transactions.size()) {
        checkAndConsumeTransferReply();
        for (int i = 0; i < transactions.size(); i++) {
            auto& t = transactions[i];
            if (issued[i] || transferringServers.find(t.sender) != transferringServers.end()) {
                continue;
            } else {
                transferringServers.insert(t.sender);
                sendTransferAsync(t.sender, t.receiver, t.amount);
                issued[i] = true;
                requestsIssued++;
            }
        }
    }
}

int AppClient::GetBalance(std::string serverName) {
    sendGetBalanceAsync(serverName);

    void* tag;
    bool ok;

    while (true) {
        grpc::CompletionQueue::NextStatus responseStatus = cq_.AsyncNext(&tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
        if (responseStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
            if (ok) {
                ClientCall* call = static_cast<ClientCall*>(tag);
                if (call->callType_ == types::GET_BALANCE) {
                    return call->getBalanceReply.amount();
                }
            } else {
                delete static_cast<ClientCall*>(tag);
            }
        } 
    }
}

std::vector<types::Transaction> AppClient::GetLogs(std::string serverName) {
    sendGetLogsAsync(serverName);

    void* tag;
    bool ok;
    std::vector<types::Transaction> logs;

    while (true) {
        grpc::CompletionQueue::NextStatus responseStatus = cq_.AsyncNext(&tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
        if (responseStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
            if (ok) {
                ClientCall* call = static_cast<ClientCall*>(tag);
                if (call->callType_ == types::GET_LOGS) {
                    for (int i = 0; i < call->getLogsReply.logs_size(); i++) {
                        const Transaction& t = call->getLogsReply.logs(i);
                        logs.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
                    }
                    return logs;
                }
            } else {
                delete static_cast<ClientCall*>(tag);
            }
        } 
    }
}

std::vector<types::Transaction> AppClient::GetDBLogs(std::string serverName) {
    sendGetDBLogsAsync(serverName);

    void* tag;
    bool ok;
    std::vector<types::Transaction> logs;

    while (true) {
        grpc::CompletionQueue::NextStatus responseStatus = cq_.AsyncNext(&tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
        if (responseStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
            if (ok) {
                ClientCall* call = static_cast<ClientCall*>(tag);
                if (call->callType_ == types::GET_DB_LOGS) {
                    for (int i = 0; i < call->getDBLogsReply.logs_size(); i++) {
                        const Transaction& t = call->getDBLogsReply.logs(i);
                        logs.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
                    }
                    return logs;
                }
            } else {
                delete static_cast<ClientCall*>(tag);
            }
        } 
    }
}

int AppClient::getServerIdFromName(std::string serverName) {
    return serverName[1] - '0';
}

void AppClient::checkAndConsumeTransferReply() {
    // check if a transfer response is available in completion queue
    // if yes, remove the target server from transferringServers so the next transfer request
    // for that server can be issued
    void* tag;
    bool ok;
    grpc::CompletionQueue::NextStatus responseStatus = cq_.AsyncNext(&tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));

    if (responseStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
        if (ok) {
            ClientCall* call = static_cast<ClientCall*>(tag);
            if (call->callType_ == types::TRANSFER && call->transferReply.ack()) {
                transferringServers.erase(call->transferReply.server_id());
            }
        } else {
            delete static_cast<ClientCall*>(tag);
        }
    } 
}

bool AppClient::sendTransferAsync(std::string sender, std::string receiver, int amount) {
    int serverId = getServerIdFromName(sender);
    
    TransferReq request;
    request.set_receiver(receiver);
    request.set_amount(amount);
    
    ClientCall* call = new ClientCall;
    call->callType_ = types::TRANSFER;
    call->targetServer = sender;
    call->transferResponseReader = stubs_[serverId - 1]->PrepareAsyncTransfer(&call->context, request, &cq_);
    call->transferResponseReader->StartCall();
    call->transferResponseReader->Finish(&call->transferReply, &call->status, (void*)call);

    return true;
}

bool AppClient::sendGetBalanceAsync(std::string serverName) {
    int serverId = getServerIdFromName(serverName);
    
    google::protobuf::Empty request;
    
    ClientCall* call = new ClientCall;
    call->callType_ = types::GET_BALANCE;
    call->targetServer = serverName;
    call->balanceResponseReader = stubs_[serverId - 1]->PrepareAsyncGetBalance(&call->context, request, &cq_);
    call->balanceResponseReader->StartCall();
    call->balanceResponseReader->Finish(&call->getBalanceReply, &call->status, (void*)call);

    return true;
}

bool AppClient::sendGetLogsAsync(std::string serverName) {
    int serverId = getServerIdFromName(serverName);
    
    google::protobuf::Empty request;
    
    ClientCall* call = new ClientCall;
    call->callType_ = types::GET_LOGS;
    call->targetServer = serverName;
    call->logsResponseReader = stubs_[serverId - 1]->PrepareAsyncGetLogs(&call->context, request, &cq_);
    call->logsResponseReader->StartCall();
    call->logsResponseReader->Finish(&call->getLogsReply, &call->status, (void*)call);

    return true;
}

bool AppClient::sendGetDBLogsAsync(std::string serverName) {
    int serverId = getServerIdFromName(serverName);
    
    google::protobuf::Empty request;
    
    ClientCall* call = new ClientCall;
    call->callType_ = types::GET_DB_LOGS;
    call->targetServer = serverName;
    call->dbLogsResponseReader = stubs_[serverId - 1]->PrepareAsyncGetDBLogs(&call->context, request, &cq_);
    call->dbLogsResponseReader->StartCall();
    call->dbLogsResponseReader->Finish(&call->getDBLogsReply, &call->status, (void*)call);

    return true;
}
