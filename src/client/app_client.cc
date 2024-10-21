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

    
AppClient::AppClient() {
    for (auto it = Constants::serverAddresses.begin(); it != Constants::serverAddresses.end(); it++) {
        std::string server = it->first;
        std::string targetAddress = it->second;
        stubs_.push_back(Paxos::NewStub(grpc::CreateChannel(targetAddress, grpc::InsecureChannelCredentials())));
    }
}

void AppClient::processTransactions(std::vector<types::Transaction> transactions) {
    int requestsIssued = 0;
    std::vector<bool> issued(transactions.size(), false);

    // while (requestsIssued < transactions.size()) {
        // checkAndConsumeTransferReply();
        for (int i = 0; i < transactions.size(); i++) {
            auto& t = transactions[i];
            // if (issued[i] || transferringServers.find(t.sender) != transferringServers.end()) {
                // continue;
            // } else {
                // std::cout << "issuing " << t.sender << ", " << t.receiver << ", " << t.amount << std::endl;
                // transferringServers.insert(t.sender);
                sendTransferAsync(t.sender, t.receiver, t.amount);
                // issued[i] = true;
                // requestsIssued++;
            // }
        }
    // }
}

void AppClient::GetBalance(std::string serverName, int& res) {
    sendGetBalanceAsync(serverName);

    void* tag;
    bool ok;

    while (true) {
        grpc::CompletionQueue::NextStatus responseStatus = cq_.AsyncNext(&tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
        if (responseStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
            ClientCall* call = static_cast<ClientCall*>(tag);
            if (ok) {
                if (!call->status.ok()) {
                    std::cout << call->status.error_message() << std::endl;
                } else if (call->callType_ == types::GET_BALANCE) {
                    res = call->getBalanceReply.amount();
                    // std::cout << serverName << " get balance reply" << std::endl;
                    delete call;
                    break;
                }
            } 

            delete call;
        } 
    }
}

void AppClient::GetLogs(std::string serverName, std::vector<types::Transaction>& logs) {
    sendGetLogsAsync(serverName);

    void* tag;
    bool ok;

    while (true) {
        grpc::CompletionQueue::NextStatus responseStatus = cq_.AsyncNext(&tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
        if (responseStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
            ClientCall* call = static_cast<ClientCall*>(tag);
            if (ok) {
                if (!call->status.ok()) {
                    std::cout << call->status.error_message() << std::endl;
                } else if (call->callType_ == types::GET_LOGS) {
                    for (int i = 0; i < call->getLogsReply.logs_size(); i++) {
                        const Transaction& t = call->getLogsReply.logs(i);
                        logs.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
                    }
                    // std::cout << serverName << " get logs reply" << std::endl;
                    delete call;
                    break;
                }
            }

            delete call;
        } 
    }
}

void AppClient::GetDBLogs(std::string serverName, std::vector<types::TransactionBlock>& blocks) {
    sendGetDBLogsAsync(serverName);

    void* tag;
    bool ok;

    while (true) {
        grpc::CompletionQueue::NextStatus responseStatus = cq_.AsyncNext(&tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
        if (responseStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
            ClientCall* call = static_cast<ClientCall*>(tag);
            if (ok) {
                if (!call->status.ok()) {
                    std::cout << call->status.error_message() << std::endl;
                } else if (call->callType_ == types::GET_DB_LOGS) {
                    for (int i = 0; i < call->getDBLogsReply.blocks_size(); i++) {
                        const TransactionBlock& b = call->getDBLogsReply.blocks(i);
                        types::TransactionBlock block;
                        block.id = b.block_id();
                        block.commitProposal.proposalNum = b.proposal().number();
                        block.commitProposal.serverName = b.proposal().server_id();

                        for (int j = 0; j < b.logs().logs_size(); j++) {
                            const Transaction& t = b.logs().logs(j);
                            block.block.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
                        }

                        blocks.push_back(block);
                    }

                    delete call;
                    break;
                }
            }

            delete call;
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
        ClientCall* call = static_cast<ClientCall*>(tag);
        if (ok) {
            if (!call->status.ok()) {
                std::cout << call->status.error_message() << std::endl;
            } else if (call->callType_ == types::TRANSFER) {
                std::cout << "transfer reply " << call->transferReply.server_id() << " " << call->transferReply.ack() << std::endl;
                transferringServers.erase(call->transferReply.server_id());
            }
        }

        delete call;
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
