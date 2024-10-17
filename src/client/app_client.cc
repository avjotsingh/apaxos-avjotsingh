#include <stdexcept>
#include <vector>
#include "app_client.h"

using grpc::Status;
using google::protobuf::Empty;

using paxos::TransferReq;
using paxos::TransferRes;
using paxos::Balance;
using paxos::Logs;
using paxos::Transaction;


AppClient::AppClient(std::string serverName, std::string targetAddress) {
    this->serverName = serverName;
    this->channel = grpc::CreateChannel(targetAddress, grpc::InsecureChannelCredentials()),
    this->stub_ = Paxos::NewStub(channel);
}

bool AppClient::TransferAmount(std::string receiver, int amount) {
    TransferReq req;
    req.set_receiver(receiver);
    req.set_amount(amount);

    ClientContext context;
    TransferRes res;
    Status status = stub_->Transfer(&context, req, &res);

    if (status.ok()) {
        return res.ack();
    } else {
        throw std::runtime_error("Exception: [" + this->serverName + "] " + std::to_string(status.error_code()) + " " + status.error_message());        
    }
}

int AppClient::GetBalance() {
    Empty req;
    ClientContext context;
    Balance res;
    Status status = stub_->GetBalance(&context, req, &res);

    if (status.ok()) {
        return res.amount();
    } else {
        throw std::runtime_error("Exception: " + std::to_string(status.error_code()) + " " + status.error_message());        
    }
} 

std::vector<types::Transaction> AppClient::GetLogs() {
    Empty req;
    ClientContext context;
    Logs res;
    Status status = stub_->GetLogs(&context, req, &res);

    if (status.ok()) {
        std::vector<types::Transaction> logs;
        int logs_count = res.logs_size();
        if (logs_count > 0) {
            for (int i = 0; i < logs_count; i++) {
                int id = res.logs(i).id();
                std::string sender = res.logs(i).sender();
                std::string receiver = res.logs(i).receiver();
                int amount = res.logs(i).amount();
                logs.push_back({ id, sender, receiver, amount });
            }
        }

        return logs;
    } else {
        throw std::runtime_error("Exception: " + std::to_string(status.error_code()) + " " + status.error_message());        
    }
} 

std::vector<types::Transaction> AppClient::GetDBLogs() {
    Empty req;
    ClientContext context;
    Logs res;
    Status status = stub_->GetDBLogs(&context, req, &res);

    if (status.ok()) {
        int logs_count = res.logs_size();
        std::vector<types::Transaction> logs;
        if (logs_count > 0) {
            for (int i = 0; i < logs_count; i++) {
                int id = res.logs(i).id();
                std::string sender = res.logs(i).sender();
                std::string receiver = res.logs(i).receiver();
                int amount = res.logs(i).amount();
                logs.push_back({ id, sender, receiver, amount });
            }
        }

        return logs;
    } else {
        throw std::runtime_error("Exception: " + std::to_string(status.error_code()) + " " + status.error_message());        
    }
}