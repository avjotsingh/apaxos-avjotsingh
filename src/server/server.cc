#include <string.h>
#include <grpcpp/grpcpp.h>
#include "paxos.grpc.pb.h"
#include "../types/transaction.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using google::protobuf::Empty;

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
class PaxosServer final : public Paxos::Service {
private:
  std::string serverName;
  int balance;
  int currentProposalNum;
  int currentTransactionNum;
  int acceptedProposalNum;
  int lastCommittedBlock;
  std::vector<types::Transaction> acceptedTransactions;
  std::vector<types::Transaction> localLogs;
  std::vector<types::Transaction> dbLogs;


public:
  PaxosServer(std::string serverName) {
    this->serverName = serverName;
    this->balance = 100;
    this->currentProposalNum = 0;
    this->currentTransactionNum = 0;
    this->acceptedProposalNum = 0;
    this->acceptedTransactions = std::vector<types::Transaction>();

    this->localLogs = std::vector<types::Transaction>();
    this->dbLogs = std::vector<types::Transaction>();
  }
    

  Status Transfer(ServerContext* context, const TransferReq* request, TransferRes* response) override {
    std::string sender = this->serverName;
    std::string receiver = request->receiver();
    int amount = request->amount();

    if (amount <= this->balance) {
      this->currentTransactionNum++;
      this->localLogs.push_back({ std::to_string(this->currentTransactionNum), sender, receiver, amount });
      this->balance -= amount;
    } else {
      // Send prepare requests to all other servers
      this->currentProposalNum += 1;
      PrepareReq* req;
      Proposal* proposal = req->mutable_proposal();
      proposal->set_number(this->currentProposalNum);
      proposal->set_server_id(this->serverName);
      req->set_last_committed_block(this->lastCommittedBlock);

    }


    response->set_ack(true);
    return Status::OK;
  }

  Status GetBalance(ServerContext* context, const Empty* request, Balance* response) override {
    response->set_amount(this->balance);
    return Status::OK;
  }

  Status GetLogs(ServerContext* context, const Empty* request, Logs* response) override {
    // TODO: Add logic to retrieve the persisted local logs
    
    for (types::Transaction& t: this->localLogs) {
      Transaction* transaction = response->add_logs();
      transaction->set_id(t.id);
      transaction->set_sender(t.sender);
      transaction->set_receiver(t.receiver);
      transaction->set_amount(t.amount);
    }

    return Status::OK;
  }

  Status GetDBLogs(ServerContext* context, const Empty* request, Logs* response) override {
    // TODO: Add logic to query the database and get all the committed logs

    for (types::Transaction& t: this->dbLogs) {
      Transaction* transaction = response->add_logs();
      transaction->set_id(t.id);
      transaction->set_sender(t.sender);
      transaction->set_receiver(t.receiver);
      transaction->set_amount(t.amount);
    }

    return Status::OK;
  }


  Status Prepare(ServerContext* context, const PrepareReq* request, PrepareRes* response) override {
    int proposalNum = request->proposal().number();
    std::string proposerId = request->proposal().server_id();
    int lastCommittedBlock = request->last_committed_block();

    std::cout << "[Prepare] " << "proposer " << proposerId << " proposal_num " << proposalNum << " last_commited_block " << lastCommittedBlock << std::endl;

    response->set_ack(true);
    Proposal* proposal = response->mutable_proposal();
    proposal->set_number(proposalNum);
    proposal->set_server_id(proposerId);

    Proposal* accept_num = response->mutable_accept_num();
    accept_num->set_number(proposalNum);
    accept_num->set_server_id(proposerId);

    
    return Status::OK;
  }


  Status Accept(ServerContext* context, const AcceptReq* request, AcceptRes* response) override {
    int proposalNum = request->proposal().number();
    int proposerId = request->proposal().number();
    std::string blockId = request->block_id();

    for (auto &transaction: request->block()) {
      std::cout << "Transaction: (" << transaction.sender() << ", " << transaction.receiver() << ", " << transaction.amount() << ")" << std::endl;
    }

    response->set_ack(true);
    return Status::OK;
  }

  Status Commit(ServerContext* context, const CommitReq* request, CommitRes* response) override {
    std::string blockId = request->block_id();

    response->set_ack(true);
    return Status::OK;
  }
};

void RunServer(std::string serverName, std::string serverAddress) {
  PaxosServer service(serverName);

  ServerBuilder builder;

  // Listen on the given address without any authentication mechanism
  builder.AddListeningPort(serverAddress, grpc::InsecureServerCredentials());
  
  // Register "service" as the instance through which
  // communication with client takes place
  builder.RegisterService(&service);

  // Assembling the server
  std::unique_ptr<Server> server(builder.BuildAndStart());
  // std::cout << "Server listening on port: " << serverAddress << std::endl;

  server->Wait();
}

int main(int argc, char** argv) {
  
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <server_name> <target_address>" << std::endl;
    return 1;
  }

  RunServer(argv[1], argv[2]);
  return 0;
}