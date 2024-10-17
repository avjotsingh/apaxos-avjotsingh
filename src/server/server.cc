#include <string.h>
#include <set>
#include <grpcpp/grpcpp.h>
#include "paxos.grpc.pb.h"
#include "../types/transaction.h"
#include "../client/client_pool.h"

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
  
  types::Proposal acceptedProposal;
  std::vector<types::Transaction> acceptedLogs;
  std::string acceptedBlockId;

  int lastCommittedBlock;
  std::vector<types::Transaction> localLogs;
  std::vector<types::TransactionBlock> committedBlocks;

  std::vector<PaxosClient*> clients;


  void populateAcceptNum(Proposal* accept_num) {
    accept_num->set_number(acceptedProposal.proposalNum);
    accept_num->set_server_id(acceptedProposal.serverName);
  }

  void populateLocalLogs(Logs* logs) {
    for (auto l : this->localLogs) {
          Transaction* t = logs->add_logs();
          t->set_id(l.id);
          t->set_sender(l.sender);
          t->set_receiver(l.receiver);
          t->set_amount(l.amount);
        }
  }

  void acceptLogs(std::vector<types::Transaction> logs) {
    // Update the client's balance
    for (auto log : logs) {
      if (log.receiver == serverName) {
        balance += log.amount;
      }
    }

    std::set<types::Transaction> existing(localLogs.begin(), localLogs.end());
    std::set<types::Transaction> incoming(logs.begin(), logs.end());
    std::set<types::Transaction> result;

    std::set_difference(existing.begin(), existing.end(), 
      incoming.begin(), incoming.end(), 
      std::inserter(result, result.begin()));

    localLogs.clear();
    for (auto log : result) {
      localLogs.push_back({ log.id, log.sender, log.receiver, log.amount });
    }
  }

  void commitLogs() {
    committedBlocks.push_back({ acceptedBlockId, acceptedLogs });
  }

  void populateAcceptedVal(Logs* logs) {
    for (auto l : this->acceptedLogs) {
          Transaction* t = logs->add_logs();
          t->set_id(l.id);
          t->set_sender(l.sender);
          t->set_receiver(l.receiver);
          t->set_amount(l.amount);
        }
  }


public:
  PaxosServer(std::string serverName) {
    this->serverName = serverName;
    this->balance = 100;
    this->currentProposalNum = 0;
    this->currentTransactionNum = 0;

    this->acceptedProposal = { 0, "" };
    this->acceptedLogs = std::vector<types::Transaction>();
    this->acceptedBlockId = "";

    this->lastCommittedBlock = 0;
    this->localLogs = std::vector<types::Transaction>();
    this->committedBlocks = std::vector<types::TransactionBlock>();

    this->clients = ClientPool::getClientsForServer(serverName);
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

      // TODO: Implement consensus
      
      // Send prepare requests to all other servers
      int numPromises = 0;
      int acceptNum = 0;
      
      for (auto c : clients) {
        PrepareRes res = c->Prepare(*req);
        if (res.ack()
          && res.proposal().number() == currentProposalNum
          && res.proposal().server_id() == serverName) {
            numPromises++;
            res.logs()
          }
 
      }

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

    Proposal* proposal = response->mutable_proposal();
    proposal->set_number(proposalNum);
    proposal->set_server_id(proposerId);
    
    if (this->acceptedProposal.proposalNum == 0 
      && proposalNum > this->currentProposalNum
      && lastCommittedBlock >= this->lastCommittedBlock) {
        // acceptor has not accepted proposal from anybody else
        // proposer's committed log is as big as acceptor's
        this->acceptedProposal.proposalNum = proposalNum;
        this->acceptedProposal.serverName = proposerId;
        this->currentProposalNum = proposalNum;

        response->set_ack(true);
        populateLocalLogs(response->mutable_logs())
        
    } else if (proposalNum > this->currentProposalNum
      && lastCommittedBlock >= this->lastCommittedBlock) {
        // acceptor has accepted a proposal from somebody else
        res->set_ack(true);
        // populateLocalLogs(response->mutable_logs());
        populateAcceptNum(response->mutable_accept_num());
        populateAcceptedVal(response->mutable_accept_val());
        
      } else {
        // the proposer needs to catch up
        res->set_ack(false);
        populateLocalLogs(response->mutable_logs());
      }
      
    return Status::OK;
  }


  Status Accept(ServerContext* context, const AcceptReq* request, AcceptRes* response) override {
    int proposalNum = request->proposal().number();
    std::string proposerId = request->proposal().server_id();

    if (proposalNum == acceptedProposal.proposalNum
      && proposerId == acceptedProposal.serverName) {
        // The accept request matches the last promised request
        std::vector<types::Transaction> logs;
        for (int i = 0; i < request->block_size(); i++) {
          Transaction& t = request->block(i);
          logs.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
        }

        acceptLogs(logs);
        acceptedBlockId = request->block_id();
        response->set_ack(true);
      } else {
        response->set_ack(false);
      }

    return Status::OK;
  }

  Status Commit(ServerContext* context, const CommitReq* request, CommitRes* response) override {
    int blockId = request->block_id();
    if (blockId == acceptedBlockId) {
      // commit request matches the last accepted request
      std::vector<types::Transaction> logs;
      for (int i = 0; i < request->block_size(); i++) {
        Transaction& t = request->block(i);
        logs.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
      }

      commitLogs(logs);
      acceptedProposal.proposalNum = 0;
      acceptedProposal.serverName = "";
      acceptedLogs.clear();
      acceptedBlockId = "";
      lastCommittedBlock = blockId;

      response->set_ack(true);
    } else {
      response->set_ack(false);
    }

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