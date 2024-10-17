#include "async_server.h"
#include "../types/request_types.h"
#include <shared_mutex>
#include "call_data.h"
#include "absl/log/check.h"
#include "../client/async_cluster_client.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using google::protobuf::Empty;

// Server Implementation
PaxosServer::PaxosServer(std::string serverName) {
  serverName = serverName;
  clusterClient = new PaxosClusterClient(serverName);
  
  balance = 100;
  currentProposalNum = 0;
  currentTransactionNum = 0;
  
  acceptedProposal.proposalNum = 0;
  acceptedProposal.serverName = "";
  acceptedLogs = std::vector<types::Transaction>();
  acceptedBlockId = 0;

  localLogs = std::vector<types::Transaction>();
  committedBlocks = std::vector<types::TransactionBlock>();
}

void PaxosServer::run(std::string targetAddress) {
  ServerBuilder builder;
  builder.AddListeningPort(targetAddress, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  cq = builder.AddCompletionQueue();
  server = builder.BuildAndStart();

  std::cout << "Server running on " << targetAddress << std::endl;

  sleep(5);
  HandleRPCs();
}

PaxosServer::~PaxosServer() {
  server->Shutdown();
  cq->Shutdown();
}

void PaxosServer::HandleRPCs() {
  new CallData(&service, this, cq.get(), types::TRANSFER);
  new CallData(&service, this, cq.get(), types::GET_BALANCE);
  new CallData(&service, this, cq.get(), types::GET_LOGS);
  new CallData(&service, this, cq.get(), types::GET_DB_LOGS);
  new CallData(&service, this, cq.get(), types::PREPARE);
  new CallData(&service, this, cq.get(), types::ACCEPT);
  new CallData(&service, this, cq.get(), types::COMMIT);

  void *tag;
  bool ok;

  while (true) {
    CHECK(cq->Next(&tag, &ok));
    CHECK(ok);
    static_cast<CallData*>(tag)->Proceed();
  }
}

void PaxosServer::populateAcceptNum(Proposal* acceptNum) {
  std::shared_lock<std::shared_mutex> lock(acceptedStateMutex);
  acceptNum->set_number(acceptedProposal.proposalNum);
  acceptNum->set_server_id(acceptedProposal.serverName);
}

void PaxosServer::populateLocalLogs(Logs* logs) {
  std::shared_lock<std::shared_mutex> lock(localLogsMutex);
  for (auto l : localLogs) {
        Transaction* t = logs->add_logs();
        t->set_id(l.id);
        t->set_sender(l.sender);
        t->set_receiver(l.receiver);
        t->set_amount(l.amount);
      }
}

void PaxosServer::acceptLogs(std::vector<types::Transaction> logs) {
  std::unique_lock<std::shared_mutex> lock1(acceptedStateMutex);
  std::unique_lock<std::shared_mutex> lock2(localLogsMutex);

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

void PaxosServer::commitLogs() {
  std::unique_lock<std::shared_mutex> lock(committedBlockMutex);
  committedBlocks.push_back({ acceptedBlockId, acceptedLogs });
}

void PaxosServer::populateAcceptVal(Logs* logs) {
  std::shared_lock<std::shared_mutex> lock(acceptedStateMutex);
  for (auto l : this->acceptedLogs) {
        Transaction* t = logs->add_logs();
        t->set_id(l.id);
        t->set_sender(l.sender);
        t->set_receiver(l.receiver);
        t->set_amount(l.amount);
      }
}

void PaxosServer::processTransferCall(TransferReq& req, TransferRes& res) {
  std::cout << "processing transfer..." << std::endl;
  // std::string sender = serverName;
  // std::string receiver = req.receiver();
  // int amount = req.amount();

  // std::unique_lock<std::shared_mutex> lock(currentStateMutex);
  // if (amount <= balance) {
  //   currentTransactionNum++;
  //   localLogs.push_back({ std::to_string(currentTransactionNum), sender, receiver, amount });
  //   balance -= amount;
  // } else {
  //   // Send prepare requests to all other servers
  //   currentProposalNum += 1;
  //   PrepareReq* req;
  //   Proposal* proposal = req->mutable_proposal();
  //   proposal->set_number(this->currentProposalNum);
  //   proposal->set_server_id(this->serverName);
  //   req->set_last_committed_block(this->lastCommittedBlock);





  //   // TODO: Implement consensus
    
  //   // Send prepare requests to all other servers
  //   int numPromises = 0;
  //   int acceptNum = 0;
    
  //   for (auto c : clients) {
  //     PrepareRes res = c->Prepare(*req);
  //     if (res.ack()
  //       && res.proposal().number() == currentProposalNum
  //       && res.proposal().server_id() == serverName) {
  //         numPromises++;
  //         res.logs()
  //       }

  //   }

  // }


  // response->set_ack(true);
}

void PaxosServer::processGetBalanceCall(Balance& res) {
  std::cout << "Processing get balance..." << std::endl;
}

void PaxosServer::processGetLogsCall(Logs& logs) {
  std::cout << "processing logs..." << std::endl;
}

void PaxosServer::processGetDBLogsCall(Logs& logs) {
  std::cout << "processing db logs..." << std::endl;
}

void PaxosServer::processPrepareCall(PrepareReq &req, PrepareRes& res) {
  std::cout << "Processing prepare call..." << std::endl;
}

void PaxosServer::processAcceptCall(AcceptReq& req, AcceptRes& res) {
  std::cout << "Processing accept..." << std::endl;
}

void PaxosServer::processCommitCall(CommitReq& req, CommitRes& res) {
  std::cout << "processing commit..." << std::endl;
}


void RunServer(std::string serverName, std::string serverAddress) {
  PaxosServer server(serverName);
  server.run(serverAddress);
}

int main(int argc, char** argv) {
  
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <server_name> <target_address>" << std::endl;
    return 1;
  }

  RunServer(argv[1], argv[2]);
  return 0;
}