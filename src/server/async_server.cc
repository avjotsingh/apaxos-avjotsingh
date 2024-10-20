#include "async_server.h"
#include "../types/request_types.h"
#include <shared_mutex>
#include "call_data.h"
#include "absl/log/check.h"
#include <chrono>
#include "paxos_client_call.h"
#include <grpc/support/time.h>
#include "../constants.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using google::protobuf::Empty;

// Server Implementation
PaxosServer::PaxosServer(std::string serverName) {
  srand(serverName[1] - '0');
  serverName = serverName;
  
  balance = 100;
  currentTransactionNum = 0;
  rpcTimeoutSeconds = 1;
  
  myProposal.proposalNum = 0;
  myProposal.serverName = serverName;
  acceptNum.proposalNum = 0;
  acceptNum.serverName = "";
  highestAcceptNum.proposalNum = 0;
  highestAcceptNum.serverName = "";
  highestAcceptVal = std::vector<types::Transaction>();

  localLogs = std::vector<types::Transaction>();
  remoteLogs = std::vector<types::Transaction>();
  acceptVal = std::vector<types::Transaction>();
  
  lastCommittedBlock = 0;
  lastCommittedProposal.proposalNum = 0;
  lastCommittedProposal.serverName = "";
  committedBlocks = std::vector<types::TransactionBlock>();

  prepareSuccesses = 0;
  prepareFailures = 0;
  acceptSuccesses = 0;
  acceptFailures = 0;
  commitSuccesses = 0;
  commitFailures = 0;

  currentState_ = IDLE;
}

void PaxosServer::run(std::string targetAddress) {
  ServerBuilder builder;
  builder.AddListeningPort(targetAddress, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  requestCQ = builder.AddCompletionQueue();
  server = builder.BuildAndStart();

  for (auto it = Constants::serverAddresses.begin(); it != Constants::serverAddresses.end(); it++) {
    std::string server = it->first;
    std::string targetAddress = it->second;
    stubs_.push_back(Paxos::NewStub(grpc::CreateChannel(it->first, grpc::InsecureChannelCredentials())));    
  }

  sleep(1);
  HandleRPCs();
}

PaxosServer::~PaxosServer() {
  server->Shutdown();
  requestCQ->Shutdown();
  responseCQ->Shutdown();
}

void PaxosServer::HandleRPCs() {
  new CallData(&service, this, requestCQ.get(), types::TRANSFER);
  new CallData(&service, this, requestCQ.get(), types::GET_BALANCE);
  new CallData(&service, this, requestCQ.get(), types::GET_LOGS);
  new CallData(&service, this, requestCQ.get(), types::GET_DB_LOGS);
  new CallData(&service, this, requestCQ.get(), types::PREPARE);
  new CallData(&service, this, requestCQ.get(), types::ACCEPT);
  new CallData(&service, this, requestCQ.get(), types::COMMIT);


  void* requestTag;
  bool requestOk;
  void* responseTag;
  bool responseOk;

  while (true) {
      // Poll the request queue
      grpc::CompletionQueue::NextStatus requestStatus = 
          requestCQ->AsyncNext(&requestTag, &requestOk, gpr_time_0(GPR_CLOCK_REALTIME));

      // Poll the response queue
      grpc::CompletionQueue::NextStatus responseStatus = 
          responseCQ->AsyncNext(&responseTag, &responseOk, gpr_time_0(GPR_CLOCK_REALTIME));

      // Handle request events
      if (requestStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
          if (requestOk) {
              static_cast<CallData*>(requestTag)->Proceed();  // Process request
          } else {
              delete static_cast<CallData*>(requestTag);  // Handle failure
          }
      }

      // Handle response events
      if (responseStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
          if (responseOk) {
              static_cast<PaxosClientCall*>(responseTag)->HandleRPCResponse();  // Process response
          } else {
              delete static_cast<PaxosClientCall*>(responseTag);  // Handle failure
          }
      }
  }
}


int PaxosServer::getServerIdFromName(std::string serverName) {
  return serverName[1] - '0';
}

/*
 * Helper methods for consensus
 */

void PaxosServer::sendPrepareToCluster(PrepareReq& request) {
  std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(rpcTimeoutSeconds);
  for (int i = 0; i < stubs_.size(); i++) {
      sendPrepareAsync(request, stubs_[i], deadline);
  }
}

void PaxosServer::sendAcceptToCluster(AcceptReq& request) {
  std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(rpcTimeoutSeconds);
  for (int i = 0; i < stubs_.size(); i++) {
      sendAcceptAsync(request, stubs_[i], deadline);
  }
}

void PaxosServer::sendCommitToCluster(CommitReq& request) {
  std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(rpcTimeoutSeconds);
  for (int i = 0; i < stubs_.size(); i++) {
      sendCommitAsync(request, stubs_[i], deadline);
  }
}

void PaxosServer::sendPrepareAsync(PrepareReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline) {
  PaxosClientCall call(this, responseCQ.get(), types::PREPARE);
  call.sendPrepare(request, stub_, deadline);
}

void PaxosServer::sendAcceptAsync(AcceptReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline) {
  PaxosClientCall call(this, responseCQ.get(), types::ACCEPT);
  call.sendAccept(request, stub_, deadline);
}

void PaxosServer::sendCommitAsync(CommitReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline) {
  PaxosClientCall call(this, responseCQ.get(), types::COMMIT);
  call.sendCommit(request, stub_, deadline);
}

void PaxosServer::sendSuccessAsync(SuccessReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline) {
  PaxosClientCall call(this, responseCQ.get(), types::SUCCESS);
  call.sendSuccess(request, stub_, deadline);
}

void PaxosServer::handlePrepareReply(PrepareRes& reply) {
  if (currentState_ != PREPARE) {
    // ignore the prepare replies
    return;
  }

  if (reply.proposal().number() == myProposal.proposalNum && reply.ack()) {
    prepareSuccesses++;
    if (reply.has_logs() && highestAcceptVal.empty()) {
      for (int i = 0; i < reply.logs().logs_size(); i++) {
        const Transaction& t = reply.logs().logs(i);
        remoteLogs.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
      }
    } else if (reply.has_accept_num() && reply.accept_num().number() > highestAcceptNum.proposalNum) {
      highestAcceptNum.proposalNum = reply.accept_num().number();
      highestAcceptNum.serverName = reply.accept_num().server_id();

      remoteLogs.clear();
      highestAcceptVal.clear();

      for (int i = 0; i < reply.accept_val().logs_size(); i++) {
        const Transaction& t = reply.accept_val().logs(i);
        highestAcceptVal.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
      }
    }
  } else if (reply.proposal().number() == myProposal.proposalNum && !reply.ack()) {
    prepareFailures++;
  }
}

void PaxosServer::handleAcceptReply(AcceptRes& reply) {
  if (currentState_ != PROPOSE) {
    // ignore the accept replies
    return;
  }

  if (reply.proposal().number() == myProposal.proposalNum && reply.ack()) {
    acceptSuccesses++;
  } else if (reply.proposal().number() == myProposal.proposalNum && !reply.ack()) {
    if (reply.has_last_committed_block()) {
      // Synchronize the server
      replicateBlock(reply.server_id(), reply.last_committed_block() + 1);
    } 
    acceptFailures++;
  }
}

void PaxosServer::handleCommitReply(CommitRes& reply) {
  if (currentState_ != COMMIT) {
    // ignore commit replies
    return;
  }

  if (reply.proposal().number() == myProposal.proposalNum && reply.ack()) {
    commitSuccesses++;
  } else if (reply.proposal().number() == myProposal.proposalNum && !reply.ack()) {
    commitFailures++;
  }
}

void PaxosServer::handleSuccessReply(SuccessRes& reply) {
  if (!reply.ack()) {
    // retry success rpc
    replicateBlock(reply.server_id(), reply.last_committed_block() + 1);
  } else {
    // replicate subsequent blocks if the server is still lagging behind
    if (reply.last_committed_block() < lastCommittedBlock) {
      replicateBlock(reply.server_id(), reply.last_committed_block() + 1);
    }
  }
}

void PaxosServer::replicateBlock(std::string serverName, int blockId) {
  SuccessReq req;
  Proposal* proposal = req.mutable_proposal();
  proposal->set_number(committedBlocks.at(blockId - 1).commitProposal.proposalNum);
  proposal->set_server_id(committedBlocks.at(blockId - 1).commitProposal.serverName);

  req.set_block_id(blockId);
  
  Logs* logs = req.mutable_block();
  for (auto& log : committedBlocks.at(blockId).block) {
    Transaction* t = logs->add_logs();
    t->set_id(log.id);
    t->set_sender(log.sender);
    t->set_receiver(log.receiver);
    t->set_amount(log.amount);
  }
  
  int serverId = getServerIdFromName(serverName);
  std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(rpcTimeoutSeconds);
  sendSuccessAsync(req, stubs_.at(serverId - 1), deadline);
}

void PaxosServer::populateProposal(Proposal* to, types::Proposal from) {
  to->set_number(from.proposalNum);
  to->set_server_id(from.serverName);
}

void PaxosServer::populateAcceptNum(Proposal* acceptNum) {
  acceptNum->set_number(this->acceptNum.proposalNum);
  acceptNum->set_server_id(this->acceptNum.serverName);
}

void PaxosServer::populateLocalLogs(Logs* logs) {
  for (auto l : localLogs) {
        Transaction* t = logs->add_logs();
        t->set_id(l.id);
        t->set_sender(l.sender);
        t->set_receiver(l.receiver);
        t->set_amount(l.amount);
      }
}

void PaxosServer::populateAcceptVal(Logs* logs) {
  for (auto l : this->acceptVal) {
        Transaction* t = logs->add_logs();
        t->set_id(l.id);
        t->set_sender(l.sender);
        t->set_receiver(l.receiver);
        t->set_amount(l.amount);
      }
}

bool PaxosServer::processTransferCall(TransferReq* req, TransferRes* res) {

  bool success;

  switch (currentState_) {
    case IDLE:
    case PROMISED:
    case ACCEPTED:
      // server can process the client transaction
      if (req->amount() <= balance) {
        currentTransactionNum++;
        localLogs.push_back({ currentTransactionNum, serverName, req->receiver(), req->amount() });
        balance -= req->amount();

        res->set_ack(true);
        success = true;
      } else {

        // initiate consensus
        currentState_ = PREPARE;
        // Compose a Prepare request
        myProposal.proposalNum = myProposal.proposalNum + 1;
        myProposal.serverName = serverName;
        
        highestAcceptNum.proposalNum = 0;
        highestAcceptNum.serverName = "";

        prepareSuccesses = 0;
        prepareFailures = 0;
        remoteLogs.clear();

        PrepareReq prepareReq;
        populateProposal(prepareReq.mutable_proposal(), myProposal);
        prepareReq.set_last_committed_block(lastCommittedBlock);
        
        // send a prepare request to cluster
        sendPrepareToCluster(prepareReq);

        // return false to indicate that the request is not fully processed yet
        success = false;
      }
      break;

    case PREPARE:
      // send accept request to other servers
      if (prepareSuccesses >= MAJORITY) {
        // prepare success
        currentState_ = PROPOSE;
        AcceptReq acceptReq;
        populateProposal(acceptReq.mutable_proposal(), myProposal);
        acceptReq.set_last_committed_block(lastCommittedBlock);
        
        // TODO: change this to have consolidated and sanitized logs from all servers
        populateAcceptVal(acceptReq.mutable_logs());

        acceptSuccesses = 0;
        acceptFailures = 0;
        sendAcceptToCluster(acceptReq);

      } else if (prepareFailures >= MAJORITY) {
        // prepare failure
        currentState_ = IDLE;
      }
      success = false;
      break;

    case PROPOSE:
      // send commit request to other servers
      if (acceptSuccesses >= MAJORITY) {
        // accept success
        currentState_ = COMMIT;
        CommitReq commitReq;
        populateProposal(commitReq.mutable_proposal(), myProposal);
        commitReq.set_last_committed_block(lastCommittedBlock);

        commitSuccesses = 0;
        commitFailures = 0;
        sendCommitToCluster(commitReq);

      } else if (acceptFailures >= MAJORITY) {
        // accept failure
        currentState_ = IDLE;
      }

      success = false;
      break;

    case COMMIT:
      if (commitSuccesses >= MAJORITY) {
        // commit success
        currentState_ = IDLE;

      } else if (commitFailures >= MAJORITY) {
        // commit failure. retry committing
        currentState_ = PROPOSE;
      }

      // Server will have updated logs now. Retrying the transaction should probably succeed
      success = false;
      break;
  }

  return success;
}

bool PaxosServer::processGetBalanceCall(Balance* res) {
  res->set_amount(balance);
  return false;
}

bool PaxosServer::processGetLogsCall(Logs* logs) {
  for (types::Transaction log : localLogs) {
    Transaction* t = logs->add_logs();
    t->set_id(log.id);
    t->set_sender(log.sender);
    t->set_receiver(log.receiver);
    t->set_amount(log.amount);
  }
  return true;
}

bool PaxosServer::processGetDBLogsCall(Logs* logs) {
  for (types::TransactionBlock block : committedBlocks) {
    for (types::Transaction log : block.block) {
      Transaction* t = logs->add_logs();
      t->set_id(log.id);
      t->set_sender(log.sender);
      t->set_receiver(log.receiver);
      t->set_amount(log.amount);
    }
  }
  return true;
}

bool PaxosServer::processPrepareCall(PrepareReq* req, PrepareRes* res) {
  res->mutable_proposal()->CopyFrom(req->proposal());

  switch (currentState_) {
    case IDLE:
    case PREPARE:
    case PROMISED:
      // just check the proposer's proposal number and last commited block
      if (req->proposal().number() > myProposal.proposalNum && req->last_committed_block() >= lastCommittedBlock) {
            myProposal.proposalNum = req->proposal().number();
            myProposal.serverName = req->proposal().server_id();
            populateLocalLogs(res->mutable_logs());
            res->set_ack(true);
            currentState_ = PROMISED;
      } else {
        res->set_ack(false);
      }
      break;

    case PROPOSE:
      // proposer cannot fall back to becoming an acceptor. it has to propose the value
      res->set_ack(false);
      break;
    
    case COMMIT:
      // proposer cannot fall back to becoming an acceptor, it has to commit the value
      res->set_ack(false);
      break;

    case ACCEPTED:
      // check if proposal is valid
      if (req->proposal().number() > acceptNum.proposalNum && req->last_committed_block() >= lastCommittedBlock) {
        myProposal.proposalNum = req->proposal().number();
        myProposal.serverName = req->proposal().server_id();
        populateAcceptNum(res->mutable_accept_num());
        populateAcceptVal(res->mutable_accept_val());
        res->set_ack(true);
        
      } else {
        res->set_ack(false);
      }
      break;
  }

  return true;
}

bool PaxosServer::processAcceptCall(AcceptReq* req, AcceptRes* res) {
  res->mutable_proposal()->CopyFrom(req->proposal());
  res->set_server_id(serverName);

  switch (currentState_) {
    case IDLE:
    case PREPARE:
    case PROPOSE:
    case PROMISED:
    case ACCEPTED:
    case COMMIT:
      // accept the value conditionally 
      // [IDLE] : server was not issued any transaction, and was not involved in consensus
      // [PREPARE] : server started consensus, but in the meantime another leader got elected who is now sending accept requests
      // [PROPOSE] : server was slow, before it could send out accept requests another leader got elected who is now sending accept requests
      // [PROMISED] : server promised to receive value in response to prepare requests
      // [ACCEPTED] : minority server accepted a value in previous round. A new value is proposed in the second round since this server did not get a prepare request
      // [COMMIT] : A partition prevented the server from committing. In the meantime, a new leader came in and committed the accepted block
      if (req->proposal().number() >= acceptNum.proposalNum && req->last_committed_block() >= lastCommittedBlock) {

        // accept request is trying to insert at an index > lastCommitedBlock index
        // the server is either in sync or needs to catch up
        acceptNum.proposalNum = req->proposal().number();
        acceptNum.serverName = req->proposal().server_id();
        
        if (req->last_committed_block() == lastCommittedBlock) {
          // server is in sync. no catch up needed
          acceptVal.clear();
          for (int i = 0; i < req->logs().logs_size(); i++) {
            const Transaction& t = req->logs().logs(i);
            acceptVal.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
          }
          
          
          res->set_ack(true);
          currentState_ = ACCEPTED;
    
        } else {
          // catch up needed
          res->set_ack(false);
          res->set_last_committed_block(lastCommittedBlock);
        }
      } else {
        res->set_ack(false);
      }
  }  

  return true;
}

bool PaxosServer::processCommitCall(CommitReq* req, CommitRes* res) {
  res->mutable_proposal()->CopyFrom(req->proposal());

  switch (currentState_) {
    case ACCEPTED:
      // NOTE: we cannot have a case wherein req->proposal().number() == lastestProposal.number but req->proposal().server_id() != lastestProposal.serverName

      if (req->last_committed_block() < lastCommittedBlock) {
        // the committer might have experienced network parition.
        // in the meantime, another leader came up and committed the accepted block
        // block has already been committed. ignore the request
        res->set_ack(true);
      } else if (req->last_committed_block() > lastCommittedBlock) {
        // server is not in sync. cannot accept commit
        res->set_ack(false);
      } else if (req->proposal().number() > acceptNum.proposalNum) {
        // server does not have the latest accepted value. so it cannot commit
        res->set_ack(false);
      } else if (req->proposal().number() < acceptNum.proposalNum) {
        // committer is not a leader anymore
        res->set_ack(false);
      } else {
        // Commit only if the server had accepted a value in the past
        // and the proposal number from accept matches the proposal number from commit

        lastCommittedBlock++;
        lastCommittedProposal.proposalNum = req->proposal().number();
        lastCommittedProposal.serverName = req->proposal().server_id();
        committedBlocks.push_back({ lastCommittedBlock, lastCommittedProposal, acceptVal });
        
        // reset acceptNum and acceptVal
        acceptNum.proposalNum = 0;
        acceptNum.serverName = "";
        acceptVal.clear();
        
        res->set_ack(true);
        currentState_ = IDLE;      
      }

    default:
      res->set_ack(false);
      break;

  }
  return true;
}

bool PaxosServer::processSuccessCall(SuccessReq* req, SuccessRes* res) {
  // just make sure you are not adding a stale committed transaction block
  switch (currentState_) {
    case IDLE:
    case PREPARE:
    case PROPOSE:
    case PROMISED:
    case ACCEPTED:
    case COMMIT:
      // scenarios same as those listed in processAcceptCall.
      // If the server's committed log was behind, it would not have changed its state
      // In response to sending failed ack, it is now receiving success RPCs
      
      if (req->proposal().number() > lastCommittedProposal.proposalNum && req->block_id() == lastCommittedBlock + 1) {
        std::vector<types::Transaction> logsToCommit;
        for (int i = 0; i < req->block().logs_size(); i++) {
          const Transaction& t = req->block().logs(i);
          logsToCommit.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
        }


        lastCommittedBlock++;
        lastCommittedProposal.proposalNum = req->proposal().number();
        lastCommittedProposal.serverName = req->proposal().server_id();
        committedBlocks.push_back({ lastCommittedBlock, lastCommittedProposal, logsToCommit });
        
        res->set_ack(true);
        currentState_ = IDLE;      

      } else {
        res->set_ack(false);
      }

      res->set_server_id(serverName);
      res->set_last_committed_block(lastCommittedBlock);
      break;
  }

  return true;
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