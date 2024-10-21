#include "async_server.h"
#include "../types/request_types.h"
#include <shared_mutex>
#include "call_data.h"
#include "absl/log/check.h"
#include <chrono>
#include "paxos_client_call.h"
#include <grpc/support/time.h>
#include "../constants.h"
#include <set>
#include <random>
#include <iostream>
#include <fstream>
#include <SQLiteCpp/SQLiteCpp.h>
#include <SQLiteCpp/Database.h>
#include <SQLiteCpp/Statement.h>
#include <SQLiteCpp/Exception.h>

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using google::protobuf::Empty;
using grpc::CompletionQueue;

// Server Implementation
PaxosServer::PaxosServer(std::string serverName) {
  srand(serverName[1] - '0');
  this->serverName = serverName;
  
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
  awaitPrepareDecision = false;
  awaitAcceptDecision = false;
  awaitCommitDecision = false;
  transferSuccess = false;

  currentState_ = IDLE;
  
  dbFilename = "../" + serverName + ".db";
  
  setupDB();
  getSavedStateDB();
  getLocalLogsDB();
  getCommittedLogsDB();
}

void PaxosServer::run(std::string targetAddress) {
  ServerBuilder builder;
  builder.AddListeningPort(targetAddress, grpc::InsecureServerCredentials());
  builder.RegisterService(&service_);
  requestCQ = builder.AddCompletionQueue();
  responseCQ = std::make_unique<CompletionQueue>();
  server_ = builder.BuildAndStart();

  for (auto it = Constants::serverAddresses.begin(); it != Constants::serverAddresses.end(); it++) {
    std::string server = it->first;
    std::string targetAddress = it->second;
    if (server != serverName) {
      stubs_.push_back(Paxos::NewStub(grpc::CreateChannel(targetAddress, grpc::InsecureChannelCredentials())));    
    }
  }

  std::cout << "Running server on " << targetAddress << std::endl;
  HandleRPCs();
}

PaxosServer::~PaxosServer() {
  server_->Shutdown();
  requestCQ->Shutdown();
  responseCQ->Shutdown();
}

void PaxosServer::HandleRPCs() {
  new CallData(&service_, this, requestCQ.get(), types::TRANSFER);
  new CallData(&service_, this, requestCQ.get(), types::GET_BALANCE);
  new CallData(&service_, this, requestCQ.get(), types::GET_LOGS);
  new CallData(&service_, this, requestCQ.get(), types::GET_DB_LOGS);
  new CallData(&service_, this, requestCQ.get(), types::PREPARE);
  new CallData(&service_, this, requestCQ.get(), types::ACCEPT);
  new CallData(&service_, this, requestCQ.get(), types::COMMIT);
  new CallData(&service_, this, requestCQ.get(), types::SUCCESS);

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
      if (requestStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT && requestOk) {
          static_cast<CallData*>(requestTag)->Proceed();  // Process request
      }

      // Handle response events
      if (responseStatus == grpc::CompletionQueue::NextStatus::GOT_EVENT && responseOk) {
          static_cast<PaxosClientCall*>(responseTag)->HandleRPCResponse();  // Process response
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
  PaxosClientCall* call = new PaxosClientCall(this, responseCQ.get(), types::PREPARE);
  call->sendPrepare(request, stub_, deadline);
}

void PaxosServer::sendAcceptAsync(AcceptReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline) {
  PaxosClientCall* call = new PaxosClientCall(this, responseCQ.get(), types::ACCEPT);
  call->sendAccept(request, stub_, deadline);
}

void PaxosServer::sendCommitAsync(CommitReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline) {
  PaxosClientCall* call = new PaxosClientCall(this, responseCQ.get(), types::COMMIT);
  call->sendCommit(request, stub_, deadline);
}

void PaxosServer::sendSuccessAsync(SuccessReq& request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline) {
  PaxosClientCall* call = new PaxosClientCall(this, responseCQ.get(), types::SUCCESS);
  call->sendSuccess(request, stub_, deadline);
}

void PaxosServer::handlePrepareReply(PrepareRes& reply) {
  if (currentState_ == PREPARE || (currentState_ == PROPOSE && !awaitAcceptDecision)) {
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

    // state change logic
    if (prepareSuccesses >= MAJORITY) {
      currentState_ = PROPOSE;
      awaitPrepareDecision = false;
    } else if (prepareFailures >= MAJORITY) {
      currentState_ = IDLE;
      awaitPrepareDecision = false;
      resetConsensusDeadline();
    }
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

  if (acceptSuccesses >= MAJORITY) {
    currentState_ = COMMIT;
    awaitAcceptDecision = false;
  } else if (acceptFailures >= MAJORITY) {
    currentState_ = IDLE;
    awaitAcceptDecision = false;
    resetConsensusDeadline();
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

  if (commitSuccesses >= MAJORITY) {
    currentState_ = POST_COMMIT;
    awaitCommitDecision = false;
  } else if (commitFailures >= MAJORITY) {
    currentState_ = IDLE;
    resetConsensusDeadline();
    awaitCommitDecision = false;
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

void PaxosServer::copyProposal(Proposal* to, types::Proposal from) {
  to->set_number(from.proposalNum);
  to->set_server_id(from.serverName);
}

void PaxosServer::copyLogs(Logs* logs, std::vector<types::Transaction>& val) {
  for (auto l : val) {
        Transaction* t = logs->add_logs();
        t->set_id(l.id);
        t->set_sender(l.sender);
        t->set_receiver(l.receiver);
        t->set_amount(l.amount);
      }
}

std::vector<types::Transaction> PaxosServer::getLogsForProposal() {
  std::vector<types::Transaction> result;  

  // set of committed logs
  std::set<std::string> committedSet;
  for (auto& block : committedBlocks) {
    for (auto& t : block.block) {
      committedSet.insert(t.toString());
    }
  }

  // std::cout << serverName << "committed size set " << committedSet.size() << std::endl;

  if (!highestAcceptVal.empty()) {
    // iterate through highestAcceptVal and check if any entries are already committed
    for (auto& t : highestAcceptVal) {
      if (committedSet.find(t.toString()) == committedSet.end()) {
        result.push_back(t);
      }
    }
  }

  if (result.size() == 0) {
    // std::cout << serverName << " propose local + remote " << std::endl;
    // highestAcceptVal is already committed. ignore it.
    // it is not possible for localLogs to be already committed
    result = localLogs;

    // however some of the remote logs could have been committed already
    for (auto& t: remoteLogs) {
      if (committedSet.find(t.toString()) == committedSet.end()) {
        result.push_back(t);
      }
    }
  }

  return result;
}

void PaxosServer::commitAcceptVal() {
  committedBlocks.push_back({ lastCommittedBlock, lastCommittedProposal, acceptVal });
  commitLogsToDB();


  // update balance and local logs
  std::set<std::string> committedSet;
  for (auto& t: acceptVal) {
    // std::cout << "committing log " << t.id << ", " << t.sender << ", " << t.receiver << ", " << t.amount << std::endl;
    committedSet.insert(t.toString());
    // any transaction whose receiver is this server must add to the balance of this server
    if (t.receiver == serverName) {
      balance += t.amount;
    }
  }

  std::vector<types::Transaction> updatedLocalLogs;
  for (auto& t : localLogs) {
    if (committedSet.find(t.toString()) == committedSet.end()) {
      updatedLocalLogs.push_back(t);
    }
  }


  // reset acceptNum and acceptVal
  acceptNum.proposalNum = 0;
  acceptNum.serverName = "";
  acceptVal.clear();
}

bool PaxosServer::processTransferCall(TransferReq* req, TransferRes* res) {

  bool success;
  res->set_server_id(serverName);
  // std::cout << serverName << " transfer" << std::endl;
  // printf("%s [transfer] cs %d, cpn %d, psuc %d, pf %d, asuc %d, af %d, csuc %d, cf %d\n", serverName.c_str(), currentState_, myProposal.proposalNum, prepareSuccesses, prepareFailures, acceptSuccesses, acceptFailures, commitSuccesses, commitFailures);

  // printf("%s state %d. cpn %d. ps: %d. pf: %d. as: %d. af: %d. cs: %d. cf: %d\n", serverName.c_str(), currentState_,  myProposal.proposalNum, prepareSuccesses, prepareFailures, acceptSuccesses, acceptFailures, commitSuccesses, commitFailures);
  switch (currentState_) {
    case IDLE:
    case PROMISED:
    case ACCEPTED:
      // server can process the client transaction
      if (req->amount() <= balance) {
        currentTransactionNum++;
        Transaction t = { currentTransactionNum, serverName, req->receiver(), req->amount() };
        localLogs.push_back(t);
        balance -= req->amount();

        res->set_ack(true);
        success = true;
      } else {
        // initiate consensus
        if (std::chrono::system_clock::now() >= consensusDeadline) {
          awaitPrepareDecision = false;
          currentState_ = PREPARE;
        }
        // wait longer. return false to indicate that the request is not fully processed yet
        success = false;
      }
      break;

    case PREPARE:
      if (!awaitPrepareDecision || (awaitPrepareDecision && std::chrono::system_clock::now() >= consensusDeadline)) {
        
        // Compose a Prepare request
        myProposal.proposalNum = myProposal.proposalNum + 1;
        myProposal.serverName = serverName;
        
        highestAcceptNum.proposalNum = 0;
        highestAcceptNum.serverName = "";

        prepareSuccesses = 1;
        prepareFailures = 0;
        remoteLogs.clear();

        PrepareReq prepareReq;
        copyProposal(prepareReq.mutable_proposal(), myProposal);
        prepareReq.set_last_committed_block(lastCommittedBlock);
        
        // printf("%s sending prepare with pn %d. ps: %d. pf: %d\n", serverName.c_str(), myProposal.proposalNum, prepareSuccesses, prepareFailures);

        // send a prepare request to cluster
        awaitPrepareDecision = true;
        sendPrepareToCluster(prepareReq);
        resetConsensusDeadline();
      }

      success = false;
      break;

    case PROPOSE:
      if (awaitAcceptDecision && std::chrono::system_clock::now() >= consensusDeadline) {
        currentState_ = PREPARE;
        awaitAcceptDecision = false;
      } else if (!awaitAcceptDecision) {
        AcceptReq acceptReq;
        copyProposal(acceptReq.mutable_proposal(), myProposal);
        acceptReq.set_last_committed_block(lastCommittedBlock);
        
        std::vector<types::Transaction> proposedVal = getLogsForProposal();
        acceptNum.proposalNum = myProposal.proposalNum;
        acceptNum.serverName = myProposal.serverName;
        acceptVal = proposedVal;
        copyLogs(acceptReq.mutable_logs(), proposedVal);

        // for (auto& t: proposedVal) {
        //   std::cout << "proposed value" << t.sender << ", " << t.receiver << ", " << t.amount << std::endl;
        // }

        // for (auto& t: remoteLogs) {
        //   std::cout << "remote logs" << t.sender << ", " << t.receiver << ", " << t.amount << std::endl;
        // }
        
        // prepareSuccesses = 0;
        // prepareFailures = 0;

        acceptSuccesses = 1;
        acceptFailures = 0;

        // printf("%s sending accept with pn %d. ps: %d. pf: %d. as: %d. af: %d\n", serverName.c_str(), myProposal.proposalNum, prepareSuccesses, prepareFailures, acceptSuccesses, acceptFailures);
        awaitAcceptDecision = true;
        sendAcceptToCluster(acceptReq);
        resetConsensusDeadline();
      }
        
      success = false;
      break;

    case COMMIT:
      CommitReq commitReq;
      copyProposal(commitReq.mutable_proposal(), myProposal);
      commitReq.set_last_committed_block(lastCommittedBlock);

      // printf("%s sending commit with pn %d. ps: %d. pf: %d. as: %d. af: %d. cs: %d. cf: %d\n", serverName.c_str(), myProposal.proposalNum, prepareSuccesses, prepareFailures, acceptSuccesses, acceptFailures, commitSuccesses, commitFailures);
      sendCommitToCluster(commitReq);
      postCommit();
      success = true;
      break;
  }

  return success;
}

bool PaxosServer::processGetBalanceCall(Balance* res) {
  res->set_amount(balance);
  return true;
}

bool PaxosServer::processGetLogsCall(Logs* logs) {
  copyLogs(logs, localLogs);
  return true;
}

bool PaxosServer::processGetDBLogsCall(Logs* logs) {
  for (types::TransactionBlock block : committedBlocks) {
    copyLogs(logs, block.block);
  }
  return true;
}

bool PaxosServer::processPrepareCall(PrepareReq* req, PrepareRes* res) {
  // std::cout << serverName << " prepare" << std::endl;
  res->mutable_proposal()->CopyFrom(req->proposal());

  if (currentState_ == IDLE || currentState_ == PREPARE || currentState_ == PROMISED || (currentState_ == PROPOSE && !awaitAcceptDecision)) {
      // just check the proposer's proposal number and last commited block
      if (req->proposal().number() > myProposal.proposalNum && req->last_committed_block() >= lastCommittedBlock) {
            myProposal.proposalNum = req->proposal().number();
            myProposal.serverName = req->proposal().server_id();
            copyLogs(res->mutable_logs(), localLogs);
            res->set_ack(true);
            currentState_ = PROMISED;
            resetConsensusDeadline();
      } else {
        res->set_ack(false);
      }

      return true;
  } else if ((currentState_ == PROPOSE && awaitAcceptDecision) || currentState_ == ACCEPTED) {
      // check if proposal is valid
      if (req->proposal().number() > acceptNum.proposalNum && req->last_committed_block() >= lastCommittedBlock) {
        myProposal.proposalNum = req->proposal().number();
        myProposal.serverName = req->proposal().server_id();
        copyProposal(res->mutable_accept_num(), acceptNum);
        copyLogs(res->mutable_accept_val(), acceptVal);
        res->set_ack(true);
        resetConsensusDeadline();
      } else {
        res->set_ack(false);
      }

      return true;
  } else {
    // handle the prepare later. do the commit first
    return false;
  }
}

bool PaxosServer::processAcceptCall(AcceptReq* req, AcceptRes* res) {  
  res->mutable_proposal()->CopyFrom(req->proposal());
  res->set_server_id(serverName);

  // accept the value conditionally 
  // [IDLE] : server was not issued any transaction, and was not involved in consensus
  // [PREPARE] : server started consensus, but in the meantime another leader got elected who is now sending accept requests
  // [PROPOSE] : server was slow, before it could send out accept requests another leader got elected who is now sending accept requests
  // [PROMISED] : server promised to receive value in response to prepare requests
  // [ACCEPTED] : minority server accepted a value in previous round. A new value is proposed in the second round since this server did not get a prepare request
  // [COMMIT] : A partition prevented the server from committing. In the meantime, a new leader came in and committed the accepted block
  if (currentState_ == IDLE || currentState_ == PREPARE || (currentState_ == PROPOSE && !awaitAcceptDecision)) {
    if (req->proposal().number() >= myProposal.proposalNum) { //&& req->last_committed_block() >= lastCommittedBlock) {
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
        resetConsensusDeadline();
  
      } else {
        // catch up needed
        res->set_ack(false);
        res->set_last_committed_block(lastCommittedBlock);
      }
    } else {
      res->set_ack(false);
    }

    return true;
  } else if ((currentState_ == PROPOSE && awaitAcceptDecision) || currentState_ == ACCEPTED) {
    if (req->proposal().number() >= acceptNum.proposalNum) { //&& req->last_committed_block() >= lastCommittedBlock) {
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
        resetConsensusDeadline();
  
      } else {
        // catch up needed
        res->set_ack(false);
        res->set_last_committed_block(lastCommittedBlock);
      }
    } else {
      res->set_ack(false);
    }

    return true;
  } else {
    // do the commit first. handle the accept later
    return false;
  }
}

bool PaxosServer::processCommitCall(CommitReq* req, CommitRes* res) {
  res->mutable_proposal()->CopyFrom(req->proposal());
  // std::cout << "commit req cs" << currentState_ << std::endl;
  // std::cout << "commit req " << req->proposal().number() << " " << req->last_committed_block() << std::endl;
  // std::cout << "commit req accepted prop " << acceptNum.proposalNum << std::endl;
  

  switch (currentState_) {
    case ACCEPTED:
      // NOTE: we cannot have a case wherein req->proposal().number() == lastestProposal.number but req->proposal().server_id() != lastestProposal.serverName
      // no need to reset consensus deadline
      if (req->last_committed_block() < lastCommittedBlock) {
        // the committer might have experienced network parition.
        // in the meantime, another leader came up and committed the accepted block
        // block has already been committed. ignore the request
        // std::cout << " case 1" << std::endl;
        res->set_ack(true);
      } else if (req->last_committed_block() > lastCommittedBlock) {
        // server is not in sync. cannot accept commit
        // std::cout << " case 2" << std::endl;
        res->set_ack(false);
      } else if (req->proposal().number() > acceptNum.proposalNum) {
        // server does not have the latest accepted value. so it cannot commit
        // std::cout << " case 3" << std::endl;
        res->set_ack(false);
      } else if (req->proposal().number() < acceptNum.proposalNum) {
        // committer is not a leader anymore
        // std::cout << " case 4" << std::endl;
        res->set_ack(false);
      } else {
        // Commit only if the server had accepted a value in the past
        // and the proposal number from accept matches the proposal number from commit
        lastCommittedBlock++;
        lastCommittedProposal.proposalNum = req->proposal().number();
        lastCommittedProposal.serverName = req->proposal().server_id();
        commitAcceptVal();
        
        res->set_ack(true);
        currentState_ = IDLE;      
      }

      break;

    default:
      // std::cout << " case default" << std::endl;
      res->set_ack(false);
      break;

  }
  return true;
}

bool PaxosServer::processSuccessCall(SuccessReq* req, SuccessRes* res) {
  // just make sure you are not adding a stale committed transaction block
  if (currentState_ == POST_COMMIT) {
    postCommit();
  }

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
      // no need to reset consensus deadline because server can start a consensus immediately after
      if (req->proposal().number() > lastCommittedProposal.proposalNum && req->block_id() == lastCommittedBlock + 1) {
        acceptVal.clear();
        for (int i = 0; i < req->block().logs_size(); i++) {
          const Transaction& t = req->block().logs(i);
          acceptVal.push_back({ t.id(), t.sender(), t.receiver(), t.amount() });
        }

        lastCommittedBlock++;
        lastCommittedProposal.proposalNum = req->proposal().number();
        lastCommittedProposal.serverName = req->proposal().server_id();
        commitAcceptVal();
        
        res->set_ack(true);
        currentState_ = IDLE;      

      } else {
        res->set_ack(false);
      }

      res->set_server_id(serverName);
      res->set_last_committed_block(lastCommittedBlock);
      break;
    
    default:
      res->set_ack(false);
  }

  return true;
}

void PaxosServer::doCommit() {
  lastCommittedBlock++;
  lastCommittedProposal.proposalNum = myProposal.proposalNum;
  lastCommittedProposal.serverName = myProposal.serverName;
  commitAcceptVal();
  currentState_ = IDLE;
}

void PaxosServer::setupDB() {
  try {
    SQLite::Database db(dbFilename.c_str(), SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);

      // Compile a SQL query, containing one parameter (index 1)
      db.exec("CREATE TABLE IF NOT EXISTS logs ("
          "block_id INTEGER NOT NULL,"
          "proposal_num INTEGER NOT NULL,"
          "proposer CHAR(2) NOT NULL,"
          "transaction_id INTEGER NOT NULL,"
          "sender CHAR(2) NOT NULL,"
          "receiver CHAR(2) NOT NULL,"
          "amount INTEGER NOT NULL"
          ")");

      db.exec("CREATE TABLE IF NOT EXISTS local ("
          "transaction_id INTEGER NOT NULL,"
          "sender CHAR(2) NOT NULL,"
          "receiver CHAR(2) NOT NULL,"
          "amount INTEGER NOT NULL"
          ")");

      db.exec("CREATE TABLE IF NOT EXISTS accept_val ("
          "transaction_id INTEGER NOT NULL,"
          "sender CHAR(2) NOT NULL,"
          "receiver CHAR(2) NOT NULL,"
          "amount INTEGER NOT NULL"
          ")");

      db.exec("CREATE TABLE IF NOT EXISTS state ("
          "proposal_num INTEGER NOT NULL,"
          "accept_num CHAR(2) NOT NULL,"
          "accept_num_server CHAR(2) NOT NULL,"
          ")");
  } catch (SQLite::Exception& e) {
    std::cerr << "exception " << e.what() << std::endl;
  }
}

void PaxosServer::getLocalLogsDB() {
  try {
    // Open a database file
    SQLite::Database db(dbFilename.c_str(), SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);
    SQLite::Statement query(db, "SELECT * FROM local ORDER BY transaction_id;");
    localLogs.clear();

    while (query.executeStep()) {
      int transaction_id = query.getColumn(3);
      std::string sender = query.getColumn(4).getString();
      std::string receiver = query.getColumn(5).getString();
      int amount = query.getColumn(6);

      if (sender == serverName) {
        balance -= amount;
      } else if (receiver == serverName) {
        balance += amount;
      }

      localLogs.push_back({ transaction_id, sender, receiver, amount });
    }
  } catch (std::exception& e) {
    std::cout << "exception " << e.what() << std::endl;
  }
}

void PaxosServer::storeLocalLogDB(types::Transaction t) {
  try {
    // Open a database file
    SQLite::Database db(dbFilename.c_str(), SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);
    SQLite::Statement insert(db, "INSERT INTO local (transaction_id, sender, receiver, amount) VALUES (?, ?, ?, ?)");

    insert.bind(1, t.id);
    insert.bind(2, t.sender);
    insert.bind(3, t.receiver);
    insert.bind(4, t.amount);

    insert.exec();
  } catch (SQLite::Exception& e) {
    std::cerr << "exception " << e.what() << std::endl;
  }
}

void PaxosServer::refreshLocalLogsDB() {
  try {
    // Open a database file
    SQLite::Database db(dbFilename.c_str(), SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);
    db.exec("BEGIN TRANSACTION");
    
    SQLite::Statement del(db, "DELETE * FROM local");
    del.exec();

    SQLite::Statement insert(db, "INSERT INTO local (transaction_id, sender, receiver, amount) VALUES (?, ?, ?, ?)");
    for (auto &t : localLogs) {
      insert.bind(1, t.id);
      insert.bind(2, t.sender);
      insert.bind(3, t.receiver);
      insert.bind(4, t.amount);
      insert.exec();
      insert.reset();
    }
    db.exec("COMMIT");
    
  } catch (SQLite::Exception& e) {
    std::cerr << "exception " << e.what() << std::endl;
  }
}

void PaxosServer::getSavedStateDB() {
  try {
    SQLite::Statement query(db, "SELECT proposal_num, accept_num, accept_num_server FROM state;");
    query.executeStep();
    myProposal.proposalNum = query.getColumn(0);
    acceptNum.proposalNum = query.getColumn(1);
    acceptNum.serverName = query.getColumn(2);

    query.reset();
    acceptVal.clear();
    query(db, "SELECT transaction_id, sender, receiver, amount FROM accept_val;");
    while (query.executeStep()) {
      int transaction_id = query.getColumn(0);
      std::string sender = query.getColumn(1).toString();
      std::string receiver = query.getColumn(1).toString();
      int amount = query.getColumn(3);

      acceptVal.push_back({ transaction_id, sender, receiver, amount });
    }

  } catch (SQLite::Exception& e) {
    std::cout << "exception " << e.what() << std::endl;
  }
}

void PaxosServer::saveStateDB() {
  try {
    db.exec("BEGIN TRANSACTION;");
    SQLite::Database db(dbFilename, SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);
    SQLite::Statement insert(db, "INSERT INTO state (proposal_num, accept_num, accept_num_server) VALUES (?, ?, ?);");
    insert.bind(1, myProposal.proposalNum);
    insert.bind(2, acceptNum.proposalNum);
    insert.bind(3, acceptNum.serverName);
    insert.exec();
    insert.reset();

    insert(db, "INSERT INTO accept_val (transaction_id, sender, receiver, amount) VALUES (?, ?, ?, ?);");
    for (auto &t : localLogs) {
      insert.bind(1, t.id);
      insert.bind(2, t.sender);
      insert.bind(3, t.receiver);
      insert.bind(4, t.amount);
      insert.exec();
      insert.reset();
    }
    db.exec("COMMIT;");
  } catch (SQLite::Exception& e) {
    std::cerr << "exception " << e.what() << std::endl;
  }
}

void PaxosServer::getCommittedLogsDB() {
  try {
    // Open a database file
    SQLite::Database db(dbFilename.c_str(), SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);
    SQLite::Statement query(db, "SELECT * FROM logs ORDER BY block_id, transaction_id;");
    committedBlocks.clear();

    while (query.executeStep()) {
      int block_id = query.getColumn(0);
      int proposal_num = query.getColumn(1);
      std::string proposer = query.getColumn(2).getString();
      int transaction_id = query.getColumn(3);
      std::string sender = query.getColumn(4).getString();
      std::string receiver = query.getColumn(5).getString();
      int amount = query.getColumn(6);

      if (block_id > committedBlocks.size()) {
        types::Proposal p = { proposal_num, proposer };
        committedBlocks.push_back( { block_id, p, std::vector<types::Transaction>() } );
      }

      if (sender == serverName) {
        balance -= amount;
      } else if (sender == receiver) {
        balance += amount;
      }

      committedBlocks[block_id - 1].block.push_back({ transaction_id, sender, receiver, amount });
    }

    lastCommittedBlock = committedBlocks.size();
    if (lastCommittedBlock > 0) {
      lastCommittedProposal.proposalNum = committedBlocks.at(committedBlocks.size() - 1).proposal.proposalNum;
      lastCommittedProposal.serverName = committedBlocks.at(committedBlocks.size() - 1).proposal.serverName;
    } else {
      lastCommittedProposal.proposalNum = 0
      lastCommittedProposal.serverName = "";
    }

  } catch (std::exception& e) {
    std::cout << "exception " << e.what() << std::endl;
  }
}

void PaxosServer::commitLogsToDB() {
  try    {
    SQLite::Database db(dbFilename, SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);
    
    db.exec("BEGIN TRANSACTION;");
    SQLite::Statement insert(db, "INSERT INTO logs (block_id, proposal_num, proposer, transaction_id, sender, receiver, amount) VALUES (?, ?, ?, ?, ?, ?, ?)");
    
    for (const auto& t : acceptVal) {
      insert.bind(1, lastCommittedBlock);
      insert.bind(2, lastCommittedProposal.proposalNum);
      insert.bind(3, lastCommittedProposal.serverName);
      insert.bind(4, t.id);
      insert.bind(5, t.sender);
      insert.bind(6, t.receiver);
      insert.bind(7, t.amount);
      insert.exec();
      insert.reset(); 
    }  
    db.exec("COMMIT");

  } catch (std::exception& e) {
    std::cout << "exception: " << e.what() << std::endl;
  }
}

void PaxosServer::resetConsensusDeadline() {
  static std::random_device rd;   // Random device for seed.
  static std::mt19937 gen(rd());  // Mersenne Twister generator.
  std::uniform_int_distribution<> dist(PaxosServer::minBackoffMs, PaxosServer::maxBackoffMs);
  int backoff = dist(gen);
  consensusDeadline = std::chrono::system_clock::now() + std::chrono::milliseconds(backoff);
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