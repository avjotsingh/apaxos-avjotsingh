#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include "paxos.grpc.pb.h"
#include "absl/log/check.h"


#include "../utils/utils.h"
#include "../types/request_types.h"
#include "async_cluster_client.h"

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

    
PaxosClusterClient::PaxosClusterClient(std::string hostServerName) {
  for (auto it = Utils::serverAddresses.begin(); it != Utils::serverAddresses.end(); it++) {
    std::string server = it->first;
    std::string targetAddress = it->second;
    if (server != hostServerName) {
      stubs_.push_back(Paxos::NewStub(
        grpc::CreateChannel(it->first, grpc::InsecureChannelCredentials())
      ));
    }
  }
}
    

  // Assembles the client's payload and sends it to the server.
void PaxosClusterClient::sendPrepareToCluster(PrepareReq request, PrepareRes* reply) {
  std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(rpcTimeoutSeconds);
  for (int i = 0; i < stubs_.size(); i++) {
      sendPrepareAsync(request, stubs_[i], deadline);
  }

  int successCount = 0;
  int majority = stubs_.size() / 2;

  // set proposal number in response
  Proposal* p = reply->mutable_proposal();
  p->set_number(request.proposal().number());
  p->set_server_id(request.proposal().server_id());

  // initialize accept num
  Proposal* a = reply->mutable_accept_num();
  a->set_number(0);
  a->set_server_id("");

  // initialize accept val
  Logs* av = reply->mutable_accept_val();

  // initialize local logs
  Logs* l = reply->mutable_logs();


  // TODO: Make handling of prepare responses async?
  while (std::chrono::system_clock::now() < deadline) {
      void *got_tag;
      bool ok = false;

      if (cq_.Next(&got_tag, &ok)) {
          AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
          CHECK(ok);
          
          if (call->callType == types::PREPARE && call->prepareReply.proposal().server_id() == request.proposal().server_id()) {
              // dequeued RPC result if from one of this server's prepare request
              if (call->prepareReply.proposal().number() > request.proposal().number()) {
                  std::cerr << "[SendPrepareToCluster] Response proposal number " << call->prepareReply.proposal().number() 
                          << " greater than request proposal number " << request.proposal().number();
              } else if (call->prepareReply.proposal().number() == request.proposal().number()) {
                  // RPC result is from the latest prepare request sent by this server
                  if (call->status.ok() && call->prepareReply.ack()) {
                      successCount++;
                      int acceptNum = call->prepareReply.accept_num().number();
                      if (acceptNum > a->number()) {
                          // an acceptor has already accepted a value, and the accept num is highest
                          a->set_number(acceptNum);
                          a->set_server_id(call->prepareReply.accept_num().server_id());
                          av->CopyFrom(call->prepareReply.accept_val());
                          l->clear_logs();
                      } else if (a->number() == 0) {
                          // an acceptor has not accepted a value
                          l->MergeFrom(call->prepareReply.logs());
                      }
                  }
              }

              delete call;
          }
      }
  }

  if (successCount >= majority) {
      reply->set_ack(true);
  } else {
      reply->set_ack(false);
  }
}

void PaxosClusterClient::sendAcceptToCluster(AcceptReq request, AcceptRes* reply) {
  std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(rpcTimeoutSeconds);
  for (int i = 0; i < stubs_.size(); i++) {
      sendAcceptAsync(request, stubs_[i], deadline);
  }

  int successCount = 0;
  int majority = stubs_.size() / 2;
  
  // TODO: Make handling of accept responses async?
  while (std::chrono::system_clock::now() < deadline) {
      void *got_tag;
      bool ok = false;

      if (cq_.Next(&got_tag, &ok)) {
          AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
          CHECK(ok);
          
          if (call->callType == types::ACCEPT && call->acceptReply.proposal().server_id() == request.proposal().server_id()) {
              // dequeued RPC result if from one of this server's accept request
              if (call->acceptReply.proposal().number() > request.proposal().number()) {
                  std::cerr << "[SendAcceptToCluster] Response proposal number " << call->acceptReply.proposal().number() 
                          << " greater than request proposal number " << request.proposal().number();
              } else if (call->acceptReply.proposal().number() == request.proposal().number()) {
                  // RPC result is from the latest accept request sent by this server
                  if (call->status.ok() && call->acceptReply.ack()) {
                      successCount++;
                  }
              }

              delete call;
          }
      }
  }

  if (successCount >= majority) {
      reply->set_ack(true);
  } else {
      reply->set_ack(false);
  }
}

void PaxosClusterClient::sendCommitToCluster(CommitReq request, CommitRes* reply) {
  std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::seconds(rpcTimeoutSeconds);
  for (int i = 0; i < stubs_.size(); i++) {
      sendCommitAsync(request, stubs_[i], deadline);
  }

  int successCount = 0;
  int majority = stubs_.size() / 2;
  
  // TODO: Make handling of commit responses async?
  while (std::chrono::system_clock::now() < deadline) {
      void *got_tag;
      bool ok = false;

      if (cq_.Next(&got_tag, &ok)) {
          AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
          CHECK(ok);
          
          if (call->callType == types::COMMIT && call->commitReply.server_id() == request.server_id()) {
              // dequeued RPC result if from one of this server's commit request
              if (call->commitReply.block_id() > request.block_id()) {
                  std::cerr << "[SendCommitToCluster] Response block ID " << call->commitReply.block_id() 
                          << " greater than request block ID " << request.block_id();
              } else if (call->commitReply.block_id() == request.block_id()) {
                  // RPC result is from the latest accept request sent by this server
                  if (call->status.ok() && call->acceptReply.ack()) {
                      successCount++;
                  }
              }

              delete call;
          }
      }
  }

  if (successCount >= majority) {
      reply->set_ack(true);
  } else {
      reply->set_ack(false);
  }
}

void PaxosClusterClient::sendPrepareAsync(PrepareReq request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline) {
  // Call object to store rpc data
  AsyncClientCall* call = new AsyncClientCall;
  call->callType = types::PREPARE;
  call->context.set_deadline(deadline);

  // stub_->PrepareAsyncSayHello() creates an RPC object, returning
  // an instance to store in "call" but does not actually start the RPC
  // Because we are using the asynchronous API, we need to hold on to
  // the "call" instance in order to get updates on the ongoing RPC.
  call->prepareResponseReader =
      stub_->PrepareAsyncPrepare(&call->context, request, &cq_);

  // StartCall initiates the RPC call
  call->prepareResponseReader->StartCall();

  // Request that, upon completion of the RPC, "reply" be updated with the
  // server's response; "status" with the indication of whether the operation
  // was successful. Tag the request with the memory address of the call
  // object.
  call->prepareResponseReader->Finish(&call->prepareReply, &call->status, (void*)call);
}

void PaxosClusterClient::sendAcceptAsync(AcceptReq request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline) {
  // Call object to store rpc data
  AsyncClientCall* call = new AsyncClientCall;
  call->callType = types::ACCEPT;
  call->context.set_deadline(deadline);

  // stub_->PrepareAsyncSayHello() creates an RPC object, returning
  // an instance to store in "call" but does not actually start the RPC
  // Because we are using the asynchronous API, we need to hold on to
  // the "call" instance in order to get updates on the ongoing RPC.
  call->acceptResponseReader =
      stub_->PrepareAsyncAccept(&call->context, request, &cq_);

  // StartCall initiates the RPC call
  call->acceptResponseReader->StartCall();

  // Request that, upon completion of the RPC, "reply" be updated with the
  // server's response; "status" with the indication of whether the operation
  // was successful. Tag the request with the memory address of the call
  // object.
  call->acceptResponseReader->Finish(&call->acceptReply, &call->status, (void*)call);
}

void PaxosClusterClient::sendCommitAsync(CommitReq request, std::unique_ptr<Paxos::Stub>& stub_, std::chrono::time_point<std::chrono::system_clock> deadline) {
  // Call object to store rpc data
  AsyncClientCall* call = new AsyncClientCall;
  call->callType = types::COMMIT;
  call->context.set_deadline(deadline);

  // stub_->PrepareAsyncSayHello() creates an RPC object, returning
  // an instance to store in "call" but does not actually start the RPC
  // Because we are using the asynchronous API, we need to hold on to
  // the "call" instance in order to get updates on the ongoing RPC.
  call->commitResponseReader =
      stub_->PrepareAsyncCommit(&call->context, request, &cq_);

  // StartCall initiates the RPC call
  call->commitResponseReader->StartCall();

  // Request that, upon completion of the RPC, "reply" be updated with the
  // server's response; "status" with the indication of whether the operation
  // was successful. Tag the request with the memory address of the call
  // object.
  call->commitResponseReader->Finish(&call->commitReply, &call->status, (void*)call);
}


// int main(int argc, char** argv) {
//   absl::ParseCommandLine(argc, argv);
//   // Instantiate the client. It requires a channel, out of which the actual RPCs
//   // are created. This channel models a connection to an endpoint specified by
//   // the argument "--target=" which is the only expected argument.
//   std::string target_str = absl::GetFlag(FLAGS_target);
//   // We indicate that the channel isn't authenticated (use of
//   // InsecureChannelCredentials()).
//   GreeterClient greeter(
//       grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

//   // Spawn reader thread that loops indefinitely
//   std::thread thread_ = std::thread(&GreeterClient::AsyncCompleteRpc, &greeter);

//   for (int i = 0; i < 100; i++) {
//     std::string user("world " + std::to_string(i));
//     greeter.SayHello(user);  // The actual RPC call!
//   }

//   std::cout << "Press control-c to quit" << std::endl << std::endl;
//   thread_.join();  // blocks forever

//   return 0;
// }