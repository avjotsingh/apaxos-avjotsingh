#include <iostream>
#include <algorithm>
#include "handlers.h"
#include "csv_reader.h"
#include "../types/transaction.h"
#include "utils.h"
#include "../client/client_pool.h"

namespace handlers {
    void handleProcessNextSet(CSVReader* reader) {
        std::cout << "Processing next set of transactions...";
        types::TransactionSet set;
        if (!reader->readNextSet(set)) {
            std::cout << "No more transaction sets to read..." << std::endl;
            return;
        }

        for (std::string& s: Utils::serverNames) {
            if (std::find(set.servers.begin(), set.servers.end(), s) != set.servers.end()) {
                // ensure that the desired servers are running
                Utils::startServer(s);
            } else {
                // ensure that the servers not in the transaction set are stopped
                Utils::killServer(s);
            }
        }

        for (types::Transaction& t: set.transactions) {
            PaxosClient* client = ClientPool::getClient(t.sender);
            client->TransferAmount(t.receiver, t.amount);
        } 
    }

    void handlePrintBalance(std::string serverName) {
        PaxosClient* client = ClientPool::getClient(serverName);
        int balance = client->GetBalance();
        std::cout << "Balance on " << serverName << ": " << balance << std::endl;
    }

    void handlePrintLogs(std::string serverName) {
        PaxosClient* client = ClientPool::getClient(serverName);
        std::vector<types::Transaction> logs = client->GetLogs();
        std::cout << "Local logs on " << serverName << ": " << std::endl;
        for (types::Transaction& t: logs) {
            std::cout << "(" << t.sender << ", " << t.receiver << ", " << t.amount << ")" << std::endl;
        }
    }

    void handlePrintDBLogs(std::string serverName) {
        PaxosClient* client = ClientPool::getClient(serverName);
        std::vector<types::Transaction> logs = client->GetDBLogs();
        std::cout << "DB logs on " << serverName << ": " << std::endl;
        for (types::Transaction& t: logs) {
            std::cout << "(" << t.sender << ", " << t.receiver << ", " << t.amount << ")" << std::endl;
        }
    }
}