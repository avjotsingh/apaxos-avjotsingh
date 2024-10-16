#include <map>
#include "client_pool.h"
#include "../utils/utils.h"

std::map<std::string, PaxosClient*> ClientPool::clients = {};

void ClientPool::initializeClients() {
    for (auto it = Utils::serverAddresses.begin(); it != Utils::serverAddresses.end(); it++) {
        std::string serverName = it->first;
        std::string targetAddress = it->second;
        ClientPool::clients[serverName] = new PaxosClient(serverName, targetAddress);
    }
}

PaxosClient* ClientPool::getClient(std::string serverName) {
    auto it = std::find(Utils::serverNames.begin(), Utils::serverNames.end(), serverName);
    if (it != Utils::serverNames.end()) {
        return ClientPool::clients[serverName];
    }

    throw std::invalid_argument("Invalid server name: " + serverName);
}

std::vector<PaxosClient*> getClientsForServer(std::string serverName) {
    auto it = std::find(Utils::serverNames.begin(), Utils::serverNames.end(), serverName);
    if (it == Utils::serverNames.end()) {
        throw std::invalid_argument("Invalid server name: " + serverName);
    }

    std::vector<PaxosClient*> clients;
    for (auto it = Utils::serverAddresses.begin(); it != Utils::serverAddresses.end(); it++) {
        if (serverName != it->first) {
            clients.push_back(new PaxosClient(serverName, it->second));
        }
    }

    return clients;
}