#include <map>
#include "app_client_pool.h"
#include "../utils/utils.h"

std::map<std::string, AppClient*> AppClientPool::clients = {};

void AppClientPool::initializeClients() {
    for (auto it = Utils::serverAddresses.begin(); it != Utils::serverAddresses.end(); it++) {
        std::string serverName = it->first;
        std::string targetAddress = it->second;
        AppClientPool::clients[serverName] = new AppClient(serverName, targetAddress);
    }
}

AppClient* AppClientPool::getClient(std::string serverName) {
    auto it = std::find(Utils::serverNames.begin(), Utils::serverNames.end(), serverName);
    if (it != Utils::serverNames.end()) {
        return AppClientPool::clients[serverName];
    }

    throw std::invalid_argument("Invalid server name: " + serverName);
}

std::vector<AppClient*> getClientsForServer(std::string serverName) {
    auto it = std::find(Utils::serverNames.begin(), Utils::serverNames.end(), serverName);
    if (it == Utils::serverNames.end()) {
        throw std::invalid_argument("Invalid server name: " + serverName);
    }

    std::vector<AppClient*> clients;
    for (auto it = Utils::serverAddresses.begin(); it != Utils::serverAddresses.end(); it++) {
        if (serverName != it->first) {
            clients.push_back(new AppClient(serverName, it->second));
        }
    }

    return clients;
}