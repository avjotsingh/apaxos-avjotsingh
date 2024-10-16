#pragma once
#include <string>
#include "client.h"

class ClientPool {
public:
    static void initializeClients();
    static PaxosClient* getClient(std::string serverName);
    static std::vector<PaxosClient*> getClientsForServer(std::string serverName);
private:
    static std::map<std::string, PaxosClient*> clients;
};