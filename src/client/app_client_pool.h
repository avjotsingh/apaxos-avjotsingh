#pragma once
#include <string>
#include "app_client.h"

class AppClientPool {
public:
    static void initializeClients();
    static AppClient* getClient(std::string serverName);
    static std::vector<AppClient*> getClientsForServer(std::string serverName);
private:
    static std::map<std::string, AppClient*> clients;
};