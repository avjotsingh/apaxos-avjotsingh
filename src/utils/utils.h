#pragma once
#include <string>
#include <unistd.h>
#include <map>

class Utils {
public:
    static std::vector<std::string> serverNames;
    static std::map<std::string, std::string> serverAddresses;
    static void initializeServers();
    static void startServer(std::string serverName);
    static void killServer(std::string serverName);

private:
    static std::map<std::string, pid_t> serverPIDs;
};