#include <iostream>
#include <stdexcept>
#include <unistd.h>
#include <sys/wait.h>
#include <csignal>
#include "utils.h"
#include "../constants.h"


std::map<std::string, int> Utils::serverPIDs = {};

void Utils::initializeServers() {
    for (std::string& s: Constants::serverNames) {
        Utils::startServer(s);
    }
}

void Utils::startServer(std::string serverName) {
    auto it = Constants::serverAddresses.find(serverName);
    if (it == Constants::serverAddresses.end()) {
        throw std::invalid_argument("Invalid server name: " + serverName);
    } 
    
    if (Utils::serverPIDs.find(serverName) != Utils::serverPIDs.end()) {
        std::cout << "Server " << serverName << " already running" << std::endl;
        return;
    } else {
        std::string targetAddress = it->second;
        pid_t pid = fork();
        if (pid < 0) {
            throw std::runtime_error("Failed to start server: " + serverName);
        } else if (pid > 0) {
            Utils::serverPIDs.insert({ serverName, pid });
            std::cout << "Started server " + serverName + " on " + targetAddress << "..." << std::endl;
        } else {
            // server process
            execl("./server", "server", serverName.c_str(), targetAddress.c_str(), nullptr);

            // if execl fails
            throw std::runtime_error("Failed to start server: " + serverName);
        }
    }
}

void Utils::killServer(std::string serverName) {
    auto it = Constants::serverAddresses.find(serverName);
    if (it == Constants::serverAddresses.end()) {
        throw std::invalid_argument("Invalid server name: " + serverName);
    }

    auto it2 = Utils::serverPIDs.find(serverName);
    if (it2 == Utils::serverPIDs.end()) {
        std::cout << "Server " << serverName << " not running" << std::endl;
    } else {
        pid_t pid = it2->second;
        if (kill(pid, SIGKILL) == 0) {
            Utils::serverPIDs.erase(serverName);
            std::cout << "Killed server " << serverName << "..." <<  std::endl;
        } else {
            throw std::runtime_error("Failed to kill server: " + serverName);
        }
    }
}