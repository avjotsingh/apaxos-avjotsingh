#include <iostream>
#include <sstream>
#include <string>
#include <map>
#include <regex>
#include <vector>

#include "client/app_client.h"
#include "utils/commands_parser.h"
#include "utils/csv_reader.h"
#include "utils/utils.h"
#include "constants.h"

void mainloop(CSVReader* reader, AppClient* client) {
    CommandsParser parser;
    bool exit = false;
    std::string command;
    types::AppCommand c;
    types::TransactionSet set;
    int balance;
    std::vector<types::Transaction> logs;
    std::vector<types::Transaction> dbLogs;


    while(!exit) {
        parser.promptUserForCommand(command);

        try {
            c = parser.parseCommand(command);
        } catch (const std::invalid_argument& e) {
            std::cout << "Exception: " << e.what() << std::endl;
            std::cout << "Try again..." << std::endl;
            continue;
        }

        try {
            switch (c.command) {
                case types::PROCESS_NEXT_SET:
                    std::cout << "Processing next set of transactions...";
                    if (!reader->readNextSet(set)) {
                        std::cout << "No more transaction sets to read..." << std::endl;
                    } else {
                        // make sure the desired (or undesired) servers and up (or down)
                        for (std::string& s: Constants::serverNames) {
                            if (std::find(set.servers.begin(), set.servers.end(), s) != set.servers.end()) {
                                // ensure that the desired servers are running
                                Utils::startServer(s);
                            } else {
                                // ensure that the servers not in the transaction set are stopped
                                Utils::killServer(s);
                            }
                        }
                        client->processTransactions(set.transactions);
                    }
                    break;
                case types::PRINT_BALANCE:
                    balance = client->GetBalance(c.serverName);
                    std::cout << "Balance on " << c.serverName << ": " << balance << std::endl;
                    break;
                case types::PRINT_LOG:
                    logs = client->GetLogs(c.serverName);
                    std::cout << "Local logs on " << c.serverName << ": " << std::endl;
                    for (types::Transaction& t: logs) {
                        std::cout << "(" << t.sender << ", " << t.receiver << ", " << t.amount << ")" << std::endl;
                    }
                    break;
                case types::PRINT_DB:
                    dbLogs = client->GetDBLogs(c.serverName);
                    std::cout << "DB logs on " << c.serverName << ": " << std::endl;
                    for (types::Transaction& t: dbLogs) {
                        std::cout << "(" << t.sender << ", " << t.receiver << ", " << t.amount << ")" << std::endl;
                    }
                    break;
                case types::EXIT:
                    std::cout << "Exiting..." << std::endl;
                    exit = true;
                    break;
                default:
                    std::runtime_error("Unknown command type: " + std::to_string(c.command));
                    break;
            }
        } catch (std::runtime_error& e) {
            std::cerr << "Exception: " << e.what() << std::endl;
            std::cout << "Try again..." << std::endl;
        }
    }
}


int main(int argc, char **argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <csv_filepath>" << std::endl;
        exit(1);
    }

    std::string filename = argv[1];
    try {
        Utils::initializeServers();
        sleep(1);
        AppClient* client = new AppClient(); 
        CSVReader* reader = new CSVReader(filename);
        mainloop(reader, client);
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        exit(1);
    }
    
    return 0;
}