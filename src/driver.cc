#include <iostream>
#include <sstream>
#include <string>
#include <map>
#include <regex>
#include <vector>

#include "client_pool.h"
#include "utils/commands_parser.h"
#include "utils/csv_reader.h"
#include "utils/handlers.h"
#include "utils/utils.h"

void mainloop(CSVReader* reader) {
    CommandsParser parser;
    while(1) {
        std::string command;
        parser.promptUserForCommand(command);
        
        types::AppCommand c;
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
                    handlers::handleProcessNextSet(reader);
                    break;
                case types::PRINT_BALANCE:
                    handlers::handlePrintBalance(c.serverName);
                    break;
                case types::PRINT_LOG:
                    handlers::handlePrintLogs(c.serverName);
                    break;
                case types::PRINT_DB:
                    handlers::handlePrintDBLogs(c.serverName);
                    break;
                case types::EXIT:
                    std::cout << "Exiting..." << std::endl;
                    return;
                default:
                    std::runtime_error("Unknown command type: " + std::to_string(c.command));
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
        ClientPool::initializeClients();

        CSVReader* reader = new CSVReader(filename);
        mainloop(reader);
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        exit(1);
    }
    
    return 0;
}