#pragma once
#include <string>
#include "csv_reader.h"

namespace handlers {
    void handleProcessNextSet(CSVReader* reader);
    void handlePrintBalance(std::string clientName);
    void handlePrintLogs(std::string serverName);
    void handlePrintDBLogs(std::string serverName);
}