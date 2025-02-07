#include "constants.h"

namespace Constants {
    std::vector<std::string> serverNames = { "S1", "S2", "S3", "S4", "S5" };
    std::map<std::string, std::string> serverAddresses = {
        { "S1", "localhost:50051" },
        { "S2", "localhost:50052" },
        { "S3", "localhost:50053" },
        { "S4", "localhost:50054" },
        { "S5", "localhost:50055" }
    };
}