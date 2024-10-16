#pragma once
#include <string>
#include <vector>


namespace types {
    struct Transaction {
        std::string id;
        std::string sender;
        std::string receiver;
        int amount;
    };

    struct TransactionSet {
        int setNo;
        std::vector<Transaction> transactions;
        std::vector<std::string> servers;
    };
}
