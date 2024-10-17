#pragma once
#include <string>
#include <vector>


namespace types {

    struct Proposal {
        int proposalNum;
        std::string serverName;
    };

    struct Transaction {
        int id;
        std::string sender;
        std::string receiver;
        int amount;

        bool operator<(const Transaction& other) const {
            if (sender < other.sender) return true;
            else return id < other.id;
        }

        bool operator==(const Transaction& other) const {
            return id == other.id 
                && sender == other.sender 
                && receiver == other.receiver 
                && amount == other.amount;
        }
    };

    struct TransactionSet {
        int setNo;
        std::vector<Transaction> transactions;
        std::vector<std::string> servers;
    };

    struct TransactionBlock {
        int id;
        std::vector<Transaction> block;
    };
}
