#include <string>
#include <vector>

namespace types {
    enum Commands {
        PROCESS_NEXT_SET,
        PRINT_BALANCE,
        PRINT_LOG,
        PRINT_DB,
        EXIT
    };

    struct AppCommand {
        Commands command;
        std::string serverName;
    };
}
