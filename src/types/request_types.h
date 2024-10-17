#pragma once

namespace types {
    enum RequestTypes {
        TRANSFER,
        PREPARE,
        ACCEPT,
        COMMIT,
        GET_BALANCE,
        GET_LOGS,
        GET_DB_LOGS
    };
}
