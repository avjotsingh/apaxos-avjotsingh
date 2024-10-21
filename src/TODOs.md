- persist the proposal number on disk 
- send prepares all at the same time
- synchronize the slow server (asynchronously)
- passive catch-up mechanism (if received last commited id > server's last committed id)
- do not send a reply message to client if transaction fails
- add timeout after receiving majority of promise messages
- synchronize acceptor if accept val is already committed (reset acceptor's acceptnum and acceptval)
- randomize delays after prepare phase failure
- verify state transitions
- verify when to ignore prepare, accept, commit messages from previous phases
- when leader receives local logs, check if any of those are already committed
- check if accept num and accept val have already been committed and ignore those values if committed
- modify cluster client to send requent to host server also
- increment current proposal number in case of majority failures
- sanitize all logs i.e. remove duplicates 
- a server starting consensus cannot respond to RPCs like other servers** (change the cluster broadcast logic)
- catchup decision should only be made on the basis of committed log length




States -> IDLE, PREPARE, PROPOSE, PROMISED, ACCEPTED, COMMIT, CATCH_UP

Transitions

On transfer message
- IDLE->PREPARE

On prepare message
- IDLE->PROMISED
- IDLE->CATCHUP
- PREPARE->PROMISED
- PREPARE->CATCHUP

On accept message
- s

