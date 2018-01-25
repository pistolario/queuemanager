# Queue Manager
A simple queue manager, based in threads with SQLite as backend and a concurrent console.

No extra dependencies needed.

## Examples of usage
 * python queuemanager.py -p 9999 -i -c mykey  => Launch the queuemanager, which listens at port 9999 with password "mykey"
 * python queuemanager.py -p 9999 -d ALL -i -c mykey  => Launch the queuemanager, clears all previous messages and listens at port 9999 with password "mykey"

## Console commands
 * EMPTY queue => deletes all messages from a queue or from all queues if "ALL"
 * LIST => Shows all setup queues
 * PUSH => push a new message
 * NEXT queue number => get next message in queue, jumping "number", without consume it, not affected by "pause" state
 * POP queue => get next message in queue, deleting it from the queue
 * MPOP queue queue queue => get next message in any of the queues, deleting it from the queue
 * RECV queue => Consumes first message in queue
 * STATUS queue => Show status of one queue
 * RECV queue => Consumes first message in the queue
 * SEND queue message => inserts a new message in the queue. That message can have special codes
 * ADD name,description => Creates a new queue, with UTF8 codes
 * DEL queue => Deletes a queue
 * PAUSE queue => A queue will not give any new messages
 * PLAY  queue => A queue comes back to normal use
 * UNREAD queue => Undoes the read status of all messages in a queue
 * LAST queue number => shows las read messages in the queue. 0 -> last, 1 -> one before, and so on
 * UNDO queue number => Undoes the read status of "number" messages in a queue
 * LOAD queue initial_row  [column-name]+ from file_name => Reads a CSV file to load as messages in a queue. It can jump several rows, and can assign columns to fields of JSON structure
 * RAWLD queue initial_row file_name => Loads CSV file to a queue
 * DELAY %Y-%m-%dT%H:%M comand => After a while, the system will execute the command provided
 * EXIT => exits
 * HELP => Shows command available
