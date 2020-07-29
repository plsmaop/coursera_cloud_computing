/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log,
                 Address *address) {
    this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    ht = new HashTable();
    this->memberNode->addr = *address;
    selfNode = Node(*address);
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
    delete ht;
    delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the
 * Membership Protocol (MP1Node) The membership list is returned as a vector of
 * Nodes. See Node class in Node.h 2) Constructs the ring based on the
 * membership list 3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
    /*
     * Implement this. Parts of it are already implemented
     */
    vector<Node> curMemList;
    bool change = false;

    /*
     *  Step 1. Get the current membership list from Membership Protocol / MP1
     */
    curMemList = getMembershipList();

    /*
     * Step 2: Construct the ring
     */
    // Sort the list based on the hashCode
    sort(curMemList.begin(), curMemList.end());

    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    // Run stabilization protocol if the hash table size is greater than zero
    // and if there has been a changed in the ring
    ring = std::move(curMemList);
    ringToTable();

    if (ring.size() != 0) {
        stabilizationProtocol();
    }

    // set neighbors
    auto iter = find_if(ring.begin(), ring.end(), [this](Node &n) -> bool {
        return n.nodeAddress == this->memberNode->addr;
    });
    auto index = distance(ring.begin(), iter);
    hasMyReplicas = vector<Node>{ring[(index + 1) % ring.size()],
                                 ring[(index + 2) % ring.size()]};
    haveReplicasOf =
        vector<Node>{ring[(index + ring.size() - 1) % ring.size()],
                     ring[(index + ring.size() - 2) % ring.size()]};
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the
 * Membership protocol/MP1 and i) generates the hash code for each member ii)
 * populates the ring member in MP2Node class It returns a vector of Nodes. Each
 * element in the vector contain the following fields: a) Address of the node b)
 * Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
    unsigned int i;
    vector<Node> curMemList;
    for (i = 0; i < this->memberNode->memberList.size(); i++) {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
        memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        curMemList.emplace_back(Node(addressOfThisMember));
    }
    return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the
 * ring HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
    std::hash<string> hashFunc;
    size_t ret = hashFunc(key);
    return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: sendWithReplicaType
 *
 * DESCRIPTION: helper function
 * 				The function does the following:
 * 				1) Insert ReplicaType into a Message
 * 				2) Send the Message the Node
 */
void MP2Node::sendWithReplicaType(Address &&addr, Message &&m, ReplicaType r) {
    m.replica = r;
    auto msgString = m.toString();
    emulNet->ENsend(&memberNode->addr, &addr, msgString);
};

/**
 * FUNCTION NAME: sendMsg
 *
 * DESCRIPTION: helper function
 * 				The function does the following:
 * 				1) Get nodes of the given key
 * 				2) Send the Message to the nodes
 */
void MP2Node::sendMsg(const string &&key, const string &&value,
                      MessageType type) {
    auto replicas = findNodes(key);

    Message m(++g_transID, memberNode->addr, type, key, value);
    transactionTable.insert({g_transID, make_pair(m, vector<Message>{})});

    for (int i = 0; i < replicas.size(); ++i) {
        sendWithReplicaType(forward<Address>(replicas[i].nodeAddress),
                            forward<Message>(m),
                            static_cast<ReplicaType>(PRIMARY + i));
    }
};

/**
 * FUNCTION NAME: replyMsg
 *
 * DESCRIPTION: helper function
 * 				The function does the following:
 * 				1) Construct reply message
 * 				2) Reply message
 */
void MP2Node::replyMsg(Message &&oldMsg, bool isSuc) {
    Message m(oldMsg);
    m.fromAddr = memberNode->addr;
    m.type = REPLY;
    m.success = isSuc;
    sendWithReplicaType(forward<Address>(oldMsg.fromAddr), forward<Message>(m),
                        oldMsg.replica);
};

/**
 * FUNCTION NAME: replyMsg
 *
 * DESCRIPTION: helper function
 * 				The function does the following:
 * 				1) Construct reply message
 * 				2) Reply message
 */
void MP2Node::replyMsg(Message &&oldMsg, string &&value) {
    Message m(oldMsg);
    m.fromAddr = memberNode->addr;
    m.type = READREPLY;
    m.value = value;
    sendWithReplicaType(forward<Address>(oldMsg.fromAddr), forward<Message>(m),
                        oldMsg.replica);
};

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    /*
     * Implement this
     */

    sendMsg(forward<string>(key), forward<string>(value), CREATE);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key) {
    /*
     * Implement this
     */

    sendMsg(forward<string>(key), "", READ);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value) {
    /*
     * Implement this
     */

    sendMsg(forward<string>(key), forward<string>(value), UPDATE);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key) {
    /*
     * Implement this
     */
    sendMsg(forward<string>(key), "", DELETE);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or
 * failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
    /*
     * Implement this
     */
    // Insert key, value, replicaType into the hash table
    Entry e(value, par->getcurrtime(), replica);
    return ht->create(key, e.convertToString());
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
    /*
     * Implement this
     */
    // Read key from local hash table and return value
    return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local
 * hash table 2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
    /*
     * Implement this
     */
    // Update key in local hash table and return true or false
    Entry e(value, par->getcurrtime(), replica);
    return ht->update(key, e.convertToString());
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or
 * failure
 */
bool MP2Node::deletekey(string key) {
    /*
     * Implement this
     */
    // Delete the key from the local hash table
    return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: logSuccess
 *
 * DESCRIPTION: log helper function
 * 				This function does the following:
 * 				1) log success msg according to msg type
 * failure
 */
void MP2Node::logSuccess(Message &&msg) {
    switch (msg.type) {
        case CREATE:
            log->logCreateSuccess(&memberNode->addr, true, msg.transID, msg.key,
                                  msg.value);
            break;
        case UPDATE:
            log->logUpdateSuccess(&memberNode->addr, true, msg.transID, msg.key,
                                  msg.value);
            break;
        case DELETE:
            log->logDeleteSuccess(&memberNode->addr, true, msg.transID,
                                  msg.key);
            break;
        default:
            break;
    }
}

/**
 * FUNCTION NAME: logFail
 *
 * DESCRIPTION: log helper function
 * 				This function does the following:
 * 				1) log fail msg according to msg type
 * failure
 */
void MP2Node::logFail(Message &&msg) {
    switch (msg.type) {
        case CREATE:
            log->logCreateFail(&memberNode->addr, true, msg.transID, msg.key,
                               msg.value);
            break;
        case UPDATE:
            log->logUpdateFail(&memberNode->addr, true, msg.transID, msg.key,
                               msg.value);
            break;
        case DELETE:
            log->logDeleteFail(&memberNode->addr, true, msg.transID, msg.key);
            break;
        default:
            break;
    }
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message
 * types
 */
void MP2Node::checkMessages() {
    /*
     * Implement this. Parts of it are already implemented
     */
    char *data;
    int size;

    /*
     * Declare your local variables here
     */

    // dequeue all messages and handle them
    while (!memberNode->mp2q.empty()) {
        /*
         * Pop a message from the queue
         */
        data = (char *)memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();

        string message(data, data + size);

        /*
         * Handle the message types here
         */

        Message m(message);
        switch (m.type) {
            case CREATE: {
                auto isSuc = createKeyValue(m.key, m.value, m.replica);
                if (m.transID == -1) break;
                if (isSuc) {
                    log->logCreateSuccess(&memberNode->addr, false, m.transID,
                                          m.key, m.value);
                } else {
                    log->logCreateFail(&memberNode->addr, false, m.transID,
                                       m.key, m.value);
                }
                replyMsg(forward<Message>(m), isSuc);

                break;
            }
            case READ: {
                auto value = readKey(m.key);
                if (m.transID == -1) break;
                if (value.length() > 0) {
                    Entry e(value);
                    value = e.value;
                    log->logReadSuccess(&memberNode->addr, false, m.transID,
                                        m.key, value);
                } else {
                    log->logReadFail(&memberNode->addr, false, m.transID,
                                     m.key);
                }

                replyMsg(forward<Message>(m), forward<string>(value));
                break;
            }
            case UPDATE: {
                auto isSuc = updateKeyValue(m.key, m.value, m.replica);
                if (m.transID == -1) break;
                if (isSuc) {
                    log->logUpdateSuccess(&memberNode->addr, false, m.transID,
                                          m.key, m.value);
                } else {
                    log->logUpdateFail(&memberNode->addr, false, m.transID,
                                       m.key, m.value);
                }
                replyMsg(forward<Message>(m), isSuc);
                break;
            }
            case DELETE: {
                auto isSuc = deletekey(m.key);
                if (m.transID == -1) break;
                if (isSuc) {
                    log->logDeleteSuccess(&memberNode->addr, false, m.transID,
                                          m.key);
                } else {
                    log->logDeleteFail(&memberNode->addr, false, m.transID,
                                       m.key);
                }
                replyMsg(forward<Message>(m), isSuc);
                break;
            }
            case REPLY: {
                auto iter = transactionTable.find(m.transID);

                // handled, drop msg
                if (iter == transactionTable.end()) break;
                iter->second.second.push_back(m);

                if (iter->second.second.size() >= 2) {
                    // handle quorum
                    int quorum = 0;
                    for (const auto &msg : iter->second.second) {
                        if (msg.success) ++quorum;
                    }

                    if (quorum >= 2) {
                        // success
                        transactionTable.erase(m.transID);
                        logSuccess(forward<Message>(iter->second.first));
                    } else if (iter->second.second.size() == 3) {
                        // fail
                        transactionTable.erase(m.transID);
                        logFail(forward<Message>(iter->second.first));
                    }
                }
                break;
            }
            case READREPLY: {
                auto iter = transactionTable.find(m.transID);

                // handled, drop msg
                if (iter == transactionTable.end()) break;
                iter->second.second.push_back(m);

                if (iter->second.second.size() >= 2) {
                    // handle quorum
                    int quorum = 0;
                    for (const auto &msg : iter->second.second) {
                        if (msg.value.length() > 0) ++quorum;
                    }

                    if (quorum >= 2) {
                        // success
                        transactionTable.erase(m.transID);
                        log->logReadSuccess(&memberNode->addr, true, m.transID,
                                            iter->second.first.key, m.value);
                    } else if (iter->second.second.size() == 3) {
                        // fail
                        transactionTable.erase(m.transID);
                        log->logReadFail(&memberNode->addr, true, m.transID,
                                         iter->second.first.key);
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    /*
     * This function should also ensure all READ and UPDATE operation
     * get QUORUM replies
     */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the
 * replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() ||
            pos > ring.at(ring.size() - 1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        } else {
            // go through the ring until pos <= node
            for (int i = 1; i < ring.size(); i++) {
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
                    addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL,
                               1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: ringToTable
 *
 * DESCRIPTION: This function turns a ring into node table
 */
void MP2Node::ringToTable() {
    /*
     * Implement this
     */
    nodeTable.clear();
    for (int i = 0; i < ring.size(); ++i) {
        nodeTable[ring[i].getHashCode()] = i;
    }
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and
 *leaves It ensures that there always 3 copies of all keys in the DHT at all
 *times The function does the following: 1) Ensures that there are three
 *"CORRECT" replicas of all the keys in spite of failures and joins Note:-
 *"CORRECT" replicas implies that every key is replicated in its two neighboring
 *nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
    /*
     * Implement this
     */
    for (const auto &pair : ht->hashTable) {
        Entry e(pair.second);
        vector<Node> nodes = findNodes(pair.first);
        if (e.replica == PRIMARY) {
            // primary, handle stabalization
            auto oldReplicaNodes = vector<Node>{selfNode};
            oldReplicaNodes.insert(oldReplicaNodes.end(), hasMyReplicas.begin(),
                                   hasMyReplicas.end());
            for (int i = 0; i < nodes.size(); ++i) {
                if (nodes[i].getHashCode() !=
                    oldReplicaNodes[i].getHashCode()) {
                    Message replicaMsg(-1, memberNode->addr, CREATE, pair.first,
                                       e.value);

                    sendWithReplicaType(
                        forward<Address>(*nodes[i].getAddress()),
                        forward<Message>(replicaMsg),
                        static_cast<ReplicaType>(PRIMARY + i + 1));

                    Message delMsg(-1, memberNode->addr, DELETE, pair.first,
                                   e.value);

                    sendWithReplicaType(
                        forward<Address>(*hasMyReplicas[i].getAddress()),
                        forward<Message>(delMsg),
                        static_cast<ReplicaType>(PRIMARY + i + 1));
                }
            }
        }
    }
}
