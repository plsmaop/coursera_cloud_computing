/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include <unordered_map>
#include <random>

#include "MP1Node.h"


/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log,
                 Address *address) {
    memset(NULLADDR, 0, ADDR_LEN * sizeof(char));
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into
 * the queue This function is called by a node to receive messages currently
 * waiting for it
 */
int MP1Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1,
                               &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if (!introduceSelfToGroup(&joinaddr)) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    auto id = getIdFromAddr(memberNode->addr.addr);
    auto port = getPortFromAddr(memberNode->addr.addr);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if (0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr),
                    sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the
        // group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    } else {
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // create JOINREQ message: format of data is {struct Address myaddr}
        // send JOINREQ message to introducer member
        sendMsg(joinaddr, JOINREQ);
    }

    return 1;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
    /*
     * Your code goes here
     */
    emulNet = NULL;
    log = NULL;
    par = NULL;

    delete memberNode;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform
 * membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if (!memberNode->inGroup) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message
 * handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while (!memberNode->mp1q.empty()) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
    /*
     * Your code goes here
     */
    auto *msg = (MessageHdr *)data;
    auto *m = (Member *)env;

    switch (msg->msgType) {
        case JOINREQ:
            handleRecvJoinReq(m, msg, size);
            break;

        case JOINREP:
            handleRecvJoinRep(m, msg, size);
            break;

        case GOSSIP:
            handleRecvGossipMsg(m, msg, size);
            break;

        default:
            return false;
    }

    // delete msg;
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and
 * then delete the nodes Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    /*
     * Your code goes here
     */

    // Update heartbeat
    memberNode->heartbeat++;

    // remove member if necessary
    auto curTime = par->getcurrtime();
    auto myId = getIdFromAddr(memberNode->addr.addr);

    auto prevMemberList = memberNode->memberList;
    memberNode->memberList.clear();
    for (auto &m : prevMemberList) {
        if (myId == m.getid()) {
            m.setheartbeat(memberNode->heartbeat);
            m.settimestamp(curTime);
            memberNode->memberList.push_back(m);
        } else if (curTime - m.getheartbeat() > 2 * TREMOVE) {
            auto id = m.getid();
            auto port = m.getport();
            auto addr = Address();
            memset(&addr, 0, sizeof(Address));
            loadAddr(&addr, id, port);
            log->logNodeRemove(&memberNode->addr, &addr);
        } else {
            memberNode->memberList.push_back(m);
        }
    }

    // Gossiping
    unordered_map<string, bool> exclude(
        {{getIdAndPortString(getIdFromAddr(memberNode->addr.addr),
                             getPortFromAddr(memberNode->addr.addr)),
          true}});
    gossip(exclude, par->getcurrtime());

    return;
}

/**
 * FUNCTION NAME: updateMemberList
 *
 * DESCRIPTION: update member or insert new member if not exist
 */
void MP1Node::updateMemberList(Member *m, char *addr, int heartbeat,
                               int timestamp) {
    auto id = getIdFromAddr(addr);
    auto port = getPortFromAddr(addr);
    auto curTime = par->getcurrtime();

    for (auto &me : m->memberList) {
        if (me.getid() == id && me.getport() == port) {
            if (heartbeat <= me.heartbeat) return;

            // update member
            me.setheartbeat(heartbeat);
            me.settimestamp(curTime);

            return;
        }
    }

    // insert new member
    if (m->heartbeat - TREMOVE < heartbeat) {
        MemberListEntry me(id, port, heartbeat, curTime);
        m->memberList.push_back(me);

        // construct an Address instance
        if (getIdFromAddr(m->addr.addr) != id) {
            auto a = Address();
            memset(&a, 0, sizeof(Address));
            memcpy(a.addr, addr, ADDR_LEN);
            log->logNodeAdd(&m->addr, &a);
        }
    }

    return;
}

/**
 * FUNCTION NAME: handleRecvJoinRep
 *
 * DESCRIPTION: handle function for Receiving Join Response
 */
void MP1Node::handleRecvJoinRep(Member *m, MessageHdr *msg, int msgSize) {
    // node has joined the group
    m->inGroup = true;
    return updateMemberList(m, msg->addr, m->heartbeat, par->getcurrtime());
}

/**
 * FUNCTION NAME: handleRecvJoinReq
 *
 * DESCRIPTION: handle function for Receiving Join Request
 */
void MP1Node::handleRecvJoinReq(Member *m, MessageHdr *msg, int msgSize) {
    auto a = Address();
    memset(&a, 0, sizeof(Address));
    memcpy(a.addr, msg->addr, ADDR_LEN);
    sendMsg(&a, JOINREP);
    return updateMemberList(m, msg->addr, m->heartbeat, par->getcurrtime());
}

/**
 * FUNCTION NAME: handleRecvGossipMsg
 *
 * DESCRIPTION: handle function for Receiving Gossiping Message
 */
void MP1Node::handleRecvGossipMsg(Member *m, MessageHdr *msg, int msgSize) {

    vector<MemberListEntry> mle;
    vector<MemberListEntry> sent;
    char *data = (char *)msg + sizeof(MessageHdr);
    unmarshall(data, msg->dataSize, msg->sentSize, mle, sent);

    for (auto &e : mle) {
        auto id = e.getid();
        auto port = e.getport();

        Address addr = Address();
        memset(&addr, 0, sizeof(Address));
        loadAddr(&addr, e.getid(), e.getport());
        updateMemberList(m, addr.addr, e.heartbeat, e.timestamp);
    }

    // update
    updateMemberList(m, msg->addr, m->heartbeat, msg->timestamp);

    // Gossiping
    // self and sender
    unordered_map<string, bool> exclude(
        {{getIdAndPortString(getIdFromAddr(memberNode->addr.addr),
                             getPortFromAddr(memberNode->addr.addr)),
          true},
         {getIdAndPortString(getIdFromAddr(msg->addr),
                             getPortFromAddr(msg->addr)),
          true}});

    for (auto s : sent) {
        exclude[getIdAndPortString(s.getid(), s.getport())] = true;
    }
    
    gossip(exclude, msg->timestamp);

    return;
}

/**
 * FUNCTION NAME: gossip
 *
 * DESCRIPTION: gossip protocol
 */
void MP1Node::gossip(unordered_map<string, bool> &exclude, long timestamp) {
    auto randomOrder(memberNode->memberList);

    // shuffle
    std::random_device rd;
    auto rng = std::default_random_engine(rd());
    shuffle(randomOrder.begin(), randomOrder.end(), rng);

    string idAndPort = "";
    MemberListEntry me;
    vector<MemberListEntry> to_be_sent;
    for (decltype(randomOrder.size()) i = 0;
         i < randomOrder.size() && to_be_sent.size() < GOSSIP_NUM; ++i) {
        me = randomOrder[i];
        idAndPort = getIdAndPortString(me.getid(), me.getport());
        if (exclude[idAndPort]) continue;

        to_be_sent.push_back(me);
    }

    vector<MemberListEntry> sent = to_be_sent;
    for (auto e : exclude) {
        string idAndPort = e.first;
        int id;
        short port;
        loadIdAndPortFromString(idAndPort, id, port);
        sent.push_back(MemberListEntry(id, port));
    }

    for (auto me : to_be_sent) {
        auto addr = Address();
        memset(&addr, 0, sizeof(Address));
        loadAddr(&addr, me.getid(), me.getport());
        sendMsg(&addr, GOSSIP, sent, timestamp);
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, ADDR_LEN) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) const {
    printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
           addr->addr[3], *(short *)&addr->addr[4]);
}

/**
 * FUNCTION NAME: getIdFromAddr
 *
 * DESCRIPTION: get id
 */
int MP1Node::getIdFromAddr(char *addr) { return addr ? *((int *)addr) : 0; }

/**
 * FUNCTION NAME: getPortFromAddr
 *
 * DESCRIPTION: get port
 */
short MP1Node::getPortFromAddr(char *addr) {
    return addr ? *((short *)&(addr[4])) : 0;
}

/**
 * FUNCTION NAME: loadAddr
 *
 * DESCRIPTION: load Address
 */
void MP1Node::loadAddr(Address *addr, int id, short port) {
    memcpy(addr->addr, &id, sizeof(int));
    memcpy(&addr->addr[4], &port, sizeof(short));
}

/**
 * FUNCTION NAME: sendMsg
 *
 * DESCRIPTION: send message to the address
 */
void MP1Node::sendMsg(Address *addr, MsgTypes ms) {
    MessageHdr msg = MessageHdr();

    memset(&msg, 0, sizeof(MessageHdr));
    memcpy(msg.addr, memberNode->addr.addr, ADDR_LEN);
    msg.msgType = ms;
    msg.timestamp = par->getcurrtime();

    emulNet->ENsend(&memberNode->addr, addr, (char *)(&msg), sizeof(MessageHdr));
}

/**
 * FUNCTION NAME: sendMsg
 *
 * DESCRIPTION: send message to the address for gossip
 */
void MP1Node::sendMsg(Address *addr, MsgTypes ms, vector<MemberListEntry> &sent, long timestamp) {
    // gossip msg includes member list and sent list
    // MessageHdr + addrs + sent
    size_t dataSize =
        DATA_FRAME_SIZE * memberNode->memberList.size();

    size_t sentSize = DATA_FRAME_SIZE * sent.size();

    size_t msgSize = sizeof(MessageHdr) + dataSize + sentSize + 1;
    MessageHdr *msg = static_cast<MessageHdr *>(malloc(msgSize));
    memset(msg, 0, msgSize);

    marshall((char *)msg + sizeof(MessageHdr), memberNode->memberList, sent);
    msg->dataSize = dataSize;
    msg->sentSize = sentSize;

    memcpy(msg->addr, memberNode->addr.addr, ADDR_LEN);
    msg->msgType = ms;
    msg->timestamp = timestamp;

    emulNet->ENsend(&memberNode->addr, addr, (char *)msg, msgSize);
    delete msg;
}

/**
 * FUNCTION NAME: getIdAndPortString
 *
 * DESCRIPTION: convert id and port to string and concat them
 */
string MP1Node::getIdAndPortString(int id, short port) {
    return to_string(id) + ":" + to_string(port);
}

/**
 * FUNCTION NAME: loadIdAndPortFromString
 *
 * DESCRIPTION: load id and port from idAndPort string
 */
void MP1Node::loadIdAndPortFromString(string idAndPort, int &id, short &port) {

    auto id_pos = idAndPort.find(":");
    auto id_string = idAndPort.substr(0, id_pos);
    auto port_string = idAndPort.substr(id_pos+1);

    id = stoi(id_string);
    port = stoi(port_string);

    return;
}

/**
 * FUNCTION NAME: marshall
 *
 * DESCRIPTION: convert adresses into byte array
 */
void MP1Node::marshall(char *_dest, vector<MemberListEntry> &m, vector<MemberListEntry> &sent) {
    auto ind = 0;
    Address a = Address();
    memset(&a, 0, sizeof(Address));

    for (auto me : m) {
        loadAddr(&a, me.getid(), me.getport());

        // addr
        memcpy(_dest + ind, a.addr, ADDR_LEN);
        ind += ADDR_LEN;

        // heartbeat
        long hb = me.getheartbeat();
        memcpy(_dest + ind, &hb, sizeof(long));
        ind += sizeof(long);
        _dest[ind++] = ',';
    }

    for (auto s : sent) {
        loadAddr(&a, s.getid(), s.getport());

        // addr
        memcpy(_dest + ind, a.addr, ADDR_LEN);
        ind += ADDR_LEN;

        // heartbeat
        memcpy(_dest + ind, &s.heartbeat, sizeof(long));
        ind += sizeof(long);
        _dest[ind++] = ',';
    }

    return;
}

/**
 * FUNCTION NAME: unmarshall
 *
 * DESCRIPTION: convert byte array into addresses
 */
void MP1Node::unmarshall(char *_src, size_t dataSize, size_t sentSize,
                         vector<MemberListEntry> &m, vector<MemberListEntry> &sent) {
    size_t mleSize = dataSize / DATA_FRAME_SIZE;
    long heartbeat;

    char addr[ADDR_LEN];
    for (decltype(mleSize) i = 0; i < mleSize; ++i) {
        // dataFrameSize = ADDR_LEN + sizeof(long) + 1

        memcpy(addr, _src, ADDR_LEN);
        _src += ADDR_LEN;
        memcpy(&heartbeat, _src, sizeof(long));
        _src += sizeof(long);

        m.push_back(MemberListEntry(getIdFromAddr(addr), getPortFromAddr(addr),
                                    heartbeat, par->getcurrtime()));

        // for `,`
        ++_src;
    }

    size_t sentNum = sentSize / DATA_FRAME_SIZE;
    for (decltype(sentNum) i = 0; i < sentNum; ++i) {
        // dataFrameSize = ADDR_LEN + sizeof(long) + 1

        memcpy(addr, _src, ADDR_LEN);
        _src += ADDR_LEN;
        memcpy(&heartbeat, _src, sizeof(long));
        _src += sizeof(long);

        sent.push_back(MemberListEntry(getIdFromAddr(addr), getPortFromAddr(addr),
                                    heartbeat, par->getcurrtime()));

        // for `,`
        ++_src;
    }

    return;
}
