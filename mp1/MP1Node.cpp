/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    memset(NULLADDR, 0, NULLADDR_LEN * sizeof(char));
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
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
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
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
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
	int id = getIdFromAddr(&memberNode->addr);
	short port = getPortFromAddr(&memberNode->addr);

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

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
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
int MP1Node::finishUpThisNode(){
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
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
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
bool MP1Node::recvCallBack( void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr *msg = (MessageHdr*)data;
    Member *m = (Member*)env;

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
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    // Update heartbeat
    memberNode->heartbeat++;

    // remove member if necessary
    int curTime = par->getcurrtime();

    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
    int myId = getIdFromAddr(&memberNode->addr);
    while (it != memberNode->memberList.end()) {
        if (myId == it->getid()) {
            it->setheartbeat(memberNode->heartbeat);
            it->settimestamp(curTime);
            it++;
            continue;
        }
        if (curTime - it->heartbeat > TREMOVE) {
            // remove
            it = memberNode->memberList.erase(it);
            Address addr = Address();
            memset(&addr, 0, sizeof(Address));
            loadAddr(&addr, it->getid(), it->getport());
            log->logNodeRemove(&memberNode->addr, &addr);
        } else it++;
    }

    // Gossiping
    gossip();

    return;
}

/**
 * FUNCTION NAME: updateMemberList
 *
 * DESCRIPTION: update member or insert new member if not exist
 */
void MP1Node::updateMemberList( Member *m, Address *addr, int heartbeat, int timestamp ) {
    int id = getIdFromAddr(addr);
    short port = getPortFromAddr(addr);

// #ifdef FUCK
    if (id > 10 || id <= 0 || port != 0) {
        // cout << id << " " << port << endl;
        return;
    }
// #endif

    for (MemberListEntry me : m->memberList) {
        if (me.getid() == id && me.getport() == port) {
            // update member
            me.setheartbeat(heartbeat > me.heartbeat ? heartbeat : me.heartbeat);
            me.settimestamp(timestamp);
            return;
        }
    }

    // insert new member
    MemberListEntry me(id, port, heartbeat, timestamp);
	m->memberList.push_back(me);
	log->logNodeAdd(&m->addr, addr);
    return;
}

/**
 * FUNCTION NAME: handleRecvJoinRep
 *
 * DESCRIPTION: handle function for Receiving Join Response
 */
void MP1Node::handleRecvJoinRep( Member *m, MessageHdr *msg, int msgSize ) {
    // node has joined the group
    m->inGroup = true;
    return updateMemberList(m, &msg->addr, m->heartbeat, par->getcurrtime());
}

/**
 * FUNCTION NAME: handleRecvJoinReq
 *
 * DESCRIPTION: handle function for Receiving Join Request
 */
void MP1Node::handleRecvJoinReq( Member *m, MessageHdr *msg, int msgSize ) {
    sendMsg(&msg->addr, JOINREP);
    return updateMemberList(m, &msg->addr, m->heartbeat, par->getcurrtime());
}

/**
 * FUNCTION NAME: handleRecvGossipMsg
 *
 * DESCRIPTION: handle function for Receiving Gossiping Message
 */
void MP1Node::handleRecvGossipMsg( Member *m, MessageHdr *msg, int msgSize ) {
    vector<MemberListEntry>::iterator it = msg->memberList.begin();

    while (it != msg->memberList.end()) {
        int id = it->getid();
        short port = it->getport();

        if (id > 10 || id <= 0 || port != 0) {
            cout << id << " " << port << " " << it->heartbeat << " " << it->timestamp << endl;
            it++;
            continue;
        }

        Address addr = Address();
        memset(&addr, 0, sizeof(Address));
        loadAddr(&addr, it->getid(), it->getport());
        updateMemberList(m, &addr, it->heartbeat, it->timestamp);
        it++;
    }

    // update
    updateMemberList(m, &msg->addr, m->heartbeat, par->getcurrtime());
}

/**
 * FUNCTION NAME: gossip
 *
 * DESCRIPTION: gossip protocol
 */
void MP1Node::gossip() {
    unordered_map<string, bool> ht;

    // self
    string idAndPort = getIdAndPortString(getIdFromAddr(&memberNode->addr), getPortFromAddr(&memberNode->addr));
    ht[idAndPort] = true;

    int mlLen = memberNode->memberList.size();
    int count = GOSSIP_NUM < mlLen ? GOSSIP_NUM : mlLen;
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
    while (count && mlLen > 0) {
        it = memberNode->memberList.begin();
        srand(time(NULL));
        int meIndex = rand() % mlLen;
        it += mlLen;
        idAndPort = getIdAndPortString(it->getid(), it->getport());

// #ifdef FUCK
        int id = it->id;
        short port = it->port;
        if (id > 10 || id <= 0 || port != 0) {
            // cout << id << " " << port << endl;
            // count++;
            memberNode->memberList.erase(it);
            mlLen = memberNode->memberList.size();
            continue;
        }
// #endif
        if (ht[idAndPort]) continue;

        Address addr = Address();
        memset(&addr, 0, sizeof(Address));
        loadAddr(&addr, it->getid(), it->getport());
        sendMsg(&addr ,GOSSIP);
        ht[idAndPort] = true;
        --count;
    }

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
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
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

/**
 * FUNCTION NAME: getIdFromAddr
 *
 * DESCRIPTION: get id
 */
int MP1Node::getIdFromAddr(Address *addr) {
    return addr ? *((int*)&(addr->addr)) : 0;
}

/**
 * FUNCTION NAME: getPortFromAddr
 *
 * DESCRIPTION: get port
 */
short MP1Node::getPortFromAddr(Address *addr) {
    return addr ? *((short*)&(addr->addr[4])) : 0;
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
    MessageHdr *msg = new MessageHdr();
    memset(msg, 0, sizeof(MessageHdr));
    msg->msgType = ms;
    msg->heartbeat = memberNode->heartbeat;
    msg->addr = memberNode->addr;
    if (ms == GOSSIP) {
        // gossip msg includes member list
        msg->memberList.assign(memberNode->memberList.begin(), memberNode->memberList.end());
    }

    emulNet->ENsend(&memberNode->addr, addr, (char *)msg, sizeof(MessageHdr));
    delete msg;
}

/**
 * FUNCTION NAME: memberListToHT
 *
 * DESCRIPTION: convert member list to hash table
 
unordered_map<string, int> MP1Node::memberListToHT(vector<MemberListEntry> &ml) {
    unordered_map<string, int> ht;
    MemberListEntry m;
    for (int i = 0; i < ml.size(); ++i) {
        m = ml[i];
        ht[to_string(m.id) + ":" + to_string(m.port)] = i;
    }

    return ht;
} */

/**
 * FUNCTION NAME: getIdAndPortString
 *
 * DESCRIPTION: convert id and port to string and concat them
 */
string MP1Node::getIdAndPortString(int id, short port) {
    return to_string(id) + ":" + to_string(port);
}
