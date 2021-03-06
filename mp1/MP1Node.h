/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include <unordered_map>

#include "EmulNet.h"
#include "Log.h"
#include "Member.h"
#include "Params.h"
#include "Queue.h"
#include "stdincludes.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5
#define ADDR_LEN 6
#define GOSSIP_NUM (par->EN_GPSZ) / 3
#define DATA_FRAME_SIZE (ADDR_LEN + sizeof(long) + 1)

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes { JOINREQ, JOINREP, GOSSIP, DUMMYLASTMSGTYPE };

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
    enum MsgTypes msgType;
    char addr[ADDR_LEN];
    long timestamp;
    size_t dataSize;
    size_t sentSize;
} MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for
 * failure detection
 */
class MP1Node {
   private:
    EmulNet *emulNet;
    Log *log;
    Params *par;
    Member *memberNode;
    char NULLADDR[ADDR_LEN];

   public:
    MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
    Member *getMemberNode() { return memberNode; }
    int recvLoop();
    static int enqueueWrapper(void *env, char *buff, int size);
    void nodeStart(char *servaddrstr, short serverport);
    int initThisNode(Address *joinaddr);
    int introduceSelfToGroup(Address *joinAddress);
    int finishUpThisNode();
    void nodeLoop();
    void checkMessages();
    bool recvCallBack(void *env, char *data, int size);
    void nodeLoopOps();
    int isNullAddress(Address *addr);
    static Address getJoinAddress();
    void initMemberListTable(Member *memberNode);
    void printAddress(Address *addr) const;
    virtual ~MP1Node();

   private:
    void handleRecvJoinReq(Member *m, MessageHdr *msg, int msgSize);
    void handleRecvJoinRep(Member *m, MessageHdr *msg, int msgSize);
    void handleRecvGossipMsg(Member *m, MessageHdr *msg, int msgSize);
    void updateMemberList(Member *m, char *addr, int heartbeat, int timestamp);
    void gossip(unordered_map<string, bool> &exclude, long timestamp);

    // util func
    static int getIdFromAddr(char *addr);
    static short getPortFromAddr(char *addr);
    static void loadAddr(Address *addr, int id, short port);
    void sendMsg(Address *addr, MsgTypes ms);
    void sendMsg(Address *addr, MsgTypes ms, vector<MemberListEntry> &sent, long timestamp);

    // unordered_map<string, int> memberListToHT(vector<MemberListEntry> &ml);
    static string getIdAndPortString(int id, short port);
    void loadIdAndPortFromString(string idAndPort, int &id, short &port);

    // serialize and deserialize
    void marshall(char *_dest, vector<MemberListEntry> &m, vector<MemberListEntry> &sent);
    void unmarshall(char *_src, size_t dataSize, size_t sentSize, vector<MemberListEntry> &m, vector<MemberListEntry> &sent);
};

#endif /* _MP1NODE_H_ */
