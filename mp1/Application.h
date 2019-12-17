/**********************************
 * FILE NAME: Application.h
 *
 * DESCRIPTION: Header file of all classes pertaining to the Application Layer
 **********************************/

#ifndef _APPLICATION_H_
#define _APPLICATION_H_

#include "EmulNet.h"
#include "Log.h"
#include "MP1Node.h"
#include "Member.h"
#include "Params.h"
#include "Queue.h"
#include "stdincludes.h"

/**
 * global variables
 */
int nodeCount = 0;

/*
 * Macros
 */
#define ARGS_COUNT 2
#define TOTAL_RUNNING_TIME 700

/**
 * CLASS NAME: Application
 *
 * DESCRIPTION: Application layer of the distributed system
 */
class Application {
   private:
    // Address for introduction to the group
    // Coordinator Node
    char JOINADDR[30];
    EmulNet *en;
    Log *log;
    MP1Node **mp1;
    Params *par;

   public:
    Application(char *);
    virtual ~Application();
    Address getjoinaddr();
    int run();
    void mp1Run();
    void fail();
};

#endif /* _APPLICATION_H__ */
