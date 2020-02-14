/**********************************
 * FILE NAME: stdincludes.h
 *
 * DESCRIPTION: standard header file
 **********************************/

#ifndef _STDINCLUDES_H_
#define _STDINCLUDES_H_

/*
 * Macros
 */
#define RING_SIZE 512
#define FAILURE -1
#define SUCCESS 0

/*
 * Standard Header files
 */
#include <assert.h>
#include <execinfo.h>
#include <fcntl.h>
#include <math.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <queue>
#include <string>
#include <vector>

using namespace std;

#define STDCLLBKARGS (void *env, char *data, int size)
#define STDCLLBKRET void
#define DEBUGLOG 1

#endif /* _STDINCLUDES_H_ */
