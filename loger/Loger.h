//
// Created by liaop on 18-8-21.
//

#ifndef LOG4_LOGER_H
#define LOG4_LOGER_H

#include <iostream>
#include <log4cplus/logger.h>
using namespace std;

class Loger {
public:
    Loger(char *name, char *propert);
    Loger(char *name);

    void log(const char *type, const char *msg);
    void trace(const char* msg);
    void debug(const char *msg);
    void info(const char* msg);
    void warn(const char* msg);
    void error(const char* msg);
    void fatal(const char* msg);

private:
    log4cplus::Logger _logger;
};


#endif //LOG4_LOGER_H
