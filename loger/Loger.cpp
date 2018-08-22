//
// Created by liaop on 18-8-21.
//

#include "Loger.h"
#include <log4cplus/configurator.h>
#include <log4cplus/loggingmacros.h>

using namespace log4cplus;
using namespace log4cplus::helpers;

Loger::Loger(char *name, char *propert) {
    this->_logger = Logger::getInstance(LOG4CPLUS_TEXT(name));
    Logger root = Logger::getRoot();
    PropertyConfigurator::doConfigure(LOG4CPLUS_TEXT(propert));
}

Loger::Loger(char *name) {
    this->_logger = Logger::getInstance(LOG4CPLUS_TEXT(name));
    Logger root = Logger::getRoot();
    PropertyConfigurator::doConfigure(LOG4CPLUS_TEXT("../log4cplus.properties"));
}

void Loger::log(const char *type, const char *msg) {
    if(type == "trace")
        LOG4CPLUS_TRACE(this->_logger, LOG4CPLUS_TEXT(msg));
    else if(type == "debug")
        LOG4CPLUS_DEBUG(this->_logger, LOG4CPLUS_TEXT(msg));
    else if(type == "info")
        LOG4CPLUS_INFO(this->_logger, LOG4CPLUS_TEXT(msg));
    else if(type == "warn")
        LOG4CPLUS_WARN(this->_logger, LOG4CPLUS_TEXT(msg));
    else if(type == "error")
        LOG4CPLUS_ERROR(this->_logger, LOG4CPLUS_TEXT(msg));
    else if(type == "fatal")
        LOG4CPLUS_FATAL(this->_logger, LOG4CPLUS_TEXT(msg));
    else
        LOG4CPLUS_DEBUG(this->_logger, LOG4CPLUS_TEXT(msg));
}

void Loger::trace(const char *msg) {
    this->log("trace", msg);
}

void Loger::debug(const char *msg) {
    this->log("debug", msg);
}

void Loger::info(const char *msg) {
    this->log("info", msg);
}

void Loger::warn(const char *msg) {
    this->log("warn", msg);
}

void Loger::error(const char *msg) {
    this->log("error", msg);
}

void Loger::fatal(const char *msg) {
    this->log("fatal", msg);
}