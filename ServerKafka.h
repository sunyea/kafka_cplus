//
// Created by liaop on 18-8-21.
//

#ifndef SIMPLESERVER_SERVERKAFKA_H
#define SIMPLESERVER_SERVERKAFKA_H

#include "kafka/Kafka.h"

class ServerKafka: public Kafka {
public:
    ServerKafka(string &hosts);

    void command(string &message, string &out_msg);
};


#endif //SIMPLESERVER_SERVERKAFKA_H
