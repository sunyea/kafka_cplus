#include <iostream>
#include <unistd.h>
#include <cstring>
#include "ServerKafka.h"

using namespace std;


///测试获取指令，并执行指令，将结果返回
int main(int argc, char **argv) {

    //定义参数
    string hosts = "192.168.100.70:9092,192.168.100.71:9092,192.168.100.72:9092";
    string in_topic = "tp.test.common";
    string out_topic = "tp.test.common.response";
    string group = "gp.test.cplus";

    kafka_err rt;

    ServerKafka *kafka = new ServerKafka(hosts);
    kafka->_loger->info("开始启动服务...");
    rt = kafka->start(in_topic, out_topic, group);
    if(rt != KFK_ERR_NOERROR) {
        kafka->_loger->error("启动kafka失败");
        return 1;
    }
    while (true) {
        rt = kafka->waitForAction();
        if(rt != KFK_ERR_NOERROR) {
            kafka->_loger->error("出现异常，重新启动kafka");
            kafka->stop();
            kafka->start(in_topic, out_topic, group);
        }
    }
    return 0;
}