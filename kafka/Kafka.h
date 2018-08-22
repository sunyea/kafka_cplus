//
// Created by liaop on 18-8-20.
//

#ifndef SIMPLESERVER_KAFKA_H
#define SIMPLESERVER_KAFKA_H

#include <iostream>
#include <vector>
#include "librdkafka/rdkafkacpp.h"
#include "../loger/Loger.h"

using namespace std;

typedef enum{
    KFK_ERR_NOERROR         = 0,    //无错误
    KFK_ERR_PRODUCER        = 10,   //生产者相关错误
    KFK_ERR_SEND_FAILED     = 12,   //生产者发送信息失败
    KFK_ERR_CONSUMER        = 20,   //消费者相关错误
    KFK_ERR_CONSUM_FAILED   = 22,   //消费者接收信息失败
} kafka_err;

class Kafka {
public:
    /**
     * 构造函数
     * @param hosts kafka服务器地址，多个地址用逗号隔开
     * @param max_buf 最大收发信息量
     * @param max_retry 消费者超时重试次数
     * @param timeout 消费者超时（毫秒）
     * @param debug 是否是调试模式
     */
    Kafka(string &hosts, int max_buf, int max_retry, int timeout, bool debug);
    Kafka(string &hosts);
    ~Kafka();

    /**
     * 开始启动kafka对象
     * @param in_topic 输入信息的主题名
     * @param out_topic 输出信息的主题名
     * @param consumer_group 消费者组群名
     * @return 正确启动返回true
     */
    kafka_err start(string &in_topic, string &out_topic, string &consumer_group);
    kafka_err start(string &in_topic, string &consumer_group);
    kafka_err start(string &out_topic);

    /**
     * 停止kafka对象
     */
    void stop();

    /**
     * 处理远程请求命令，在具体应用中重载该方法进行具体业务逻辑处理
     * @param message 远程请求消息，json字符串，格式：{'action':'command', 'sessionid':'****', 'data':{...}}
     * @param out_msg 业务处理后的反馈信息，json字符串，格式：{'code':0, 'err':'info', 'sessionid':'****', 'data':{...}}
     */
    virtual void command(string &message, string &out_msg);

    /**
     * 等待处理远程请求命令
     */
    kafka_err waitForAction();

    /**
     * 发送远程请求指令，并等待指令结果
     * @param message 发送给远程的指令，json字符串，格式：{'action':'command', 'sessionid':'****', 'data':{...}}
     * @param out_msg 指令结果，json字符串，格式：{'code':0, 'err':'info', 'sessionid':'****', 'data':{...}}
     */
    kafka_err requestAndResponse(string &message, string &out_msg);

    /**
     * 可持续发送信息
     * @param message 信息内容
     */
    kafka_err send_always(string &message);

    /**
     * 可持续接收信息
     * @param out_msg 收到的信息内容
     */
    kafka_err get_always(string &out_msg);

    /**
     * 发送单条信息到指定主题
     * @param topic 主题名
     * @param message 信息内容
     * @return 如果成功返回true
     */
    kafka_err send_msg(string &topic, string &message);

    /**
     * 从指定主题获取一条信息
     * @param topic 主题名
     * @param consumer_group 消费者组群
     * @param out_msg 获取的信息，如果未获取返回空串
     */
    kafka_err get_msg(string &topic, string &consumer_group, string &out_msg);

    /**
     * 写日志对象
     */
    Loger *_loger;

private:
    string _hosts;
    int _max_buf, _max_retry, _timeout, _debug;

    bool _run = true;

    RdKafka::Producer *_producer = nullptr;
    RdKafka::Consumer *_consumer = nullptr;

    RdKafka::Topic *_topic_producer = nullptr;
    RdKafka::Topic *_topic_consumer = nullptr;

    bool producer_send(string &message);
    bool consumer_get(string &out_message);
    bool init_producer(string &out_topic);
    bool init_consumer(string &in_topic, string &consumer_group);
    void get_session(string &message, string &session);
};


#endif //SIMPLESERVER_KAFKA_H
