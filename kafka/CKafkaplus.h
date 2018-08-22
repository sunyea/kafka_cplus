//
// Created by liaop on 18-8-1.
//

#ifndef MDPRODUCER_CKAFKAPLUS_H
#define MDPRODUCER_CKAFKAPLUS_H

#include <iostream>

#include "librdkafka/rdkafkacpp.h"

using namespace std;

typedef enum {
    ///无错误
            KAFKA_ERR_NOERR = 0,

    ///消费者其他错
            KAFKA_ERR_CON_OTHER = 10,
    ///消费者创建失败
            KAFKA_ERR_CON_CREATE = 11,
    ///消费者主题创建失败
            KAFKA_ERR_CON_TOPIC_CREATE = 12,
    ///消费者订阅失败
            KAFKA_ERR_CON_SUBSCRIBE = 13,
    ///消费者设置失败
            KAFKA_ERR_CON_CONF = 14,
    ///消费者启动失败
            KAFKA_ERR_CON_START = 15,

    ///生产者其他错误
            KAFKA_ERR_PRO_OTHER = 20,
    ///生产者创建失败
            KAFKA_ERR_PRO_CREATE = 21,
    ///生产者主题创建失败
            KAFKA_ERR_PRO_TOPIC_CREATE = 22,
    ///生产者设置失败
            KAFKA_ERR_PRO_CONF = 23
} kafka_err;

/******************************
 *  命令处理的回调函数
 *
 * @param msg: 远程请求的消息数据
 * @param out: 处理后返回的消息数据
 * @param topic: 回传的主题名称
 */
typedef void (*Fun_callback)(string&, string&);

class CKafkaplus {
public:
    CKafkaplus(string &hosts);
    ~CKafkaplus();

    /******************************
     * 等待远程请求，并进行相应处理
     *
     * @param timeout 获取超时
     * @param callback 指令处理回调函数
     */
    void waitForAction(string &in_topic, string &out_topic, string &group, int timeout, Fun_callback callback);

    /******************************
     * 发出远程请求，并等待服务端回复
     *
     * @param msg 发出的请求
     * @param out 获得的回复
     * @param timeout 获取回复的时限（毫秒）
     */
    void requestAndResponse(string &in_topic, string &out_topic, string &group, string &msg, string &out, int timeout);

    /********************************
     * 单独发送一条信息到指定主题
     *
     * @param topic 主题名
     * @param msg 待法信息
     * @return 是否发送成功
     */
    bool send_msg(string &topic, string &msg);

    string get_msg(string &topic, string &group, int timeout);

private:
    /******************************
     * 初始化kafka，准备开始工作
     *
     * @param in_topic 输入的主题名
     * @param out_topic 输出的主题名
     * @param group 群组名
     * @return 错误信息
     */
    kafka_err start(string &in_topic, string &out_topic, string &group);

    ///停止kafka工作，销毁其对象
    void stop();

    void todoForAction(RdKafka::Message *msg, Fun_callback callback);
    void todoForResponse(RdKafka::Message *msg, string &out, string &sessionid);
    void send(string &msg);

private:
    string _hosts;
    bool _run = true;

    RdKafka::Producer *_producer = nullptr;
    RdKafka::Consumer *_consumer = nullptr;

    RdKafka::Topic *_topic_producer = nullptr;
    RdKafka::Topic *_topic_consumer = nullptr;

};


#endif //MDPRODUCER_CKAFKAPLUS_H
