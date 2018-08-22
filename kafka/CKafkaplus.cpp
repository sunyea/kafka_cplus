//
// Created by liaop on 18-8-1.
//

#include <cstring>
#include <time.h>
#include <zconf.h>
#include "CKafkaplus.h"
#include "../json/cJSON.h"

CKafkaplus::CKafkaplus(string &hosts) {
    this->_hosts = hosts;
}

CKafkaplus::~CKafkaplus() {
    //
}

kafka_err CKafkaplus::start(string &in_topic, string &out_topic, string &group) {
    string strerr;
    RdKafka::Conf *_conf_producer = nullptr;
    RdKafka::Conf *_conf_consumer = nullptr;
    RdKafka::Conf *_conf_topic_producer = nullptr;
    RdKafka::Conf *_conf_topic_consumer = nullptr;

    ///********************Producer部分*******************
    ///
    //创建producer配置，并设置
    _conf_producer = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (!_conf_producer) {
        fprintf(stderr, "创建producer配置失败\n");
        return KAFKA_ERR_PRO_CONF;
    }
    if (_conf_producer->set("bootstrap.servers", this->_hosts, strerr) != RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "为producer设置服务器地址失败，原因：%s\n", strerr.c_str());
        return KAFKA_ERR_PRO_CONF;
    }

    //创建producer实例
    this->_producer = RdKafka::Producer::create(_conf_producer, strerr);
    if (!(this->_producer)) {
        fprintf(stderr, "创建producer实例失败，原因：%s\n", strerr.c_str());
        return KAFKA_ERR_PRO_CREATE;
    }
    delete _conf_producer;

    //创建producer的topic配置
    _conf_topic_producer = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (!_conf_topic_producer) {
        fprintf(stderr, "创建producer的topic配置失败\n");
        return KAFKA_ERR_PRO_CONF;
    }

    //创建producer的topic实例
    this->_topic_producer = RdKafka::Topic::create(this->_producer, out_topic, _conf_topic_producer, strerr);
    if (!(this->_topic_producer)) {
        fprintf(stderr, "创建producer的topic实例失败，原因：%s\n", strerr.c_str());
        return KAFKA_ERR_PRO_TOPIC_CREATE;
    }
    delete _conf_topic_producer;


    ///********************Consumer部分*******************
    ///
    //创建consumer配置，并设置
    _conf_consumer = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (!_conf_consumer) {
        fprintf(stderr, "创建consumer配置失败\n");
        return KAFKA_ERR_CON_CONF;
    }
    if (_conf_consumer->set("bootstrap.servers", this->_hosts, strerr) != RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "为consumer设置服务器地址失败，原因：%s\n", strerr.c_str());
        return KAFKA_ERR_CON_CONF;
    }
    if (_conf_consumer->set("group.id", group, strerr) != RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "为consumer设置group失败，原因：%s\n", strerr.c_str());
        return KAFKA_ERR_CON_CONF;
    }

    //创建consumer实例
    this->_consumer = RdKafka::Consumer::create(_conf_consumer, strerr);
    if (!(this->_consumer)) {
        fprintf(stderr, "创建consumer实例失败，原因：%s\n", strerr.c_str());
        return KAFKA_ERR_CON_CREATE;
    }
    delete _conf_consumer;

    //创建consumer的topic配置
    _conf_topic_consumer = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (!_conf_topic_consumer) {
        fprintf(stderr, "创建consumer的topic配置失败\n");
        return KAFKA_ERR_CON_CONF;
    }

    //创建consumer的topic实例
    this->_topic_consumer = RdKafka::Topic::create(this->_consumer, in_topic, _conf_topic_consumer, strerr);
    if (!(this->_topic_consumer)) {
        fprintf(stderr, "创建consumer的topic实例失败，原因：%s\n", strerr.c_str());
        return KAFKA_ERR_CON_TOPIC_CREATE;
    }
    delete _conf_topic_consumer;

    //启动consumer实例
    RdKafka::ErrorCode err_resp = this->_consumer->start(this->_topic_consumer, NULL, RdKafka::Topic::OFFSET_END);
    if (err_resp != RdKafka::ERR_NO_ERROR) {
        fprintf(stderr, "启动consumer失败，原因:%s\n", RdKafka::err2str(err_resp));
        return KAFKA_ERR_CON_START;
    }
    this->_run = true;
    return KAFKA_ERR_NOERR;
}

void CKafkaplus::stop() {
    this->_run = false;
    //停止和销毁producer部分
    if (this->_topic_producer) {
        delete this->_topic_producer;
        this->_topic_producer = nullptr;
    }
    if (this->_producer) {
        delete this->_producer;
        this->_producer = nullptr;
    }

    //停止和销毁consumer部分
    this->_consumer->stop(this->_topic_consumer, NULL);
    if (this->_topic_consumer) {
        delete this->_topic_consumer;
        this->_topic_consumer = nullptr;
    }
    if (this->_consumer) {
        delete this->_consumer;
        this->_consumer = nullptr;
    }
    RdKafka::wait_destroyed(5000);
}

void CKafkaplus::waitForAction(string &in_topic, string &out_topic, string &group, int timeout, Fun_callback callback) {
    if((this->start(in_topic, out_topic, group)) != KAFKA_ERR_NOERR) {
        fprintf(stderr, "初始化Kafka失败\n");
        return;
    }
    RdKafka::Message *_msg = nullptr;
    while (this->_run) {
        _msg = this->_consumer->consume(this->_topic_consumer, NULL, timeout);
        this->todoForAction(_msg, callback);
        this->_consumer->poll(0);
        delete _msg;
    }
}

void CKafkaplus::send(string &msg) {
    RdKafka::ErrorCode err_resp = this->_producer->produce(this->_topic_producer, RdKafka::Topic::PARTITION_UA,
                                                           RdKafka::Producer::RK_MSG_COPY,
                                                           (void *)msg.c_str(), msg.size(),
                                                           NULL, NULL);
    if (err_resp != RdKafka::ERR_NO_ERROR) {
        fprintf(stderr, "发送失败，原因：%s\n", RdKafka::err2str(err_resp));
        return;
    }
    this->_producer->poll(0);
    fprintf(stdout, "成功发送：%s\n", msg.c_str());
}

void CKafkaplus::todoForAction(RdKafka::Message *msg, Fun_callback callback) {
    if (!msg) {
        return;
    }
    string msg_payload, msg_out, topic_name;

    string strerr;
    RdKafka::Conf *_conf_topic_producer = nullptr;

    switch (msg->err()){
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__PARTITION_EOF:
            //超时，无新信息
            break;
        case RdKafka::ERR_NO_ERROR:
            //正确获取
            msg_payload = (char *)(msg->payload());
            fprintf(stdout, "成功接收信息：%s\n", msg_payload.c_str());
            (*callback)(msg_payload, msg_out);

            if(!(msg_out.empty())){
                this->send(msg_out);
            }
            break;
        case RdKafka::ERR__UNKNOWN_TOPIC:
        case RdKafka::ERR__UNKNOWN_PARTITION:
            fprintf(stderr, "不可识别的主题或分区：%s\n", msg->errstr());
            this->_run = false;
            break;
        default:
            fprintf(stderr, "其他错误：%s\n", msg->errstr());
            this->_run = false;
            break;
    }
    delete _conf_topic_producer;
}

void CKafkaplus::requestAndResponse(string &in_topic, string &out_topic, string &group, string &msg, string &out, int timeout) {
    string sessionid;
    clock_t start, ends;
    int ms;

    //解析信息，获取sessionid
    cJSON *j_root, *session;
    j_root = cJSON_Parse(msg.c_str());
    session = cJSON_GetObjectItem(j_root, "sessionid");
    if (session) {
        sessionid = session->valuestring;
    }
    cJSON_Delete(j_root);

    if (sessionid.empty()){
        fprintf(stderr, "待发送的信息没有sessionid，发送失败\n");
        out = "{'code': -1, 'err': 'sessionid异常', 'sessionid': none, 'data': none}";
        return;
    }

    if((this->start(in_topic, out_topic, group)) != KAFKA_ERR_NOERR) {
        fprintf(stderr, "初始化Kafka失败\n");
        out = "{'code': -2, 'err': '初始化Kafka失败', 'sessionid': '" + sessionid + "', 'data': none}";
        return;
    }
    this->send(msg);

    //计算超时(毫秒)
    start = clock();
    RdKafka::Message *_msg = nullptr;
    while(this->_run) {
        ends = clock();
        ms = (int)(ends-start);
        if(ms > timeout) {
            fprintf(stderr, "获取信息超时\n");
            out = "{'code': -3, 'err': '获取信息超时', 'sessionid': '" + sessionid + "', 'data': none}";
            break;
        }
        _msg = this->_consumer->consume(this->_topic_consumer, NULL, timeout);
        this->todoForResponse(_msg, out, sessionid);
        this->_consumer->poll(0);
        delete _msg;
    }
    this->stop();
}

void CKafkaplus::todoForResponse(RdKafka::Message *msg, string &out, string &sessionid) {
    if (!msg) {
        return;
    }

    string payload;
    cJSON *j_root, *session;
    switch (msg->err()){
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__PARTITION_EOF:
            //超时，无新信息
            break;
        case RdKafka::ERR_NO_ERROR:
            //正确获取
            payload = (char *)msg->payload();
            //通过sessionid判断是否是对应的回复
            j_root = cJSON_Parse((char *)msg->payload());
            session = cJSON_GetObjectItem(j_root, "sessionid");
            if (session) {
                if (!strcmp(session->valuestring, sessionid.c_str())) {
                    out = payload;
                    this->_run = false;
                }
            }
            break;
        case RdKafka::ERR__UNKNOWN_TOPIC:
        case RdKafka::ERR__UNKNOWN_PARTITION:
            fprintf(stderr, "不可识别的主题或分区：%s\n", msg->errstr());
            this->_run = false;
            break;
        default:
            fprintf(stderr, "其他错误：%s\n", msg->errstr());
            this->_run = false;
            break;
    }
}

bool CKafkaplus::send_msg(string &topic, string &msg) {
    bool rt = true;
    string strerr;
    RdKafka::Conf *_conf_producer = nullptr;
    RdKafka::Conf *_conf_topic_producer = nullptr;
    //创建producer配置，并设置
    _conf_producer = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (!_conf_producer) {
        fprintf(stderr, "创建producer配置失败\n");
        return false;
    }
    if (_conf_producer->set("bootstrap.servers", this->_hosts, strerr) != RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "为producer设置服务器地址失败，原因：%s\n", strerr.c_str());
        delete _conf_producer;
        return false;
    }

    //创建producer实例
    this->_producer = RdKafka::Producer::create(_conf_producer, strerr);
    if (!(this->_producer)) {
        fprintf(stderr, "创建producer实例失败，原因：%s\n", strerr.c_str());
        delete _conf_producer;
        return false;
    }
    delete _conf_producer;

    //创建producer的topic配置
    _conf_topic_producer = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (!_conf_topic_producer) {
        fprintf(stderr, "创建producer的topic配置失败\n");
        return false;
    }

    //创建producer的topic实例
    this->_topic_producer = RdKafka::Topic::create(this->_producer, topic, _conf_topic_producer, strerr);
    if (!(this->_topic_producer)) {
        fprintf(stderr, "创建producer的topic实例失败，原因：%s\n", strerr.c_str());
        delete _conf_topic_producer;
        return  false;
    }
    delete _conf_topic_producer;

    RdKafka::ErrorCode err_resp = this->_producer->produce(this->_topic_producer, RdKafka::Topic::PARTITION_UA,
                                                               RdKafka::Producer::RK_MSG_COPY,
                                                               (void *)msg.c_str(), msg.size(),
                                                               NULL, NULL);
    if (err_resp != RdKafka::ERR_NO_ERROR) {
        fprintf(stderr, "发送失败，原因：%s\n", RdKafka::err2str(err_resp));
        rt = false;
    }

    if (rt) {
        this->_producer->poll(0);
        fprintf(stdout, "成功发送：%s\n", msg.c_str());
    }

    //停止和销毁producer部分
    sleep(2);
    if (this->_topic_producer) {
        delete this->_topic_producer;
        this->_topic_producer = nullptr;
    }
    if (this->_producer) {
        delete this->_producer;
        this->_producer = nullptr;
    }
    RdKafka::wait_destroyed(5000);
    return rt;
}

string CKafkaplus::get_msg(string &topic, string &group, int timeout) {
    string strerr, rt;
    RdKafka::Conf *_conf_consumer = nullptr;
    RdKafka::Conf *_conf_topic_consumer = nullptr;
    //创建consumer配置，并设置
    _conf_consumer = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (!_conf_consumer) {
        fprintf(stderr, "创建consumer配置失败\n");
        return rt;
    }
    if (_conf_consumer->set("bootstrap.servers", this->_hosts, strerr) != RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "为consumer设置服务器地址失败，原因：%s\n", strerr.c_str());
        delete _conf_consumer;
        return rt;
    }
    if (_conf_consumer->set("group.id", group, strerr) != RdKafka::Conf::CONF_OK) {
        fprintf(stderr, "为consumer设置group失败，原因：%s\n", strerr.c_str());
        delete _conf_consumer;
        return rt;
    }

    //创建consumer实例
    this->_consumer = RdKafka::Consumer::create(_conf_consumer, strerr);
    if (!(this->_consumer)) {
        fprintf(stderr, "创建consumer实例失败，原因：%s\n", strerr.c_str());
        delete _conf_consumer;
        return rt;
    }
    delete _conf_consumer;

    //创建consumer的topic配置
    _conf_topic_consumer = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (!_conf_topic_consumer) {
        fprintf(stderr, "创建consumer的topic配置失败\n");
        return rt;
    }

    //创建consumer的topic实例
    this->_topic_consumer = RdKafka::Topic::create(this->_consumer, topic, _conf_topic_consumer, strerr);
    if (!(this->_topic_consumer)) {
        fprintf(stderr, "创建consumer的topic实例失败，原因：%s\n", strerr.c_str());
        delete _conf_topic_consumer;
        return rt;
    }
    delete _conf_topic_consumer;

    //启动consumer实例
    RdKafka::ErrorCode err_resp = this->_consumer->start(this->_topic_consumer, NULL, RdKafka::Topic::OFFSET_END);
    if (err_resp != RdKafka::ERR_NO_ERROR) {
        fprintf(stderr, "启动consumer失败，原因:%s\n", RdKafka::err2str(err_resp));
        return rt;
    }
    this->_run = true;

    RdKafka::Message *_msg = nullptr;
    while (this->_run) {
        _msg = this->_consumer->consume(this->_topic_consumer, NULL, timeout);
        switch (_msg->err()){
            case RdKafka::ERR__TIMED_OUT:
            case RdKafka::ERR__PARTITION_EOF:
                //超时，无新信息
                break;
            case RdKafka::ERR_NO_ERROR:
                //正确获取
                rt = (char *)(_msg->payload());
                fprintf(stdout, "成功接收信息：%s\n", rt.c_str());
                this->_run = false;
                break;
            case RdKafka::ERR__UNKNOWN_TOPIC:
            case RdKafka::ERR__UNKNOWN_PARTITION:
                fprintf(stderr, "不可识别的主题或分区：%s\n", _msg->errstr());
                this->_run = false;
                break;
            default:
                fprintf(stderr, "其他错误：%s\n", _msg->errstr());
                this->_run = false;
                break;
        }
        this->_consumer->poll(0);
        delete _msg;
    }

    //停止和销毁consumer部分
    this->_consumer->stop(this->_topic_consumer, NULL);
    if (this->_topic_consumer) {
        delete this->_topic_consumer;
        this->_topic_consumer = nullptr;
    }
    if (this->_consumer) {
        delete this->_consumer;
        this->_consumer = nullptr;
    }
    RdKafka::wait_destroyed(5000);
    return rt;
}