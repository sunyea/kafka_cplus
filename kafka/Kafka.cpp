//
// Created by liaop on 18-8-20.
//
#include <sstream>
#include <cstring>
#include "Kafka.h"
#include "log4cplus/log4cplus.h"
#include "../json/cJSON.h"

Kafka::Kafka(string &hosts, int max_buf, int max_retry, int timeout, bool debug) {
    this->_hosts = hosts;
    this->_max_buf = max_buf;
    this->_max_retry = max_retry;
    this->_timeout = timeout;
    this->_debug = debug;
    this->_loger = new Loger("Kakfa");
}

Kafka::Kafka(string &hosts) {
    this->_hosts = hosts;
    this->_max_buf = 999950;
    this->_max_retry = 4;
    this->_timeout = 6000;
    this->_debug = true;
    this->_loger = new Loger("Kafka");
}

bool Kafka::init_producer(string &out_topic) {
    string strerr;
    stringstream sserr;
    RdKafka::Conf *_conf_producer = nullptr;
    RdKafka::Conf *_conf_topic_producer = nullptr;

    ///********************Producer部分*******************
    ///
    //创建producer配置，并设置
    _conf_producer = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (!_conf_producer) {
        this->_loger->error("创建producer配置失败");
        return false;
    }
    if (_conf_producer->set("bootstrap.servers", this->_hosts, strerr) != RdKafka::Conf::CONF_OK) {
        sserr << "为producer设置服务器地址失败，原因：" << strerr;
        this->_loger->error(sserr.str().c_str());
        sserr.str("");
        return false;
    }

    //创建producer实例
    this->_producer = RdKafka::Producer::create(_conf_producer, strerr);
    if (!(this->_producer)) {
        sserr << "创建producer实例失败，原因：" << strerr;
        this->_loger->error(sserr.str().c_str());
        sserr.str("");
        return false;
    }
    delete _conf_producer;

    //创建producer的topic配置
    _conf_topic_producer = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (!_conf_topic_producer) {
        this->_loger->error("创建producer的topic配置失败");
        return false;
    }

    //创建producer的topic实例
    this->_topic_producer = RdKafka::Topic::create(this->_producer, out_topic, _conf_topic_producer, strerr);
    if (!(this->_topic_producer)) {
        sserr << "创建producer的topic实例失败，原因：" << strerr;
        this->_loger->error(sserr.str().c_str());
        sserr.str("");
        return false;
    }
    delete _conf_topic_producer;
    return true;
}

bool Kafka::init_consumer(string &in_topic, string &consumer_group) {
    string strerr;
    stringstream sserr;
    RdKafka::Conf *_conf_consumer = nullptr;
    RdKafka::Conf *_conf_topic_consumer = nullptr;

    ///********************Consumer部分*******************
    ///
    //创建consumer配置，并设置
    _conf_consumer = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    if (!_conf_consumer) {
        this->_loger->error("创建consumer配置失败");
        return false;
    }
    if (_conf_consumer->set("bootstrap.servers", this->_hosts, strerr) != RdKafka::Conf::CONF_OK) {
        sserr << "为consumer设置服务器地址失败，原因：" << strerr;
        this->_loger->error(sserr.str().c_str());
        sserr.str("");
        return false;
    }
    if (_conf_consumer->set("group.id", consumer_group, strerr) != RdKafka::Conf::CONF_OK) {
        sserr << "为consumer设置group失败，原因：" << strerr;
        this->_loger->error(sserr.str().c_str());
        sserr.str("");
        return false;
    }

    //创建consumer实例
    this->_consumer = RdKafka::Consumer::create(_conf_consumer, strerr);
    if (!(this->_consumer)) {
        sserr << "创建consumer实例失败，原因：" << strerr;
        this->_loger->error(sserr.str().c_str());
        sserr.str("");
        return false;
    }
    delete _conf_consumer;

    //创建consumer的topic配置
    _conf_topic_consumer = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    if (!_conf_topic_consumer) {
        this->_loger->error("创建consumer的topic配置失败");
        return false;
    }

    //创建consumer的topic实例
    this->_topic_consumer = RdKafka::Topic::create(this->_consumer, in_topic, _conf_topic_consumer, strerr);
    if (!(this->_topic_consumer)) {
        sserr << "创建consumer的topic实例失败，原因：" << strerr;
        this->_loger->error(sserr.str().c_str());
        sserr.str("");
        return false;
    }
    delete _conf_topic_consumer;

    //启动consumer实例
    RdKafka::ErrorCode err_resp = this->_consumer->start(this->_topic_consumer, NULL, RdKafka::Topic::OFFSET_END);
    if (err_resp != RdKafka::ERR_NO_ERROR) {
        sserr << "启动consumer失败，原因:" << strerr;
        this->_loger->error(sserr.str().c_str());
        sserr.str("");
        return false;
    }
    return true;
}

kafka_err Kafka::start(string &in_topic, string &out_topic, string &consumer_group) {
    if(!(this->init_producer(out_topic)))
        return KFK_ERR_PRODUCER;

    if(!(this->init_consumer(in_topic, consumer_group)))
        return KFK_ERR_CONSUMER;

    this->_run = true;
    return KFK_ERR_NOERROR;
}

kafka_err Kafka::start(string &in_topic, string &consumer_group) {
    if(!(this->init_consumer(in_topic, consumer_group)))
        return KFK_ERR_CONSUMER;

    this->_run = true;
    return KFK_ERR_NOERROR;
}

kafka_err Kafka::start(string &out_topic) {
    if(!(this->init_producer(out_topic)))
        return KFK_ERR_PRODUCER;

    this->_run = true;
    return KFK_ERR_NOERROR;
}

void Kafka::stop() {
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

void Kafka::command(string &message, string &out_msg) {
    //解析获取到的信息
    cJSON *get_root, *action, *sessionid, *data;
    get_root = cJSON_Parse(message.c_str());
    action = cJSON_GetObjectItem(get_root, "action");
    sessionid = cJSON_GetObjectItem(get_root, "sessionid");
    data = cJSON_GetObjectItem(get_root, "data");
    if(action and sessionid and data){
        //执行resp命令
        string _action = action->valuestring;
        if(!strcmp("resp", _action.c_str())){
            //拼接返回信息
            cJSON *sen_root, *rep_data;
            sen_root = cJSON_CreateObject();
            cJSON_AddNumberToObject(sen_root, "code", 0);
            cJSON_AddStringToObject(sen_root, "sessionid", sessionid->valuestring);
            cJSON_AddStringToObject(sen_root, "err", "成功");
            rep_data = cJSON_CreateObjectReference(data->child);
            cJSON_AddItemToObject(sen_root, "data", rep_data);
            out_msg = cJSON_Print(sen_root);
            cJSON_Delete(sen_root);
        }else{
            //返回错误信息
            string err_txt = "未知命令：";
            string err_action = err_txt+_action;
            cJSON *sen_root, *rep_data;
            sen_root = cJSON_CreateObject();
            cJSON_AddNumberToObject(sen_root, "code", -1);
            cJSON_AddStringToObject(sen_root, "sessionid", sessionid->valuestring);
            cJSON_AddStringToObject(sen_root, "err", err_action.c_str());
            rep_data = cJSON_CreateObjectReference(data->child);
            cJSON_AddItemToObject(sen_root, "data", rep_data);
            out_msg = cJSON_Print(sen_root);
            cJSON_Delete(sen_root);
        }
    }
    cJSON_Delete(get_root);
}

bool Kafka::producer_send(string &message) {
    stringstream sserr;
    if(!(this->_run)){
        this->_loger->error("对象还未启动！" );
        return false;
    }
    if(message.size() > this->_max_buf){
        sserr << "消息大小超过限制：" << this->_max_buf;
        this->_loger->error(sserr.str().c_str());
        sserr.str("");
        return false;
    }
    RdKafka::ErrorCode err_resp = this->_producer->produce(this->_topic_producer, RdKafka::Topic::PARTITION_UA,
                                                           RdKafka::Producer::RK_MSG_COPY,
                                                           (void *)message.c_str(), message.size(),
                                                           NULL, NULL);
    if (err_resp != RdKafka::ERR_NO_ERROR) {
        sserr << "发送失败，原因：" << RdKafka::err2str(err_resp);
        this->_loger->error(sserr.str().c_str());
        sserr.str("");
        return false;
    }
    this->_producer->poll(0);
    if(this->_debug) {
        sserr << "成功发送信息：" << message;
        this->_loger->debug(sserr.str().c_str());
        sserr.str("");
    }
    return true;
}

bool Kafka::consumer_get(string &out_message) {
    RdKafka::Message *_msg = nullptr;
    stringstream sserr;
    bool rt;

    if(!(this->_run)){
        this->_loger->error("对象还未启动！" );
        return false;
    }

    _msg = this->_consumer->consume(this->_topic_consumer, NULL, this->_timeout);
    switch (_msg->err()){
        case RdKafka::ERR__TIMED_OUT:
        case RdKafka::ERR__PARTITION_EOF:
            //超时，无新信息
            out_message = "";
            rt = true;
            break;
        case RdKafka::ERR_NO_ERROR:
            //正确获取
            out_message = (char *)(_msg->payload());
            sserr << "成功收到信息：" << out_message;
            this->_loger->debug(sserr.str().c_str());
            sserr.str("");
            rt = true;
            break;
        case RdKafka::ERR__UNKNOWN_TOPIC:
        case RdKafka::ERR__UNKNOWN_PARTITION:
            sserr << "不可识别的主题或分区：" << _msg->errstr();
            this->_loger->error(sserr.str().c_str());
            sserr.str("");
            rt = false;
            break;
        default:
            sserr << "其他错误：" << _msg->errstr();
            this->_loger->error(sserr.str().c_str());
            sserr.str("");
            rt = false;
            break;
    }
    this->_consumer->poll(0);
    delete _msg;
    return rt;
}

void Kafka::get_session(string &message, string &session) {
    //解析信息，获取sessionid
    cJSON *j_root, *j_session;
    j_root = cJSON_Parse(message.c_str());
    j_session = cJSON_GetObjectItem(j_root, "sessionid");
    if (j_session) {
        session = j_session->valuestring;
    }
    cJSON_Delete(j_root);
}

kafka_err Kafka::waitForAction() {
    string msg_payload, msg_out;

    while(this->_run) {
        if (!(this->consumer_get(msg_payload)))
            return KFK_ERR_CONSUM_FAILED;
        if(!(msg_payload.empty())) {
            this->command(msg_payload, msg_out);
            if (!(this->producer_send(msg_out)))
                return KFK_ERR_SEND_FAILED;
        }
    }
}

kafka_err Kafka::requestAndResponse(string &message, string &out_msg) {
    string sessionid, msg_payload, resp_sessionid;
    stringstream sserr;

    get_session(message, sessionid);

    if (sessionid.empty()) {
        this->_loger->error("待发送的信息没有sessionid，发送失败");
        out_msg = "{'code': -1, 'err': 'sessionid异常', 'sessionid': none, 'data': none}";
        return KFK_ERR_SEND_FAILED;
    }
    if (!(this->producer_send(message))) {
        this->_loger->error("消息发送失败");
        out_msg = "{'code': -2, 'err': '消息发送失败', 'sessionid':'" + sessionid + "', 'data': none}";
        return KFK_ERR_SEND_FAILED;
    }

    for (int c_count=0; c_count < this->_max_retry; c_count++) {
        while (true) {
            if (!(this->consumer_get(msg_payload))) {
                out_msg = "{'code': -3, 'err': '接收反馈失败', 'sessionid':'" + sessionid + "', 'data': none}";
                return KFK_ERR_CONSUM_FAILED;
            }else if (msg_payload.empty()) {
                sserr << "获取超时，尝试第" << c_count + 1 << "次再次获取...";
                this->_loger->error(sserr.str().c_str());
                break;
            }else{
                get_session(msg_payload, resp_sessionid);
                if (sessionid == resp_sessionid) {
                    out_msg = msg_payload;
                    return KFK_ERR_NOERROR;
                }
            }
        }
    }
    out_msg = "{'code': -4, 'err': '接收反馈超时失败', 'sessionid':'" + sessionid + "', 'data': none}";
    return KFK_ERR_CONSUM_FAILED;
    //获取反馈信息
//    RdKafka::Message *_msg = nullptr;
//    string payload;
//    for (int c_count=0; c_count < this->_max_retry; c_count++) {
//        _msg = this->_consumer->consume(this->_topic_consumer, NULL, this->_timeout);
//        switch (_msg->err()){
//            case RdKafka::ERR__TIMED_OUT:
//            case RdKafka::ERR__PARTITION_EOF:
//                sserr << "获取超时，尝试第" << c_count+1 << "次再次获取...";
//                this->_loger->error(sserr.str().c_str());
//                break;
//            case RdKafka::ERR_NO_ERROR:
//                //正确获取
//                payload = (char *)_msg->payload();
//                //通过sessionid判断是否是对应的回复
//                j_root = cJSON_Parse((char *)_msg->payload());
//                session = cJSON_GetObjectItem(j_root, "sessionid");
//                if (session) {
//                    if (!strcmp(session->valuestring, sessionid.c_str())) {
//                        out_msg = payload;
//                        return KFK_ERR_NOERROR;
//                    }
//                }
//                break;
//            default:
//                sserr << "其他错误：" << _msg->errstr();
//                this->_loger->error(sserr.str().c_str());
//                out_msg = "{'code': -3, 'err': '接收反馈失败', 'sessionid':'" + sessionid + "', 'data': none}";
//                return KFK_ERR_CONSUM_FAILED;
//        }
//        this->_consumer->poll(0);
//        delete _msg;
//    }
//    out_msg = "{'code': -4, 'err': '接收反馈超时失败', 'sessionid':'" + sessionid + "', 'data': none}";
}

kafka_err Kafka::send_always(string &message) {
    if(this->producer_send(message))
        return KFK_ERR_NOERROR;
    else
        return KFK_ERR_SEND_FAILED;
}

kafka_err Kafka::get_always(string &out_msg) {
    if(this->consumer_get(out_msg))
        return KFK_ERR_NOERROR;
    else
        return KFK_ERR_CONSUM_FAILED;
}

kafka_err Kafka::send_msg(string &topic, string &message) {
    kafka_err rt;
    rt = this->start(topic);
    if(rt == KFK_ERR_NOERROR) {
        if(this->producer_send(message)){
            this->stop();
            return KFK_ERR_NOERROR;
        }else{
            this->stop();
            return KFK_ERR_SEND_FAILED;
        }
    }
    return rt;
}

kafka_err Kafka::get_msg(string &topic, string &consumer_group, string &out_msg) {
    kafka_err rt;
    rt = this->start(topic, consumer_group);
    if(rt == KFK_ERR_NOERROR){
        if(this->consumer_get(out_msg)){
            this->stop();
            return KFK_ERR_NOERROR;
        }else{
            this->stop();
            return KFK_ERR_CONSUM_FAILED;
        }
    }
    return rt;
}