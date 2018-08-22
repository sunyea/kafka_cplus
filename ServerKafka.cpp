//
// Created by liaop on 18-8-21.
//

#include <cstring>
#include "ServerKafka.h"
#include "json/cJSON.h"

ServerKafka::ServerKafka(string &hosts):Kafka(hosts) {
    //
}

void ServerKafka::command(string &message, string &out_msg) {
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
            cJSON_AddStringToObject(sen_root, "err", "[c++]成功");
            rep_data = cJSON_CreateObjectReference(data->child);
            cJSON_AddItemToObject(sen_root, "data", rep_data);
            out_msg = cJSON_Print(sen_root);
            cJSON_Delete(sen_root);
        }else{
            //返回错误信息
            string err_txt = "[c++]未知命令：";
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