cmake_minimum_required(VERSION 3.10)
project(SimpleServer)

set(CMAKE_CXX_STANDARD 11)

add_executable(SimpleServer main.cpp json/cJSON.c json/cJSON.h loger/Loger.cpp loger/Loger.h kafka/Kafka.cpp kafka/Kafka.h ServerKafka.cpp ServerKafka.h)

target_link_libraries(SimpleServer /usr/local/lib/libjsoncpp.a /usr/local/lib/librdkafka++.so /usr/local/lib/liblog4cplus.so)