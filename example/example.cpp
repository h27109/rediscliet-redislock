#include "RedisClient.hpp"
#include "common/time_utility.h"
#include "unistd.h"
#include "common/app_conf.h"
#include "log/log.h"
#include "log/scribe_log.h"
#include "mexception.h"
//#include "pebble_common.h"
#include "boost/shared_ptr.hpp"
#include <string>
#include "redislock.hpp"
/*
using namespace lspf::log;
//初始化日志服务
static bool initLog()
{
    try{
        AppConf *conf = AppConf::Instance();

        lspf::log::ScribeLog::SetHostAndPort(conf->GetConfStr("Log", "ServiceIp"),
                                             conf->GetConfInt("Log", "ServicePort"));

        lspf::log::ScribeLog::SetLogCategory(conf->GetConfStr("Log", "LogCategory"));

        lspf::log::ScribeLog::SetErrorCategory(conf->GetConfStr("Log", "ErrLogCategory"));

        lspf::log::Log::SetLogPriority(conf->GetConfStr("Log", "Priority"));


        //TLogFunc tlogfunc = boost::bind(&lspf::log::ScribeLog::Write, _1, _2, _3, _4, _5, _6);

        //lspf::log::Log::RegisterTlogFunc(tlogfunc, NULL);

        ScribeLog::Start();


    }catch(MException &mx){
        PLOG_ERROR("call initLog Exception...:%s",mx.what());
        return false;
    }

    return true;
}

*/
void func(boost::shared_ptr<CRedisClient> rediscli, int index)
{
    PLOG_DEBUG("thread start:%d\n", index);
    boost::this_thread::sleep_for(boost::chrono::milliseconds(1000+index*100));
    
    while(true)
    {
        CRedisLock lock(rediscli, "mylock");
        if(lock.Lock(4000))
        {
            PLOG_DEBUG("Get Lock OK, thread ID:%u", pthread_self());
            boost::this_thread::sleep_for(boost::chrono::milliseconds(500));
            PLOG_DEBUG("Del Lock OK, thread ID:%u", pthread_self());
            lock.UnLock();
            boost::this_thread::sleep_for(boost::chrono::milliseconds(500));
        }
        else
        {
            PLOG_DEBUG("Get Lock NOK, thread ID:%u", pthread_self());
            boost::this_thread::sleep_for(boost::chrono::milliseconds(500));
        }
    }
}

int main()
{
    //initLog();

    //lspf::log::Log::EnableCrashRecord();
    //std::string strINI("lspf_redis.ini");
    //AppConf::Instance()->LoadConf(strINI);

    /*long xxxx = 1234567890123456789;
    char szTmp[30];
    sprintf(szTmp, "%lu", xxxx);
    printf("%s\n", szTmp);

    string aaa = "1234567890123456789";
    long yyy = atol(aaa.c_str());
    printf("%lu\n", yyy);
    return 0;*/


    boost::shared_ptr<CRedisClient> redisCli = boost::shared_ptr<CRedisClient>(new CRedisClient());
    string server = "172.20.12.1";
    int port1 = 7000;
    int port2 = 7001;
    int port3 = 7002;
    int port4 = 7003;
    int port5 = 7004;
    int port6 = 7005;
    vector<pair<string, int>>servervec;
    servervec.push_back(pair<string, int>(server, port1));
    servervec.push_back(pair<string, int>(server, port2));
    servervec.push_back(pair<string, int>(server, port3));
    servervec.push_back(pair<string, int>(server, port4));
    servervec.push_back(pair<string, int>(server, port5));
    servervec.push_back(pair<string, int>(server, port6));

	if(!redisCli->Initialize(servervec, 2, 10, true))
    {
        return 0;
    }
    for(int i = 0; i < 10; i++)
    {
        boost::thread thid = boost::thread(boost::bind(func, redisCli, i));
        thid.detach();
    }
    while(true)
    {}
    return 0;

    int i = 500;

restart:
    TimerClock clock;

    int j = i + 10000;
    for (; i < j; i++)
    {
        std::string strKey("key_" + boost::to_string(i));
        if (int iret = redisCli->Del(strKey) != RC_SUCCESS)
        {
            
            std::cout << "del key:" << strKey << "failed, error=" << iret << std::endl;
            break;
        }
        std::string strVal("value_abcdefghigklmnopqrstuvwxyz_abcdefghigklmnopqrstuvwxyz_abcdefghigklmnopqrstuvwxyz_abcdefghigklmnopqrstuvwxyz_abcdefghigklmnopqrstuvwxyz_abcdefghigklmnopqrstuvwxyz_abcdefghigklmnopqrstuvwxyz_abcdefghigklmnopqrstuvwxyz_abcdefghigklmnopqrstuvwxyz_" + boost::to_string(i));
        if (redisCli->Set(strKey, strVal) != RC_SUCCESS)
        {
            std::cout << "set key: " << strKey << " failed" << std::endl;
            break;
        }
        if (redisCli->Get(strKey, &strVal) != RC_SUCCESS)
        {
            std::cout << "get key: " << strKey << ": vaule: " << strVal << " failed " << std::endl;
            break;
        }
    }
    std::cout << "time elapsed:  " << clock.Elapsed() << std::endl;

    std::string xx;
    while (std::cin >> xx && xx != "q")
    {
        if (xx == "restart")
        {
            goto restart;
        }
        else
        {
            std::cout << "unknow command" << std::endl;
        }
    }
    std::cout << "quit" << std::endl;
}