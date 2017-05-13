#ifndef __REDIS_LOCK_H__
#define __REDIS_LOCK_H__

#include "redisclient.hpp"

class CRedisLock
{
public:
    CRedisLock(boost::shared_ptr<CRedisClient>client, string name, int64_t maxexpire = 10000) : m_redisclient(client), m_name(name), 
                                                                                            m_maxexpire(maxexpire), m_bLocked(false),
                                                                                            m_myValue(-1)
    {
    }

    //根据当前毫秒生成key，key的规则为“随机数+毫秒“
    string GenerateValue(int64_t ms)
    {
        char szTmp[30];
        sprintf(szTmp, "%lu", ms);
        return string(szTmp);
    }

    bool IsExpired(string strvalue)
    {
        int64_t ms = atol(strvalue.c_str());
        int64_t currentms = TimeUtility::GetCurremtMs();
        if(currentms - ms >= m_maxexpire)
        {
            PLOG_DEBUG("Lock expired, current ms=%lu, redisms=%lu, threadid=%u", currentms, ms, pthread_self());
            return true;
        }
        return false;
    }

    ~CRedisLock(){}

    bool Lock(int64_t timeout = 2000);

    void UnLock();
private:
    CRedisLock():m_redisclient(), m_name(""), m_maxexpire(10000){}

private:
    const boost::shared_ptr<CRedisClient> m_redisclient;
    const string m_name;
    const int64_t m_maxexpire;

    bool m_bLocked;
    int64_t m_myValue;//加锁的时候设置的时间戳，在大并发的时候，这个值有可能被其他线程/进程更改
    
};


#endif

