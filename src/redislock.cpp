#include "redislock.hpp"
bool CRedisLock::Lock(int64_t timeout)
{
    while (timeout >= 0) 
    {
        int64_t current = TimeUtility::GetCurremtMs();
        string myValue = GenerateValue(current);

        int iRet = m_redisclient->Setnx(m_name, myValue);
        //如果setnx成功，获得锁
        if(RC_SUCCESS == iRet)
        {
            m_myValue = current;
            m_bLocked = true;
            PLOG_DEBUG("Get Lock OK(setnx success), threadid=%u", pthread_self());
            return true;
        }
        //如果节点已经存在，需要判断是否锁已经超时
        else if(iRet == RC_OBJ_EXIST)
        {
            string redisValue;
            int iRet1 = m_redisclient->Get(m_name, &redisValue);
            if(RC_SUCCESS == iRet1)//如果超过有效期，则解锁
            {
                if(redisValue.size() != 0 && IsExpired(redisValue))
                {
                    string tmpmyValue = myValue;
                    m_redisclient->Getset(m_name, &tmpmyValue);//尝试去设置timesstamp，并且获取原来的timestamp
                    if(tmpmyValue == redisValue)
                    {
                        m_myValue = current;
                        m_bLocked = true;
                        PLOG_DEBUG("Get Lock OK(del expired lock), threadid=%u", pthread_self());
                        return true;
                    }
                }
            }
        }
        int sleeptime = rand()%100;
        timeout -= sleeptime;
        boost::this_thread::sleep_for(boost::chrono::milliseconds(sleeptime));
    }
    return false;
}

void CRedisLock::UnLock()//不能轻易的del key，因为有可能锁已经被抢了
{
    PLOG_DEBUG("Del Lock start, threadid=%u", pthread_self());
    if(m_bLocked)
    {
        if(m_myValue != -1 && TimeUtility::GetCurremtMs() - m_myValue < m_maxexpire)
        {
            PLOG_DEBUG("Del Lock OK, threadid=%u", pthread_self());
            m_redisclient->Del(m_name);
        }
        m_bLocked = false;
        m_myValue = -1;
    }
}
