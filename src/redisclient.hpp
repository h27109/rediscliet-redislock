#ifndef REDIS_CLIENT_H
#define REDIS_CLIENT_H

#include "hiredis.h"
#include <string>
#include <vector>
#include <list>
#include <map>
#include <set>
#include <queue>
#include <sstream>
#include <functional>
#include <thread>
#include <condition_variable>
#include <iostream>
#include <algorithm>
#include <pthread.h>
#include <string.h>
#include "boost/thread.hpp"
#include "boost/function.hpp"
#include "log/log.h"
#include "common/time_utility.h"

using namespace std;

#define RC_RESULT_EOF 5
#define RC_NO_EFFECT 4
#define RC_OBJ_NOT_EXIST 3
#define RC_OBJ_EXIST 2
#define RC_PART_SUCCESS 1
#define RC_SUCCESS 0
#define RC_PARAM_ERR -1
#define RC_REPLY_ERR -2
#define RC_RQST_ERR -3
#define RC_NO_RESOURCE -4
#define RC_PIPELINE_ERR -5
#define RC_NOT_SUPPORT -6
#define RC_SLOT_CHANGED -100

#define RQST_RETRY_TIMES 3
#define WAIT_RETRY_TIMES 60

typedef boost::function<int(redisReply *)> TFuncFetch;
typedef boost::function<int(int, redisReply *)> TFuncConvert;

int FUNC_DEF_CONV(int nRet, redisReply *reply);


class CRedisServer;
struct SlotRegion
{
    int nStartSlot;
    int nEndSlot;

    vector<pair<string, int>> HostVec;
    vector<CRedisServer *>RedisServVec;
};

class CRedisCommand
{
  public:
    CRedisCommand(const string &strCmd, bool bDump = true);
    virtual ~CRedisCommand() { ClearArgs(); }
    void ClearArgs();
    void DumpArgs() const;
    void DumpReply() const;

    int GetSlot() const { return m_nSlot; }
    const redisReply *GetReply() const { return m_pReply; }
    string FetchErrMsg() const;
    bool IsMovedErr() const;

    void SetSlot(int nSlot) { m_nSlot = nSlot; }
    void SetConvFunc(TFuncConvert funcConv) { m_funcConv = funcConv; }

    void SetArgs();
    void SetArgs(const string &strArg);
    void SetArgs(const vector<string> &vecArg);
    void SetArgs(const string &strArg1, const string &strArg2);
    void SetArgs(const string &strArg1, const vector<string> &vecArg2);
    void SetArgs(const string &strArg1, const set<string> &setArg2);
    void SetArgs(const vector<string> &vecArg1, const string &strArg2);
    void SetArgs(const vector<string> &vecArg1, const vector<string> &vecArg2);
    void SetArgs(const map<string, string> &mapArg);
    void SetArgs(const string &strArg1, const map<string, string> &mapArg2);
    void SetArgs(const string &strArg1, const string &strArg2, const string &strArg3);
    void SetArgs(const string &strArg1, const string &strArg2, const vector<string> &vecArg2);
    void SetArgs(const string &strArg1, const vector<string> &vecArg2, const vector<string> &vecArg3);
    void SetArgs(const string &strArg1, const string &strArg2, const string &strArg3, const string &strArg4);

    int CmdRequest(redisContext *pContext);
    int CmdAppend(redisContext *pContext);
    int CmdReply(redisContext *pContext);
    int FetchResult(const TFuncFetch &funcFetch);

  private:
    void Init();
    void AppendValue(const string &strVal);

  protected:
    string m_strCmd;
    vector<string> m_Args;
    redisReply *m_pReply;
    int m_nSlot;
    TFuncConvert m_funcConv;
    bool m_bDump;
};

class CRedisServer;
class CRedisConnection
{
  public:
    CRedisConnection(string host, int nport);
    ~CRedisConnection();
    bool IsValid() { return m_pContext != NULL; }
    int ConnRequest(CRedisCommand *pRedisCmd);
    int ConnRequest(vector<CRedisCommand *> &vecRedisCmd);
    bool Reconnect();

 private:
    bool ConnectToRedis();

  private:
    redisContext *m_pContext;
    string m_strHost;
    int m_nPort;
    int m_nCliTimeout;
};

class CRedisServer
{
    friend class CRedisConnection;
    friend class CRedisClient;

  public:
    CRedisServer(const string &strHost, int nPort, int nTimeout, int nConnNum);
    virtual ~CRedisServer();

    bool IsMaster() const { return m_bMaster; }
    void SetMaster(bool bMaster) { m_bMaster = bMaster;}

    void SetCluster(bool bCluster) { m_bCluster = bCluster; }

    string GetHost() const { return m_strHost; }
    int GetPort() const { return m_nPort; }

    // for the blocking request
    int ServRequest(CRedisCommand *pRedisCmd);

    // for the pipeline requirement
    int ServRequest(vector<CRedisCommand *> &vecRedisCmd);

    bool Initialize();

    bool LoadClusterSlots();

    void threadFun();

    bool LoadSlaveInfo();

    void CheckConnection();

  private:
    CRedisConnection *FetchConnection();
    void ReturnConnection(CRedisConnection *pRedisConn);
    void CleanConn();

  private:
    string m_strHost;
    int m_nPort;
    int m_nCliTimeout;
    int m_nConnNum;

    bool m_bExit;
    //server是否主用节点
    bool m_bMaster;

    //空闲连接（因为同步调用hiredis没在断连通知机制，所以这个queue里面的连接有可能状态不正常）
    queue<CRedisConnection *> m_queIdleConn;
    mutex m_mutexConn;
    //断开了连接（如果命令执行失败，则放到这个list)
    list<CRedisConnection *> m_listBadConn;
    mutex m_mutexBadConn;
    
    bool m_bValid;
    bool m_bCluster;

    //定时检查连接状况
    boost::thread *m_pThread;
};

class CRedisClient;
class CRedisPipeline
{
    friend class CRedisClient;

  public:
    ~CRedisPipeline();

  private:
    CRedisPipeline() : m_nCursor(0) {}

    void QueueCommand(CRedisCommand *pRedisCmd);
    int FlushCommand(CRedisClient *pRedisCli);
    int FetchNext(TFuncFetch funcFetch);
    int FetchNext(redisReply **pReply);

  private:
    map<CRedisServer *, vector<CRedisCommand *>> m_mapCmd;
    vector<CRedisCommand *> m_vecCmd;
    size_t m_nCursor;
};

typedef void *Pipeline;
class CRedisClient
{
    friend class CRedisPipeline;

  public:
    CRedisClient();
    ~CRedisClient();

    bool Initialize(vector<pair<string, int>>& hostVec, int nTimeout, int nConnNum, bool bIfCluster);
    bool LoadClusterSlots();

    void threadFun();

    bool IsCluster() { return m_bCluster; }

    Pipeline CreatePipeline();
    int FlushPipeline(Pipeline ppLine);
    int FetchReply(Pipeline ppLine, long *pnVal);
    int FetchReply(Pipeline ppLine, string *pstrVal);
    int FetchReply(Pipeline ppLine, vector<long> *pvecLongVal);
    int FetchReply(Pipeline ppLine, vector<string> *pvecStrVal);
    int FetchReply(Pipeline ppLine, redisReply **pReply);
    void FreePipeline(Pipeline ppLine);

    /* interfaces for generic */
    int Del(const string &strKey, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Dump(const string &strKey, string *pstrVal, Pipeline ppLine = NULL);
    int Exists(const string &strKey, long *pnVal, Pipeline ppLine = NULL);
    int Expire(const string &strKey, long nSec, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Expireat(const string &strKey, long nTime, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Keys(const string &strPattern, vector<string> *pvecVal);
    int Persist(const string &strKey, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Pexpire(const string &strKey, long nMilliSec, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Pexpireat(const string &strKey, long nMilliTime, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Pttl(const string &strKey, long *pnVal, Pipeline ppLine = NULL);
    int Randomkey(string *pstrVal, Pipeline ppLine = NULL);
    int Rename(const string &strKey, const string &strNewKey);
    int Renamenx(const string &strKey, const string &strNewKey);
    int Restore(const string &strKey, long nTtl, const string &strVal, Pipeline ppLine = NULL);
    int Scan(long *pnCursor, const string &strPattern, long nCount, vector<string> *pvecVal);
    int Ttl(const string &strKey, long *pnVal, Pipeline ppLine = NULL);
    int Type(const string &strKey, string *pstrVal, Pipeline ppLine = NULL);

    /* interfaces for string */
    int Append(const string &strKey, const string &strVal, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Bitcount(const string &strKey, long *pnVal, Pipeline ppLine = NULL);
    int Bitcount(const string &strKey, long nStart, long nEnd, long *pnVal, Pipeline ppLine = NULL);
    int Bitop(const string &strDestKey, const string &strOp, const vector<string> &vecKey, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Bitpos(const string &strKey, long nBitVal, long *pnVal, Pipeline ppLine = NULL);
    int Bitpos(const string &strKey, long nBitVal, long nStart, long nEnd, long *pnVal, Pipeline ppLine = NULL);
    int Decr(const string &strKey, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Decrby(const string &strKey, long nDecr, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Get(const string &strKey, string *pstrVal, Pipeline ppLine = NULL);
    int Getbit(const string &strKey, long nOffset, long *pnVal, Pipeline ppLine = NULL);
    int Getrange(const string &strKey, long nStart, long nEnd, string *pstrVal, Pipeline ppLine = NULL);
    int Getset(const string &strKey, string *pstrVal, Pipeline ppLine = NULL);
    int Incr(const string &strKey, long *pnVal, Pipeline ppLine = NULL);
    int Incrby(const string &strKey, long nIncr, long *pnVal, Pipeline ppLine = NULL);
    int Incrbyfloat(const string &strKey, double dIncr, double *pdVal, Pipeline ppLine = NULL);
    int Mget(const vector<string> &vecKey, vector<string> *pvecVal);
    int Mset(const vector<string> &vecKey, const vector<string> &vecVal);
    int Psetex(const string &strKey, long nMilliSec, const string &strVal, Pipeline ppLine = NULL);
    int Set(const string &strKey, const string &strVal, Pipeline ppLine = NULL);
    int Setbit(const string &strKey, long nOffset, bool bVal, Pipeline ppLine = NULL);
    int Setex(const string &strKey, long nSec, const string &strVal, Pipeline ppLine = NULL);
    int Setnx(const string &strKey, const string &strVal, Pipeline ppLine = NULL);
    int Setrange(const string &strKey, long nOffset, const string &strVal, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Strlen(const string &strKey, long *pnVal, Pipeline ppLine = NULL);

    /* interfaces for list */
    int Blpop(const string &strKey, long nTimeout, vector<string> *pvecVal);
    int Blpop(const vector<string> &vecKey, long nTimeout, vector<string> *pvecVal);
    int Brpop(const string &strKey, long nTimeout, vector<string> *pvecVal);
    int Brpop(const vector<string> &vecKey, long nTimeout, vector<string> *pvecVal);
    int Lindex(const string &strKey, long nIndex, string *pstrVal, Pipeline ppLine = NULL);
    int Linsert(const string &strKey, const string &strPos, const string &strPivot, const string &strVal, long *pnVal, Pipeline ppLine = NULL);
    int Llen(const string &strKey, long *pnVal, Pipeline ppLine = NULL);
    int Lpop(const string &strKey, string *pstrVal, Pipeline ppLine = NULL);
    int Lpush(const string &strKey, const string &strVal, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Lpush(const string &strKey, const vector<string> &vecVal, Pipeline ppLine = NULL);
    int Lpushx(const string &strKey, const string &strVal, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Lrange(const string &strKey, long nStart, long nStop, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Lrem(const string &strKey, long nCount, const string &strVal, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Lset(const string &strKey, long nIndex, const string &strVal, Pipeline ppLine = NULL);
    int Ltrim(const string &strKey, long nStart, long nStop, Pipeline ppLine = NULL);
    int Rpop(const string &strKey, string *pstrVal, Pipeline ppLine = NULL);
    int Rpush(const string &strKey, const string &strVal, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Rpush(const string &strKey, const vector<string> &vecVal, Pipeline ppLine = NULL);
    int Rpushx(const string &strKey, const string &strVal, long *pnVal = NULL, Pipeline ppLine = NULL);

    /* interfaces for set */
    int Sadd(const string &strKey, const string &strVal, long *pnVal = NULL, Pipeline = NULL);
    int Scard(const string &strKey, long *pnVal, Pipeline = NULL);
    //int Sdiff(const vector<string> &vecKey, vector<string> *pvecVal, Pipeline ppLine = NULL);
    //int Sinter(const vector<string> &vecKey, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Sismember(const string &strKey, const string &strVal, long *pnVal, Pipeline ppLine = NULL);
    int Smembers(const string &strKey, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Spop(const string &strKey, string *pstrVal, Pipeline ppLine = NULL);
    int Srandmember(const string &strKey, long nCount, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Srem(const string &strKey, const string &strVal, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Srem(const string &strKey, const vector<string> &vecVal, long *pnVal = NULL, Pipeline ppLine = NULL);
    //int Sunion(const vector<string> &vecKey, vector<string> *pvecVal, Pipeline ppLine = NULL);

    /* interfaces for hash */
    int Hdel(const string &strKey, const string &strField, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Hexists(const string &strKey, const string &strField, long *pnVal, Pipeline ppLine = NULL);
    int Hget(const string &strKey, const string &strField, string *pstrVal, Pipeline ppLine = NULL);
    int Hgetall(const string &strKey, map<string, string> *pmapFv, Pipeline ppLine = NULL);
    int Hincrby(const string &strKey, const string &strField, long nIncr, long *pnVal, Pipeline ppLine = NULL);
    int Hincrbyfloat(const string &strKey, const string &strField, double dIncr, double *pdVal, Pipeline ppLine = NULL);
    int Hkeys(const string &strKey, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Hlen(const string &strKey, long *pnVal, Pipeline ppLine = NULL);
    int Hmget(const string &strKey, const vector<string> &vecField, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Hmget(const string &strKey, const vector<string> &vecField, map<string, string> *pmapVal);
    int Hmget(const string &strKey, const set<string> &setField, map<string, string> *pmapVal);
    int Hmset(const string &strKey, const vector<string> &vecField, const vector<string> &vecVal, Pipeline ppLine = NULL);
    int Hmset(const string &strKey, const map<string, string> &mapFv, Pipeline ppLine = NULL);
    //int Hscan(const string &strKey, long *pnCursor, const string &strMatch, long nCount, vector<string> *pvecVal);
    int Hset(const string &strKey, const string &strField, const string &strVal, Pipeline ppLine = NULL);
    int Hsetnx(const string &strKey, const string &strField, const string &strVal, Pipeline ppLine = NULL);
    int Hvals(const string &strKey, vector<string> *pvecVal, Pipeline ppLine = NULL);

    /* interfaces for sorted set */
    int Zadd(const string &strKey, double dScore, const string &strElem, long *pnVal = NULL, Pipeline = NULL);
    int Zcard(const string &strKey, long *pnVal, Pipeline = NULL);
    int Zcount(const string &strKey, double dMin, double dMax, long *pnVal, Pipeline ppLine = NULL);
    int Zincrby(const string &strKey, double dIncr, const string &strElem, double *pdVal, Pipeline ppLine = NULL);
    int Zlexcount(const string &strKey, const string &strMin, const string &strMax, long *pnVal, Pipeline ppLine = NULL);
    int Zrange(const string &strKey, long nStart, long nStop, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Zrangewithscore(const string &strKey, long nStart, long nStop, map<string, string> *pmapVal, Pipeline ppLine = NULL);
    int Zrangebylex(const string &strKey, const string &strMin, const string &strMax, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Zrangebyscore(const string &strKey, double dMin, double dMax, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Zrangebyscore(const string &strKey, double dMin, double dMax, map<string, double> *pmapVal, Pipeline ppLine = NULL);
    int Zrank(const string &strKey, const string &strElem, long *pnVal, Pipeline ppLine = NULL);
    int Zrem(const string &strKey, const string &strElem, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Zrem(const string &strKey, const vector<string> &vecElem, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Zremrangebylex(const string &strKey, const string &strMin, const string &strMax, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Zremrangebyrank(const string &strKey, long nStart, long nStop, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Zremrangebyscore(const string &strKey, double dMin, double dMax, long *pnVal = NULL, Pipeline ppLine = NULL);
    int Zrevrange(const string &strKey, long nStart, long nStop, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Zrevrangebyscore(const string &strKey, double dMax, double dMin, vector<string> *pvecVal, Pipeline ppLine = NULL);
    int Zrevrangebyscore(const string &strKey, double dMax, double dMin, map<string, double> *pmapVal, Pipeline ppLine = NULL);
    int Zrevrank(const string &strKey, const string &strElem, long *pnVal, Pipeline ppLine = NULL);
    int Zscore(const string &strKey, const string &strElem, double *pdVal, Pipeline ppLine = NULL);

    /* interfaces for system */
    int Time(struct timeval *ptmVal, Pipeline ppLine = NULL);


    static bool ConvertToMapInfo(const string &strVal, map<string, string> &mapVal);
    static bool GetValue(redisReply *pReply, string &strVal);
    static bool GetArray(redisReply *pReply, vector<string> &vecVal);

  private:
    void CleanServer();
    
    bool InSameNode(const string &strKey1, const string &strKey2);
    CRedisServer *GetMatchedServer(const CRedisCommand *pRedisCmd) const;
    CRedisServer *FindServer(const string &strHost, int nPort);
    CRedisServer *FindServer(int nSlot) const;
    //bool WaitForRefresh();
    int Execute(CRedisCommand *pRedisCmd, Pipeline ppLine = NULL);
    int SimpleExecute(CRedisCommand *pRedisCmd);

    int ExecuteImpl(const string &strCmd, int nSlot, Pipeline ppLine, TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV);

    template <typename P>
    int ExecuteImpl(const string &strCmd, const P &tArg, int nSlot, Pipeline ppLine, TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV);

    template <typename P1, typename P2>
    int ExecuteImpl(const string &strCmd, const P1 &tArg1, const P2 &tArg2, int nSlot, Pipeline ppLine, TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV);

    template <typename P1, typename P2, typename P3>
    int ExecuteImpl(const string &strCmd, const P1 &tArg1, const P2 &tArg2, const P3 &tArg3, int nSlot, Pipeline ppLine, TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV);

    template <typename P1, typename P2, typename P3, typename P4>
    int ExecuteImpl(const string &strCmd, const P1 &tArg1, const P2 &tArg2, const P3 &tArg3, const P4 &tArg4, int nSlot, Pipeline ppLine, TFuncFetch funcFetch, TFuncConvert funcConv = FUNC_DEF_CONV);

  private:
    string m_strHost;
    int m_nPort;
    vector<pair<string, int>> m_HostVec;
    
    int m_nTimeout;
    int m_nConnNum;

    bool m_bCluster;
    bool m_bExit;

    vector<SlotRegion> m_vecSlot;
    vector<CRedisServer *> m_vecRedisServ;

    boost::thread * m_pThread;
};

#endif
