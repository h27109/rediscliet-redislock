#include "redisclient.hpp"

#define BIND_INT(val) boost::bind(&FetchInteger, _1, val)
#define BIND_STR(val) boost::bind(&FetchString, _1, val)
#define BIND_VINT(val) boost::bind(&FetchIntegerArray, _1, val)
#define BIND_VSTR(val) boost::bind(&FetchStringArray, _1, val)
#define BIND_MAP(val) boost::bind(&FetchMap, _1, val)
#define BIND_TIME(val) boost::bind(&FetchTime, _1, val)
#define BIND_SLOT(val) boost::bind(&FetchSlot, _1, val)

// crc16 for computing redis cluster slot
static const uint16_t crc16Table[256] =
    {
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
        0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
        0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
        0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
        0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
        0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
        0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
        0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
        0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
        0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
        0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
        0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
        0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
        0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
        0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
        0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
        0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
        0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
        0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
        0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
        0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
        0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
        0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
        0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
        0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
        0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
        0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
        0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
        0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
        0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
        0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0};

uint16_t CRC16(const char *pszData, int nLen)
{
    int nCounter;
    uint16_t nCrc = 0;
    for (nCounter = 0; nCounter < nLen; nCounter++)
    {
        nCrc = (nCrc << 8) ^ crc16Table[((nCrc >> 8) ^ *pszData++) & 0x00FF];
    }
    return nCrc;
}

uint32_t HASH_SLOT(const string &strKey)
{
    const char *pszKey = strKey.data();
    size_t nKeyLen = strKey.size();
    size_t nStart, nEnd; /* start-end indexes of { and  } */

    /* Search the first occurrence of '{'. */
    for (nStart = 0; nStart < nKeyLen; nStart++)
    {
        if (pszKey[nStart] == '{')
        {
            break;
        }
    }

    /* No '{' ? Hash the whole key. This is the base case. */
    if (nStart == nKeyLen)
    {
        return CRC16(pszKey, nKeyLen) & 16383;
    }

    /* '{' found? Check if we have the corresponding '}'. */
    for (nEnd = nStart + 1; nEnd < nKeyLen; nEnd++)
    {
        if (pszKey[nEnd] == '}')
        {
            break;
        }
    }

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (nEnd == nKeyLen || nEnd == nStart + 1)
    {
        return CRC16(pszKey, nKeyLen) & 16383;
    }

    /* If we are here there is both a { and a  } on its right. Hash
     * what is in the middle between { and  }. */
    return CRC16(pszKey + nStart + 1, nEnd - nStart - 1) & 16383;
}

int FUNC_DEF_CONV(int nRet, redisReply *reply)
{
    return nRet;
}

static inline ostream &operator<<(ostream &os, const pair<int, redisReply *> &pairReply)
{
    int nLevel = pairReply.first;
    redisReply *pReply = pairReply.second;

    for (int i = 0; i < nLevel; ++i)
    {
        os << "  ";
    }

    if (!pReply)
    {
        os << "NULL" << endl;
    }
    else if (pReply->type == REDIS_REPLY_INTEGER)
    {
        os << "integer: " << pReply->integer << endl;
    }
    else if (pReply->type == REDIS_REPLY_STRING)
    {
        os << "string: " << pReply->str << endl;
    }
    else if (pReply->type == REDIS_REPLY_ARRAY)
    {
        os << "array: " << endl;
        for (unsigned int j = 0; j < pReply->elements; ++j)
        {
            os << make_pair(nLevel + 1, pReply->element[j]);
        }
    }
    else if (pReply->type == REDIS_REPLY_STATUS)
    {
        os << "status: " << pReply->str << endl;
    }
    else if (pReply->type == REDIS_REPLY_ERROR)
    {
        os << "error: " << pReply->str << endl;
    }
    else if (pReply->type == REDIS_REPLY_NIL)
    {
        os << "nil" << endl;
    }
    else
    {
        os << "unknown" << endl;
    }
    return os;
}

static inline ostream &operator<<(ostream &os, redisReply *pReply)
{
    return os << make_pair(0, pReply);
}

static string GetReplySting(const pair<int, redisReply *> &pairReply)
{
    int nLevel = pairReply.first;
    redisReply *pReply = pairReply.second;
    string out;

    for (int i = 0; i < nLevel; ++i)
    {
        out += "  ";
    }

    if (!pReply)
    {
        out += "NULL\n";
    }
    else if (pReply->type == REDIS_REPLY_INTEGER)
    {
        out += "integer: ";
        out += boost::to_string(pReply->integer);
        out += "\n";
    }
    else if (pReply->type == REDIS_REPLY_STRING)
    {
        out += "string: ";
        out += pReply->str;
        out += "\n";
    }
    else if (pReply->type == REDIS_REPLY_ARRAY)
    {
        out += "array: \n";
        for (unsigned int j = 0; j < pReply->elements; ++j)
        {
            out += GetReplySting(make_pair(nLevel + 1, pReply->element[j]));
        }
    }
    else if (pReply->type == REDIS_REPLY_STATUS)
    {
        out += "status: ";
        out += pReply->str;
        out += "\n";
    }
    else if (pReply->type == REDIS_REPLY_ERROR)
    {
        out += "error: ";
        out += pReply->str;
        out += "\n";
    }
    else if (pReply->type == REDIS_REPLY_NIL)
    {
        out += "nil\n";
    }
    else
    {
        out += "unknown\n";
    }
    return out;
}


template <typename T>
string ConvertToString(T t)
{
    stringstream sstream;
    sstream << t;
    return sstream.str();
}

// for finding matched slot/server with binary search
bool operator<(const SlotRegion &lReg, const SlotRegion &rReg)
{
    return lReg.nStartSlot < rReg.nStartSlot;
}

class CompSlot
{
  public:
    bool operator()(const SlotRegion &slotReg, int nSlot) { return slotReg.nEndSlot < nSlot; }
    bool operator()(int nSlot, const SlotRegion &slotReg) { return nSlot < slotReg.nStartSlot; }
};

class IntResConv
{
  public:
    IntResConv(int nConvRet = RC_OBJ_NOT_EXIST, long nVal = 0) : m_nConvRet(nConvRet), m_nVal(nVal) {}
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_SUCCESS && pReply->integer == m_nVal)
        {
            return m_nConvRet;
        }
        return nRet;
    }

  private:
    int m_nConvRet;
    long m_nVal;
};

class StuResConv
{
  public:
    StuResConv() : m_strVal("OK") {}
    StuResConv(const string &strVal) : m_strVal(strVal) {}
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_SUCCESS && m_strVal != pReply->str)
        {
            return RC_REPLY_ERR;
        }
        return nRet;
    }

  private:
    string m_strVal;
};

class NilResConv
{
  public:
    NilResConv();
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_SUCCESS && pReply->type == REDIS_REPLY_NIL)
        {
            return RC_OBJ_NOT_EXIST;
        }
        return nRet;
    }
};

class ExistErrConv
{
  public:
    ExistErrConv() : m_strErr("Target key name is busy") {}
    int operator()(int nRet, redisReply *pReply)
    {
        if (nRet == RC_REPLY_ERR && m_strErr == pReply->str)
        {
            return RC_OBJ_EXIST;
        }
        return nRet;
    }

  private:
    string m_strErr;
};

static inline int FetchInteger(redisReply *pReply, long *pnVal)
{
    if (pReply->type == REDIS_REPLY_INTEGER)
    {
        if (pnVal)
        {
            *pnVal = pReply->integer;
        }
        return RC_SUCCESS;
    }
    else if (pReply->type == REDIS_REPLY_NIL)
    {
        return RC_OBJ_NOT_EXIST;
    }
    else
    {
        return RC_REPLY_ERR;
    }
}

static inline int FetchString(redisReply *pReply, string *pstrVal)
{
    if (pReply->type == REDIS_REPLY_STRING || pReply->type == REDIS_REPLY_STATUS)
    {
        if (pstrVal)
        {
            pstrVal->assign(pReply->str, pReply->len);
        }
        return RC_SUCCESS;
    }
    else if (pReply->type == REDIS_REPLY_NIL)
    {
        if (pstrVal)
        {
            pstrVal->clear();
        }
        return RC_SUCCESS;
    }
    else
    {
        return RC_REPLY_ERR;
    }
}

static inline int FetchIntegerArray(redisReply *pReply, vector<long> *pvecLongVal)
{
    if (pReply->type == REDIS_REPLY_INTEGER)
    {
        int nRet = RC_SUCCESS;
        if (!pvecLongVal)
        {
            return nRet;
        }

        long nVal;
        pvecLongVal->clear();
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            int nSubRet = FetchInteger(pReply->element[i], &nVal);
            if (nSubRet == RC_SUCCESS)
            {
                pvecLongVal->push_back(nVal);
            }
            else
            {
                nRet = RC_PART_SUCCESS;
            }
        }
        return nRet;
    }
    else
    {
        return RC_REPLY_ERR;
    }
}

static inline int FetchStringArray(redisReply *pReply, vector<string> *pvecStrVal)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        int nRet = RC_SUCCESS;
        if (!pvecStrVal)
        {
            return nRet;
        }

        string strVal;
        pvecStrVal->clear();
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            int nSubRet = FetchString(pReply->element[i], &strVal);
            if (nSubRet == RC_SUCCESS)
            {
                pvecStrVal->push_back(strVal);
            }
            else
            {
                nRet = RC_PART_SUCCESS;
            }
        }
        return nRet;
    }
    else if (pReply->type == REDIS_REPLY_NIL)
    {
        if (pvecStrVal)
        {
            pvecStrVal->clear();
        }
        return RC_SUCCESS;
    }
    else
    {
        return RC_REPLY_ERR;
    }
}

static inline int FetchMap(redisReply *pReply, map<string, string> *pmapFv)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        int nRet = RC_SUCCESS;
        if (!pmapFv)
        {
            return nRet;
        }

        if ((pReply->elements % 2) != 0)
        {
            return RC_REPLY_ERR;
        }

        string strFld;
        string strVal;
        pmapFv->clear();
        for (size_t i = 0; i < pReply->elements;)
        {
            int nSubRet = FetchString(pReply->element[i++], &strFld);
            if (nSubRet == RC_SUCCESS)
            {
                nSubRet = FetchString(pReply->element[i++], &strVal);
            }

            if (nSubRet == RC_SUCCESS)
            {
                pmapFv->insert(make_pair(strFld, strVal));
            }
            else
            {
                return nSubRet;
            }
        }
        return nRet;
    }
    else
    {
        return RC_REPLY_ERR;
    }
}

static inline int FetchTime(redisReply *pReply, struct timeval *ptmVal)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        if (pReply->elements != 2 || pReply->element[0]->type != REDIS_REPLY_STRING || pReply->element[1]->type != REDIS_REPLY_STRING)
        {
            return RC_REPLY_ERR;
        }

        if (ptmVal)
        {
            ptmVal->tv_sec = atol(pReply->element[0]->str);
            ptmVal->tv_usec = atol(pReply->element[1]->str);
        }
        return RC_SUCCESS;
    }
    else
    {
        return RC_REPLY_ERR;
    }
}

static inline int FetchSlot(redisReply *pReply, vector<SlotRegion> *pvecSlot)
{
    if (pReply->type == REDIS_REPLY_ARRAY)
    {
        if (!pvecSlot)
        {
            return RC_SUCCESS;
        }

        pvecSlot->clear();
        for (size_t i = 0; i < pReply->elements; ++i)
        {
            SlotRegion slotReg;
            redisReply *pSubReply = pReply->element[i];
            if (pSubReply->type != REDIS_REPLY_ARRAY || pSubReply->elements < 3)
            {
                return RC_REPLY_ERR;
            }
            
            slotReg.nStartSlot = pSubReply->element[0]->integer;
            slotReg.nEndSlot = pSubReply->element[1]->integer;
            for(size_t j = 2; j < pSubReply->elements; ++j)
            {
                redisReply *pSubSubReply = pSubReply->element[j];
                if (pSubSubReply->type != REDIS_REPLY_ARRAY || pSubSubReply->elements < 3)
                {
                    break;
                }
                string ip = pSubSubReply->element[0]->str;
                int port = pSubSubReply->element[1]->integer;
                slotReg.HostVec.push_back(pair<string, int>(ip, port));                
            }
            slotReg.RedisServVec.clear();
            pvecSlot->push_back(slotReg);
        }
        return RC_SUCCESS;
    }
    else
    {
        return RC_REPLY_ERR;
    }
}

// CRedisCommand methods
CRedisCommand::CRedisCommand(const string &strCmd, bool ifDump)
    : m_strCmd(strCmd), m_pReply(NULL), m_nSlot(-1), m_funcConv(FUNC_DEF_CONV), m_bDump(ifDump)
{
    
}

void CRedisCommand::ClearArgs()
{
    if (m_pReply)
    {
        freeReplyObject(m_pReply);
    }
    m_pReply = NULL;
    m_Args.clear();
}

string CRedisCommand::FetchErrMsg() const
{
    string strErrMsg;
    if (m_pReply && m_pReply->type == REDIS_REPLY_ERROR)
    {
        strErrMsg.assign(m_pReply->str, m_pReply->len);
    }
    return strErrMsg;
}

bool CRedisCommand::IsMovedErr() const
{
    return FetchErrMsg().substr(0, 5) == "MOVED";
}

void CRedisCommand::DumpArgs() const
{
    if(!m_bDump)
    {
        return;
    }
    
    PLOG_DEBUG("start to excute command, args size=%d",m_Args.size());
    for (size_t i = 0; i < m_Args.size(); ++i)
    {
        PLOG_DEBUG("Arg(%d)=%s", (i + 1), m_Args[i].c_str());
    }
}

void CRedisCommand::DumpReply() const
{
    if(!m_bDump)
    {
        return;
    }

    if (!m_pReply)
    {
        PLOG_DEBUG("Dump Reply, result=no reply");
    }
    else
    {
        string log = "Dump Reply, result=" + GetReplySting(pair<int, redisReply *>(0, m_pReply));
        PLOG_DEBUG(log.c_str());
    }
}

void CRedisCommand::Init()
{
    ClearArgs();
    m_Args.push_back(m_strCmd);
}

void CRedisCommand::AppendValue(const string &strVal)
{
    m_Args.push_back(strVal);
}

//根据参数个数不同，设置参数到类
void CRedisCommand::SetArgs()
{
    Init();
}

void CRedisCommand::SetArgs(const string &strArg)
{
    Init();
    AppendValue(strArg);
}

void CRedisCommand::SetArgs(const vector<string> &vecArg)
{
    Init();
    for (size_t i = 0; i < vecArg.size(); i++)
    {
        AppendValue(vecArg[i]);
    }
}

void CRedisCommand::SetArgs(const string &strArg1, const string &strArg2)
{
    Init();
    AppendValue(strArg1);
    AppendValue(strArg2);
}

void CRedisCommand::SetArgs(const string &strArg1, const vector<string> &vecArg2)
{
    Init();
    AppendValue(strArg1);
    for (size_t i = 0; i < vecArg2.size(); i++)
    {
        AppendValue(vecArg2[i]);
    }
}

void CRedisCommand::SetArgs(const string &strArg1, const set<string> &setArg2)
{
    Init();
    AppendValue(strArg1);
    set<string>::const_iterator it = setArg2.begin();
    while (it != setArg2.end())
    {
        AppendValue(*it);
        it++;
    }
}

void CRedisCommand::SetArgs(const vector<string> &vecArg1, const string &strArg2)
{
    if (vecArg1.empty())
        return;

    Init();
    for (size_t i = 0; i < vecArg1.size(); i++)
    {
        AppendValue(vecArg1[i]);
    }
    AppendValue(strArg2);
}

void CRedisCommand::SetArgs(const vector<string> &vecArg1, const vector<string> &vecArg2)
{
    if (vecArg1.size() != vecArg2.size())
    {
        return;
    }

    Init();
    for (size_t i = 0; i < vecArg1.size(); ++i)
    {
        AppendValue(vecArg1[i]);
        AppendValue(vecArg2[i]);
    }
}

void CRedisCommand::SetArgs(const map<string, string> &mapArg)
{
    Init();
    map<string, string>::const_iterator it = mapArg.begin();
    while (it != mapArg.end())
    {
        AppendValue(it->first);
        AppendValue(it->second);
        it++;
    }
}

void CRedisCommand::SetArgs(const string &strArg1, const map<string, string> &mapArg2)
{
    Init();
    AppendValue(strArg1);
    map<string, string>::const_iterator it = mapArg2.begin();
    while (it != mapArg2.end())
    {
        AppendValue(it->first);
        AppendValue(it->second);
        it++;
    }
}

void CRedisCommand::SetArgs(const string &strArg1, const string &strArg2, const string &strArg3)
{
    Init();
    AppendValue(strArg1);
    AppendValue(strArg2);
    AppendValue(strArg3);
}

void CRedisCommand::SetArgs(const string &strArg1, const string &strArg2, const vector<string> &vecArg3)
{
    Init();
    AppendValue(strArg1);
    AppendValue(strArg2);
    for (size_t i = 0; i < vecArg3.size(); i++)
    {
        AppendValue(vecArg3[i]);
    }
}

void CRedisCommand::SetArgs(const string &strArg1, const vector<string> &vecArg2, const vector<string> &vecArg3)
{
    Init();
    AppendValue(strArg1);
    for (size_t i = 0; i < vecArg2.size(); ++i)
    {
        AppendValue(vecArg2[i]);
        AppendValue(vecArg3[i]);
    }
}

void CRedisCommand::SetArgs(const string &strArg1, const string &strArg2, const string &strArg3, const string &strArg4)
{
    Init();
    AppendValue(strArg1);
    AppendValue(strArg2);
    AppendValue(strArg3);
    AppendValue(strArg4);
}

int CRedisCommand::CmdRequest(redisContext *pContext)
{
    if (m_Args.size() <= 0)
    {
        return RC_PARAM_ERR;
    }

    if (!pContext)
    {
        return RC_RQST_ERR;
    }

    if (m_pReply)
    {
        freeReplyObject(m_pReply);
        m_pReply = NULL;
    }

    char **pArgs = new char *[m_Args.size()];
    size_t *pArgsLen = new size_t[m_Args.size()];
    
    for (size_t i = 0; i < m_Args.size(); i++)
    {
        pArgs[i] = (char *)m_Args[i].c_str();
        *(pArgsLen+i) =  m_Args[i].size();
    }

    m_pReply = (redisReply *)(redisCommandArgv(pContext, m_Args.size(), (const char **)pArgs, pArgsLen));
    delete[] pArgs;
    delete[] pArgsLen;

    return m_pReply ? RC_SUCCESS : RC_RQST_ERR;
}

int CRedisCommand::CmdAppend(redisContext *pContext)
{
    if (m_Args.size() <= 0)
    {
        return RC_PARAM_ERR;
    }

    if (!pContext)
    {
        return RC_RQST_ERR;
    }

    char **pArgs = new char *[m_Args.size()];
    size_t *pArgsLen = new size_t[m_Args.size()];
    
    for (size_t i = 0; i < m_Args.size(); i++)
    {
        pArgs[i] = (char *)m_Args[i].c_str();
        *(pArgsLen+i) =  m_Args[i].size();
    }

    int nRet = redisAppendCommandArgv(pContext, m_Args.size(), (const char **)pArgs, pArgsLen);

    delete[] pArgs;
    delete[] pArgsLen;
    return nRet == REDIS_OK ? RC_SUCCESS : RC_RQST_ERR;
}

int CRedisCommand::CmdReply(redisContext *pContext)
{
    if (m_pReply)
    {
        freeReplyObject(m_pReply);
        m_pReply = NULL;
    }

    return redisGetReply(pContext, (void **)&m_pReply) == REDIS_OK ? RC_SUCCESS : RC_RQST_ERR;
}

int CRedisCommand::FetchResult(const TFuncFetch &funcFetch)
{
    return m_funcConv(funcFetch(m_pReply), m_pReply);
}

// CRedisConnection methods
CRedisConnection::CRedisConnection(string host, int nport) : m_pContext(NULL), m_strHost(host), m_nPort(nport)
{
}

CRedisConnection::~CRedisConnection()
{
    if (m_pContext)
    {
        redisFree(m_pContext);
    }
}

int CRedisConnection::ConnRequest(CRedisCommand *pRedisCmd)
{
    pRedisCmd->DumpArgs();
    if (!m_pContext)
    {
        if (!Reconnect())
        {
            return RC_RQST_ERR;
        }
    }

    int nRet = pRedisCmd->CmdRequest(m_pContext);
    if (nRet == RC_RQST_ERR)
    {
        if (!Reconnect())
        {
            return RC_RQST_ERR;
        }
        else
        {
            nRet = pRedisCmd->CmdRequest(m_pContext);
        }
    }

    pRedisCmd->DumpReply();
    return nRet;
}

int CRedisConnection::ConnRequest(vector<CRedisCommand *> &vecRedisCmd)
{
    if (!m_pContext)
    {
        if (!Reconnect())
        {
            return RC_RQST_ERR;
        }
    }

    int nRet = RC_SUCCESS;
    for (size_t i = 0; i < vecRedisCmd.size() && nRet == RC_SUCCESS; ++i)
    {
        nRet = vecRedisCmd[i]->CmdAppend(m_pContext);
    }
    for (size_t i = 0; i < vecRedisCmd.size() && nRet == RC_SUCCESS; ++i)
    {
        nRet = vecRedisCmd[i]->CmdReply(m_pContext);
    }
    return nRet;
}

bool CRedisConnection::ConnectToRedis()
{
    if (m_pContext)
    {
        redisFree(m_pContext);
    }

    struct timeval tmTimeout = {m_nCliTimeout, 0};
    m_pContext = redisConnectWithTimeout(m_strHost.c_str(), m_nPort, tmTimeout);
    if (!m_pContext || m_pContext->err)
    {
        if(m_pContext)
        {
            PLOG_INFO("Connection error=%s, ip=%s, port=%d", m_pContext->errstr, m_strHost.c_str(), m_nPort);
            redisFree(m_pContext);
            m_pContext = NULL;
        }
        else
        {
            PLOG_INFO("Connection error=can't allocate redis context, ip=%s, port=%d", m_strHost.c_str(), m_nPort);
        }
        return false;
    }

    redisSetTimeout(m_pContext, tmTimeout);
    PLOG_INFO("Connection OK, ip=%s, port=%d", m_strHost.c_str(), m_nPort);
    return true;
}

bool CRedisConnection::Reconnect()
{
    return ConnectToRedis();
}

// CRedisServer methods
CRedisServer::CRedisServer(const string &strHost, int nPort, int nTimeout, int nConnNum)
    : m_strHost(strHost), m_nPort(nPort), m_nCliTimeout(nTimeout), m_nConnNum(nConnNum)
{
}

CRedisServer::~CRedisServer()
{
    CleanConn();
    m_bExit = true;
    if (m_pThread)
    {
        //m_pThread->join();
        delete m_pThread;
    }
}

void CRedisServer::CleanConn()
{
    m_mutexConn.lock();
    while (!m_queIdleConn.empty())
    {
        delete m_queIdleConn.front();
        m_queIdleConn.pop();
    }
    m_mutexConn.unlock();

    m_mutexBadConn.lock();
    while (!m_listBadConn.empty())
    {
        delete m_listBadConn.front();
        m_listBadConn.pop_front();
    }
    m_mutexBadConn.unlock();
}

CRedisConnection *CRedisServer::FetchConnection()
{
    CRedisConnection *pRedisConn = NULL;
    m_mutexConn.lock();
    if (!m_queIdleConn.empty())
    {
        pRedisConn = m_queIdleConn.front();
        m_queIdleConn.pop();
    }
    m_mutexConn.unlock();
    return pRedisConn;
}

void CRedisServer::ReturnConnection(CRedisConnection *pRedisConn)
{
    if(pRedisConn->IsValid())
    {
        m_mutexConn.lock();
        m_queIdleConn.push(pRedisConn);
        m_mutexConn.unlock();
    }
    else
    {
        m_mutexBadConn.lock();
        m_listBadConn.push_back(pRedisConn);
        m_mutexBadConn.unlock();
    }
    
}

bool CRedisServer::Initialize()
{
    m_bExit = false;
    CleanConn();

    for (int i = 0; i < m_nConnNum; ++i)
    {
        CRedisConnection *pRedisConn = new CRedisConnection(m_strHost, m_nPort);
        if(pRedisConn->Reconnect())
        {
            m_mutexConn.lock();
            m_queIdleConn.push(pRedisConn);
            m_mutexConn.unlock();
        }
        else
        {
            m_mutexBadConn.lock();
            m_listBadConn.push_back(pRedisConn);
            m_mutexBadConn.unlock();
        }  
    }
    m_pThread = new boost::thread(boost::bind(&CRedisServer::threadFun, this));
    m_pThread->detach();
    return true;
}

void CRedisServer::threadFun()
{
    while (!m_bExit)
    {
        if(m_bCluster)
        {
            LoadSlaveInfo();
        }
        CheckConnection();
        boost::this_thread::sleep_for(boost::chrono::seconds(2));
    }
}

void CRedisServer::CheckConnection()
{
    CRedisConnection *pRedisConn = NULL;
    vector<CRedisConnection* >tmpVec;
    m_mutexBadConn.lock();
    list<CRedisConnection *>::iterator it = m_listBadConn.begin();
    while (it != m_listBadConn.end())
    {          
        pRedisConn = *it;
        if(pRedisConn->Reconnect())
        {
            list<CRedisConnection *>::iterator it1 = it;
            it++;
            m_listBadConn.erase(it1);

            tmpVec.push_back(pRedisConn);
        }
        else
        {
            it++;
        }    
    }
    m_mutexBadConn.unlock();
    m_mutexConn.lock();
    for(size_t i = 0; i < tmpVec.size(); i++)
    {
        m_queIdleConn.push(pRedisConn); 
    }
    m_mutexConn.unlock();
    
}
int CRedisServer::ServRequest(CRedisCommand *pRedisCmd)
{
    CRedisConnection *pRedisConn = NULL;
    int nTry = RQST_RETRY_TIMES;
    while (nTry--)
    {
        if ((pRedisConn = FetchConnection()))
        {
            break;
        }
        boost::this_thread::sleep_for(boost::chrono::milliseconds(5));
    }

    if (!pRedisConn)
    {
        return RC_NO_RESOURCE;
    }

    int nRet = pRedisConn->ConnRequest(pRedisCmd);
    ReturnConnection(pRedisConn);
    return nRet;
}

int CRedisServer::ServRequest(vector<CRedisCommand *> &vecRedisCmd)
{
    CRedisConnection *pRedisConn = NULL;
    int nTry = RQST_RETRY_TIMES;
    while (nTry--)
    {
        pRedisConn = FetchConnection();
        if ((pRedisConn != NULL &&  pRedisConn->IsValid()))
        {
            break;
        }
        boost::this_thread::sleep_for(boost::chrono::milliseconds(5));
    }

    if (!pRedisConn)
    {
        return RC_NO_RESOURCE;
    }

    int nRet = pRedisConn->ConnRequest(vecRedisCmd);
    ReturnConnection(pRedisConn);
    return nRet;
}

bool CRedisServer::LoadSlaveInfo()
{
    string strInfo;
    map<string, string> mapInfo;
    CRedisCommand redisCmd("info", false);
    redisCmd.SetArgs("Replication");

    if (ServRequest(&redisCmd) != RC_SUCCESS || redisCmd.FetchResult(BIND_STR(&strInfo)) != RC_SUCCESS || !CRedisClient::ConvertToMapInfo(strInfo, mapInfo))
    {
        return false;
    }
    map<string, string>::const_iterator it = mapInfo.find("role");
    if(it == mapInfo.end())
    {
        m_bMaster = true;
    }
    else
    {
        if(it->second == "master\r")
        {
           m_bMaster = true;
        }
        else
        {
            m_bMaster = false;
        }
        
    }
    return true;
}

// CRedisPipeline methods
CRedisPipeline::~CRedisPipeline()
{
    for (size_t i = 0; i < m_vecCmd.size(); i++)
    {
        delete m_vecCmd[i];
    }
}

void CRedisPipeline::QueueCommand(CRedisCommand *pRedisCmd)
{
    m_vecCmd.push_back(pRedisCmd);
}

int CRedisPipeline::FlushCommand(CRedisClient *pRedisCli)
{
    m_mapCmd.clear();
    for (size_t i = 0; i < m_vecCmd.size(); i++)
    {
        CRedisCommand *pRedisCmd = m_vecCmd[i];
        CRedisServer *pRedisServ = pRedisCli->GetMatchedServer(pRedisCmd);
        //auto it = m_mapCmd.find(pRedisServ);
        map<CRedisServer *, vector<CRedisCommand *>>::iterator it = m_mapCmd.find(pRedisServ);
        if (it != m_mapCmd.end())
        {
            it->second.push_back(pRedisCmd);
        }
        else
        {
            vector<CRedisCommand *> vecCmd;
            vecCmd.push_back(pRedisCmd);
            m_mapCmd.insert(make_pair(pRedisServ, vecCmd));
        }
    }

    int nRet = RC_SUCCESS;
    for (map<CRedisServer *, vector<CRedisCommand *>>::iterator it = m_mapCmd.begin(); it != m_mapCmd.end() && nRet == RC_SUCCESS; ++it)
    {
        nRet = it->first->ServRequest(it->second);
    }
    return nRet;
}

int CRedisPipeline::FetchNext(TFuncFetch funcFetch)
{
    if (m_nCursor >= m_vecCmd.size())
    {
        return RC_RESULT_EOF;
    }
    else
    {
        return m_vecCmd[m_nCursor++]->FetchResult(funcFetch);
    }
}

int CRedisPipeline::FetchNext(redisReply **pReply)
{
    if (m_nCursor >= m_vecCmd.size())
    {
        return RC_RESULT_EOF;
    }
    else
    {
        *pReply = (redisReply *)m_vecCmd[m_nCursor++]->GetReply();
        return RC_SUCCESS;
    }
}

// CRedisClient methods
CRedisClient::CRedisClient() : m_nPort(-1), m_nTimeout(-1), m_nConnNum(-1),  m_bCluster(false), m_bExit(false)
{

}

CRedisClient::~CRedisClient()
{
    m_bExit = true;

    CleanServer();
    if(m_pThread)
    {
        delete m_pThread;
    }
}

//method of credisclient
bool CRedisClient::Initialize(vector<pair<string, int>>& hostVec, int nTimeout, int nConnNum, bool bIfCluster)
{
    m_nTimeout = nTimeout;
    m_nConnNum = nConnNum;

    m_bCluster = bIfCluster;

    if (m_nTimeout <= 0 || m_nConnNum <= 0)
    {
        PLOG_INFO("CRedisClient Initialize failed, Timeout(%d) or ConnectionNum(%d) invalid!", m_nTimeout, m_nConnNum);
        return false;
    }

    for(size_t i = 0; i < hostVec.size(); i++)
    {
        string ip = hostVec[i].first;
        int port = hostVec[i].second;
        PLOG_INFO("Add redis server ip=%s, port=%d", ip.c_str(), port);
        if(ip.size() == 0 || port <= 0)
        {
            printf("Host Info invallid, ip=%s, port=%d\n", ip.c_str(), port);
            continue;
        }
        CRedisServer *pRedisServ = new CRedisServer(ip, port, m_nTimeout, m_nConnNum);
        pRedisServ->Initialize();
        //非cluster，只建一个server
        if(!m_bCluster)
        {
            pRedisServ->SetCluster(false);
            m_vecRedisServ.push_back(pRedisServ);
            return true;
        }
        pRedisServ->SetCluster(true);
        m_vecRedisServ.push_back(pRedisServ);
        
        for(size_t i = 0; i < m_vecRedisServ.size(); i++)
        {
            m_vecRedisServ[i]->LoadSlaveInfo();
        }
    }
    m_pThread = new boost::thread(boost::bind(&CRedisClient::threadFun, this));
    m_pThread->detach();  
    return true;
}

int CRedisClient::ExecuteImpl(const string &strCmd, int nSlot, Pipeline ppLine, TFuncFetch funcFetch, TFuncConvert funcConv)
{
    //CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
    CRedisCommand *pRedisCmd = new CRedisCommand(strCmd);
    pRedisCmd->SetArgs();
    pRedisCmd->SetSlot(nSlot);
    pRedisCmd->SetConvFunc(funcConv);
    int nRet = Execute(pRedisCmd, ppLine);
    if (nRet == RC_SUCCESS && !ppLine)
    {
        nRet = pRedisCmd->FetchResult(funcFetch);
    }
    if (!ppLine)
    {
        delete pRedisCmd;
    }
    return nRet;
}

template <typename P>
int CRedisClient::ExecuteImpl(const string &strCmd, const P &tArg, int nSlot, Pipeline ppLine,
                TFuncFetch funcFetch, TFuncConvert funcConv)
{
    //CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
    CRedisCommand *pRedisCmd = new CRedisCommand(strCmd);
    pRedisCmd->SetArgs(tArg);
    pRedisCmd->SetSlot(nSlot);
    pRedisCmd->SetConvFunc(funcConv);
    int nRet = Execute(pRedisCmd, ppLine);
    if (nRet == RC_SUCCESS && !ppLine)
    {
        nRet = pRedisCmd->FetchResult(funcFetch);
    }
    if (!ppLine)
    {
        delete pRedisCmd;
    }
    return nRet;
}

template <typename P1, typename P2>
int CRedisClient::ExecuteImpl(const string &strCmd, const P1 &tArg1, const P2 &tArg2, int nSlot, Pipeline ppLine,
                TFuncFetch funcFetch, TFuncConvert funcConv)
{
    //CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
    CRedisCommand *pRedisCmd = new CRedisCommand(strCmd);
    pRedisCmd->SetArgs(tArg1, tArg2);
    pRedisCmd->SetSlot(nSlot);
    pRedisCmd->SetConvFunc(funcConv);
    int nRet = Execute(pRedisCmd, ppLine);
    if (nRet == RC_SUCCESS && !ppLine)
    {
        nRet = pRedisCmd->FetchResult(funcFetch);
    }
    if (!ppLine)
    {
        delete pRedisCmd;
    }
    return nRet;
}

template <typename P1, typename P2, typename P3>
int CRedisClient::ExecuteImpl(const string &strCmd, const P1 &tArg1, const P2 &tArg2, const P3 &tArg3, int nSlot, Pipeline ppLine,
                TFuncFetch funcFetch, TFuncConvert funcConv)
{
    //CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
    CRedisCommand *pRedisCmd = new CRedisCommand(strCmd);
    pRedisCmd->SetArgs(tArg1, tArg2, tArg3);
    pRedisCmd->SetSlot(nSlot);
    pRedisCmd->SetConvFunc(funcConv);
    int nRet = Execute(pRedisCmd, ppLine);
    if (nRet == RC_SUCCESS && !ppLine)
    {
        nRet = pRedisCmd->FetchResult(funcFetch);
    }
    if (!ppLine)
    {
        delete pRedisCmd;
    }
    return nRet;
}

template <typename P1, typename P2, typename P3, typename P4>
int CRedisClient::ExecuteImpl(const string &strCmd, const P1 &tArg1, const P2 &tArg2, const P3 &tArg3, const P4 &tArg4, int nSlot, Pipeline ppLine,
                TFuncFetch funcFetch, TFuncConvert funcConv)
{
    //CRedisCommand *pRedisCmd = new CRedisCommand(strCmd, !ppLine);
    CRedisCommand *pRedisCmd = new CRedisCommand(strCmd);
    pRedisCmd->SetArgs(tArg1, tArg2, tArg3, tArg4);
    pRedisCmd->SetSlot(nSlot);
    pRedisCmd->SetConvFunc(funcConv);
    int nRet = Execute(pRedisCmd, ppLine);
    if (nRet == RC_SUCCESS && !ppLine)
    {
        nRet = pRedisCmd->FetchResult(funcFetch);
    }
    if (!ppLine)
    {
        delete pRedisCmd;
    }
    return nRet;
}

/* interfaces for generic */
int CRedisClient::Del(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("del", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Dump(const string &strKey, string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("dump", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Exists(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("exists", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Expire(const string &strKey, long nSec, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("expire", strKey, ConvertToString(nSec), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Expireat(const string &strKey, long nTime, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("expireat", strKey, ConvertToString(nTime), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Keys(const string &strPattern, vector<string> *pvecVal)
{
    CRedisCommand redisCmd("keys");
    redisCmd.SetArgs(strPattern);

    int nRet = RC_SUCCESS;
    vector<string> vecVal;
    //for (auto pRedisServ : m_vecRedisServ)
    for (size_t i = 0; i < m_vecRedisServ.size(); i++)
    {
        CRedisServer *pRedisServ = m_vecRedisServ[i];
        if ((nRet = pRedisServ->ServRequest(&redisCmd)) != RC_SUCCESS ||
            (nRet = redisCmd.FetchResult(BIND_VSTR(&vecVal))) != RC_SUCCESS)
        {
            if (pvecVal)
            {
                pvecVal->clear();
            }
            return nRet;
        }
        else
        {
            if (pvecVal)
            {
                copy(vecVal.begin(), vecVal.end(), back_inserter(*pvecVal));
            }
        }
    }
    return nRet;
}

int CRedisClient::Persist(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("persist", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Pexpire(const string &strKey, long nMilliSec, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("pexpire", strKey, ConvertToString(nMilliSec), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Pexpireat(const string &strKey, long nMilliTime, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("pexpireat", strKey, ConvertToString(nMilliTime), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Pttl(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("pttl", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Randomkey(string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("randomkey", -1, ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Rename(const string &strKey, const string &strNewKey)
{
    if (InSameNode(strKey, strNewKey))
    {
        return ExecuteImpl("rename", strKey, strNewKey, HASH_SLOT(strKey), NULL, BIND_STR((string *)0), StuResConv());
    }
    else
    {
        string strVal;
        long nTtl;
        int nRet;
        if ((nRet = Dump(strKey, &strVal)) == RC_SUCCESS && !strVal.empty() &&
            (nRet = Pttl(strKey, &nTtl)) == RC_SUCCESS && nTtl != -2)
        {
            if (nTtl == -1)
            {
                nTtl = 0;
            }
            if ((nRet = Del(strNewKey)) >= RC_SUCCESS &&
                (nRet = Restore(strNewKey, nTtl, strVal)) == RC_SUCCESS &&
                (nRet = Del(strKey)) >= RC_SUCCESS)
            {
                return RC_SUCCESS;
            }
        }

        Del(strNewKey);
        return nRet == RC_SUCCESS ? RC_REPLY_ERR : nRet;
    }
}

int CRedisClient::Renamenx(const string &strKey, const string &strNewKey)
{
    if (InSameNode(strKey, strNewKey))
    {
        return ExecuteImpl("renamenx", strKey, strNewKey, HASH_SLOT(strKey), NULL, BIND_STR((string *)0), StuResConv());
    }
    else
    {
        int nRet;
        long nVal;
        if ((nRet = Exists(strNewKey, &nVal)) != RC_SUCCESS)
            return nRet;
        else if (nVal == 1)
            return RC_REPLY_ERR;

        string strVal;
        long nTtl;
        if ((nRet = Dump(strKey, &strVal)) == RC_SUCCESS &&
            (nRet = Pttl(strKey, &nTtl)) == RC_SUCCESS)
        {
            if (nTtl == -1)
                nTtl = 0;
            if ((nRet = Restore(strNewKey, nTtl, strVal)) == RC_SUCCESS &&
                (nRet = Del(strKey)) >= RC_SUCCESS)
                return RC_SUCCESS;
        }

        Del(strNewKey);
        return nRet == RC_SUCCESS ? RC_REPLY_ERR : nRet;
    }
}

int CRedisClient::Restore(const string &strKey, long nTtl, const string &strVal, Pipeline ppLine)
{
    return ExecuteImpl("restore", strKey, ConvertToString(nTtl), strVal, HASH_SLOT(strKey), ppLine, BIND_STR((string *)0), StuResConv());
}

int CRedisClient::Scan(long *pnCursor, const string &strPattern, long nCount, vector<string> *pvecVal)
{
    return RC_SUCCESS;
}

int CRedisClient::Ttl(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("ttl", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Type(const string &strKey, string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("type", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

/* interfaces for string */
int CRedisClient::Append(const string &strKey, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("append", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Bitcount(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("bitcount", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Bitcount(const string &strKey, long nStart, long nEnd, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("bitcount", strKey, ConvertToString(nStart), ConvertToString(nEnd), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Bitop(const string &strDestKey, const string &strOp, const vector<string> &vecKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("bitop", strOp, strDestKey, vecKey, HASH_SLOT(strDestKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Bitpos(const string &strKey, long nBitVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("bitpos", strKey, ConvertToString(nBitVal), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Bitpos(const string &strKey, long nBitVal, long nStart, long nEnd, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("bitpos", strKey, ConvertToString(nBitVal), ConvertToString(nStart), ConvertToString(nEnd), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Decr(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("decr", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Decrby(const string &strKey, long nDecr, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("decrby", strKey, ConvertToString(nDecr), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Get(const string &strKey, string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("get", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Getbit(const string &strKey, long nOffset, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("getbit", strKey, ConvertToString(nOffset), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Getrange(const string &strKey, long nStart, long nStop, string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("getrange", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Getset(const string &strKey, string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("getset", strKey, *pstrVal, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Incr(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("incr", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Incrby(const string &strKey, long nIncr, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("incrby", strKey, ConvertToString(nIncr), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Incrbyfloat(const string &strKey, double dIncr, double *pdVal, Pipeline ppLine)
{
    string strVal;
    int nRet = ExecuteImpl("incrbyfloat", strKey, ConvertToString(dIncr), HASH_SLOT(strKey), ppLine, BIND_STR(&strVal));
    if (nRet == RC_SUCCESS)
        *pdVal = atof(strVal.c_str());
    return nRet;
}

int CRedisClient::Mget(const vector<string> &vecKey, vector<string> *pvecVal)
{
    if (vecKey.empty())
    {
        return RC_SUCCESS;
    }

    if (!m_bCluster)
    {
        return ExecuteImpl("mget", vecKey, HASH_SLOT(vecKey[0]), NULL, BIND_VSTR(pvecVal));
    }
    else
    {
        Pipeline ppLine = CreatePipeline();
        if (!ppLine)
        {
            return RC_RQST_ERR;
        }

        int nRet = RC_SUCCESS;
        string strVal;
        for (size_t i = 0; i < vecKey.size(); i++)
        {
            string strKey = vecKey[i];
            if ((nRet = Get(strKey, NULL, ppLine)) != RC_SUCCESS)
            {
                break;
            }
        }
        FlushPipeline(ppLine);
        if (nRet == RC_SUCCESS)
        {
            int nSubRet;
            while ((nSubRet = FetchReply(ppLine, &strVal)) != RC_RESULT_EOF)
            {
                if (nSubRet == RC_SUCCESS)
                {
                    pvecVal->push_back(strVal);
                }
                else if (nSubRet == RC_REPLY_ERR)
                    pvecVal->push_back("");
                else
                {
                    nRet = nSubRet;
                    break;
                }
            }
        }
        FreePipeline(ppLine);
        return nRet;
    }
}

int CRedisClient::Mset(const vector<string> &vecKey, const vector<string> &vecVal)
{
    if (vecKey.empty())
        return RC_SUCCESS;

    if (!m_bCluster)
        return ExecuteImpl("mset", vecKey, vecVal, HASH_SLOT(vecKey[0]), NULL, BIND_STR((string *)0), StuResConv());
    else
    {
        Pipeline ppLine = CreatePipeline();
        if (!ppLine)
            return RC_RQST_ERR;

        int nRet = RC_SUCCESS;
        for (size_t i = 0; i < vecKey.size(); ++i)
        {
            if ((nRet = Set(vecKey[i], vecVal[i], ppLine)) != RC_SUCCESS)
                break;
        }
        FlushPipeline(ppLine);
        if (nRet == RC_SUCCESS)
        {
            int nSubRet;
            while ((nSubRet = FetchReply(ppLine, (string *)NULL)) != RC_RESULT_EOF)
            {
                if (nSubRet != RC_SUCCESS)
                    nRet = RC_PART_SUCCESS;
            }
        }
        FreePipeline(ppLine);
        return nRet;
    }
    return 0;
}

int CRedisClient::Psetex(const string &strKey, long nMilliSec, const string &strVal, Pipeline ppLine)
{
    return ExecuteImpl("psetex", strKey, ConvertToString(nMilliSec), strVal, HASH_SLOT(strKey), ppLine, BIND_STR((string *)0), StuResConv());
}

int CRedisClient::Set(const string &strKey, const string &strVal, Pipeline ppLine)
{
    int nRet = ExecuteImpl("set", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_STR((string *)0), StuResConv());
    return nRet;
}

int CRedisClient::Setbit(const string &strKey, long nOffset, bool bVal, Pipeline ppLine)
{
    return ExecuteImpl("setbit", strKey, ConvertToString(nOffset), ConvertToString((long)bVal), HASH_SLOT(strKey), ppLine, BIND_INT((long *)0));
}

int CRedisClient::Setex(const string &strKey, long nSec, const string &strVal, Pipeline ppLine)
{
    return ExecuteImpl("setex", strKey, ConvertToString(nSec), strVal, HASH_SLOT(strKey), ppLine, BIND_STR((string *)0), StuResConv());
}

int CRedisClient::Setnx(const string &strKey, const string &strVal, Pipeline ppLine)
{
    return ExecuteImpl("setnx", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT((long *)NULL), IntResConv(RC_OBJ_EXIST));
}

int CRedisClient::Setrange(const string &strKey, long nOffset, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("setrange", strKey, ConvertToString(nOffset), strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Strlen(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("strlen", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Blpop(const string &strKey, long nTimeout, vector<string> *pvecVal)
{
    return ExecuteImpl("blpop", strKey, ConvertToString(nTimeout), HASH_SLOT(strKey), NULL, BIND_VSTR(pvecVal));
}

int CRedisClient::Blpop(const vector<string> &vecKey, long nTimeout, vector<string> *pvecVal)
{
    if (m_bCluster)
        return RC_NOT_SUPPORT;
    else
        return vecKey.empty() ? RC_PARAM_ERR : ExecuteImpl("blpop", vecKey, ConvertToString(nTimeout), HASH_SLOT(vecKey[0]), NULL, BIND_VSTR(pvecVal));
}

int CRedisClient::Brpop(const string &strKey, long nTimeout, vector<string> *pvecVal)
{
    return ExecuteImpl("brpop", strKey, ConvertToString(nTimeout), HASH_SLOT(strKey), NULL, BIND_VSTR(pvecVal));
}

int CRedisClient::Brpop(const vector<string> &vecKey, long nTimeout, vector<string> *pvecVal)
{
    if (m_bCluster)
        return RC_NOT_SUPPORT;
    else
        return vecKey.empty() ? RC_PARAM_ERR : ExecuteImpl("brpop", vecKey, ConvertToString(nTimeout), HASH_SLOT(vecKey[0]), NULL, BIND_VSTR(pvecVal));
}

int CRedisClient::Lindex(const string &strKey, long nIndex, string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("lindex", strKey, ConvertToString(nIndex), HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Linsert(const string &strKey, const string &strPos, const string &strPivot, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("linsert", strKey, strPos, strPivot, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Llen(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("llen", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Lpop(const string &strKey, string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("lpop", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Lpush(const string &strKey, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("lpush", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

//int CRedisClient::Lpush(const string &strKey, const vector<string> &vecVal, Pipeline ppLine)
//{
//    return ExecuteImpl("lpush", BIND_INT(NULL), strKey, vecVal, HASH_SLOT(strKey), ppLine);
//}

int CRedisClient::Lpushx(const string &strKey, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("lpushx", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal), IntResConv(RC_REPLY_ERR));
}

int CRedisClient::Lrange(const string &strKey, long nStart, long nStop, vector<string> *pvecVal, Pipeline ppLine)
{
    return ExecuteImpl("lrange", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

int CRedisClient::Lrem(const string &strKey, long nCount, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("lrem", strKey, ConvertToString(nCount), strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Lset(const string &strKey, long nIndex, const string &strVal, Pipeline ppLine)
{
    return ExecuteImpl("lset", strKey, ConvertToString(nIndex), strVal, HASH_SLOT(strKey), ppLine, BIND_STR((string *)0), StuResConv());
}

int CRedisClient::Ltrim(const string &strKey, long nStart, long nStop, Pipeline ppLine)
{
    return ExecuteImpl("ltrim", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_STR((string *)0), StuResConv());
}

int CRedisClient::Rpop(const string &strKey, string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("rpop", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Rpush(const string &strKey, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("rpush", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}
//
//int CRedisClient::Rpush(const string &strKey, const vector<string> &vecVal, Pipeline ppLine)
//{
//    return ExecuteImpl("rpush", BIND_INT(NULL), strKey, vecVal, HASH_SLOT(strKey), ppLine);
//}

int CRedisClient::Rpushx(const string &strKey, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("rpushx", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal), IntResConv(RC_REPLY_ERR));
}

/* interfaces for set */
int CRedisClient::Sadd(const string &strKey, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("sadd", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Scard(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("scard", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

//int CRedisClient::Sdiff(const vector<string> &vecKey, vector<string> *pvecVal, Pipeline ppLine = NULL);
//int CRedisClient::Sinter(const vector<string> &vecKey, vector<string> *pvecVal, Pipeline ppLine = NULL);
int CRedisClient::Sismember(const string &strKey, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("sismember", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Smembers(const string &strKey, vector<string> *pvecVal, Pipeline ppLine)
{
    return ExecuteImpl("smembers", strKey, HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

int CRedisClient::Spop(const string &strKey, string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("spop", strKey, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

//int CRedisClient::Srandmember(const string &strKey, long nCount, vector<string> *pvecVal, Pipeline ppLine = NULL);
int CRedisClient::Srem(const string &strKey, const string &strVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("srem", strKey, strVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Srem(const string &strKey, const vector<string> &vecVal, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("srem", strKey, vecVal, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

//int CRedisClient::Sunion(const vector<string> &vecKey, vector<string> *pvecVal, Pipeline ppLine = NULL);

/* interfaces for hash */
int CRedisClient::Hdel(const string &strKey, const string &strField, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("hdel", strKey, strField, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Hexists(const string &strKey, const string &strField, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("hexists", strKey, strField, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Hget(const string &strKey, const string &strField, string *pstrVal, Pipeline ppLine)
{
    return ExecuteImpl("hget", strKey, strField, HASH_SLOT(strKey), ppLine, BIND_STR(pstrVal));
}

int CRedisClient::Hgetall(const string &strKey, map<string, string> *pmapFv, Pipeline ppLine)
{
    return ExecuteImpl("hgetall", strKey, HASH_SLOT(strKey), ppLine, BIND_MAP(pmapFv));
}

int CRedisClient::Hincrby(const string &strKey, const string &strField, long nIncr, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("hincrby", strKey, strField, ConvertToString(nIncr), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Hincrbyfloat(const string &strKey, const string &strField, double dIncr, double *pdVal, Pipeline ppLine)
{
    string strVal;
    int nRet = ExecuteImpl("hincrbyfloat", strKey, strField, ConvertToString(dIncr), HASH_SLOT(strKey), ppLine, BIND_STR(&strVal));
    if (nRet == RC_SUCCESS)
        *pdVal = atof(strVal.c_str());
    return nRet;
}

int CRedisClient::Hkeys(const string &strKey, vector<string> *pvecVal, Pipeline ppLine)
{
    return ExecuteImpl("hkeys", strKey, HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

int CRedisClient::Hlen(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("hlen", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Hmget(const string &strKey, const vector<string> &vecField, vector<string> *pvecVal, Pipeline ppLine)
{
    return ExecuteImpl("hmget", strKey, vecField, HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

int CRedisClient::Hmget(const string &strKey, const vector<string> &vecField, map<string, string> *pmapVal)
{
    vector<string> vecVal;
    int nRet = ExecuteImpl("hmget", strKey, vecField, HASH_SLOT(strKey), NULL, BIND_VSTR(&vecVal));
    if (nRet == RC_SUCCESS)
    {
        if (!vecVal.empty() && vecField.size() != vecVal.size())
            nRet = RC_RQST_ERR;
        else if (pmapVal)
        {
            pmapVal->clear();
            if (!vecVal.empty())
            {
                //auto it1 = vecField.begin();
                //auto it2 = vecVal.begin();
                vector<string>::const_iterator it1 = vecField.begin();
                vector<string>::const_iterator it2 = vecVal.begin();
                while (it1 != vecField.end())
                {
                    if (!((*it2).empty()))
                    {
                        pmapVal->insert(make_pair(*it1, *it2));
                    }
                    ++it1;
                    ++it2;
                }
            }
        }
    }
    return nRet;
}

int CRedisClient::Hmget(const string &strKey, const set<string> &setField, map<string, string> *pmapVal)
{
    vector<string> vecVal;
    int nRet = ExecuteImpl("hmget", strKey, setField, HASH_SLOT(strKey), NULL, BIND_VSTR(&vecVal));
    if (nRet == RC_SUCCESS)
    {
        if (vecVal.size() != setField.size())
            nRet = RC_RQST_ERR;
        else if (pmapVal)
        {
            pmapVal->clear();
            set<string>::const_iterator it1 = setField.begin();
            vector<string>::const_iterator it2 = vecVal.begin();
            while (it1 != setField.end())
            {
                if (!((*it2).empty()))
                {
                    pmapVal->insert(make_pair(*it1, *it2));
                }
                ++it1;
                ++it2;
            }
        }
    }
    return nRet;
}

int CRedisClient::Hmset(const string &strKey, const vector<string> &vecField, const vector<string> &vecVal, Pipeline ppLine)
{
    return ExecuteImpl("hmset", strKey, vecField, vecVal, HASH_SLOT(strKey), ppLine, BIND_STR((string *)0), StuResConv());
}

int CRedisClient::Hmset(const string &strKey, const map<string, string> &mapFv, Pipeline ppLine)
{
    return ExecuteImpl("hmset", strKey, mapFv, HASH_SLOT(strKey), ppLine, BIND_STR((string *)0), StuResConv());
}

int CRedisClient::Hset(const string &strKey, const string &strField, const string &strVal, Pipeline ppLine)
{
    return ExecuteImpl("hset", strKey, strField, strVal, HASH_SLOT(strKey), ppLine, BIND_INT((long *)0));
}

int CRedisClient::Hsetnx(const string &strKey, const string &strField, const string &strVal, Pipeline ppLine)
{
    return ExecuteImpl("hsetnx", strKey, strField, strVal, HASH_SLOT(strKey), ppLine, BIND_INT((long *)0), IntResConv(RC_REPLY_ERR));
}

int CRedisClient::Hvals(const string &strKey, vector<string> *pvecVal, Pipeline ppLine)
{
    return ExecuteImpl("hvals", strKey, HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

/* interfaces for sorted set */
int CRedisClient::Zadd(const string &strKey, double dScore, const string &strElem, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zadd", strKey, ConvertToString(dScore), strElem, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zcard(const string &strKey, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zcard", strKey, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zcount(const string &strKey, double dMin, double dMax, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zcount", strKey, ConvertToString(dMin), ConvertToString(dMax), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zincrby(const string &strKey, double dIncr, const string &strElem, double *pdVal, Pipeline ppLine)
{
    string strVal;
    int nRet = ExecuteImpl("zincrby", strKey, ConvertToString(dIncr), strElem, HASH_SLOT(strKey), ppLine, BIND_STR(&strVal));
    if (nRet == RC_SUCCESS)
        *pdVal = atof(strVal.c_str());
    return nRet;
}

int CRedisClient::Zlexcount(const string &strKey, const string &strMin, const string &strMax, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zlexcount", strKey, strMin, strMax, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zrange(const string &strKey, long nStart, long nStop, vector<string> *pvecVal, Pipeline ppLine)
{
    return ExecuteImpl("zrange", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

int CRedisClient::Zrangewithscore(const string &strKey, long nStart, long nStop, map<string, string> *pmapVal, Pipeline ppLine)
{
    return ExecuteImpl("zrange", strKey, ConvertToString(nStart), ConvertToString(nStop), string("WITHSCORES"), HASH_SLOT(strKey), ppLine, BIND_MAP(pmapVal));
}

int CRedisClient::Zrangebylex(const string &strKey, const string &strMin, const string &strMax, vector<string> *pvecVal, Pipeline ppLine)
{
    return ExecuteImpl("zrangebylex", strKey, strMin, strMax, HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

int CRedisClient::Zrangebyscore(const string &strKey, double dMin, double dMax, vector<string> *pvecVal, Pipeline ppLine)
{
    return ExecuteImpl("zrangebyscore", strKey, ConvertToString(dMin), ConvertToString(dMax), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

int CRedisClient::Zrangebyscore(const string &strKey, double dMin, double dMax, map<string, double> *pmapVal, Pipeline ppLine)
{
    map<string, string> mapVal;
    int nRet = ExecuteImpl("zrangebyscore", strKey, ConvertToString(dMin), ConvertToString(dMax), string("WITHSCORES"), HASH_SLOT(strKey), ppLine, BIND_MAP(&mapVal));
    if (nRet == RC_SUCCESS && pmapVal)
    {
        pmapVal->clear();
        for (map<string, string>::const_iterator it = mapVal.begin(); it != mapVal.end(); it++)
        {
            pmapVal->insert(make_pair(it->first, atof(it->second.c_str())));
        }
    }
    return nRet;
}

int CRedisClient::Zrank(const string &strKey, const string &strElem, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zrank", strKey, strElem, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zrem(const string &strKey, const string &strElem, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zrem", strKey, strElem, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zrem(const string &strKey, const vector<string> &vecElem, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zrem", strKey, vecElem, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zremrangebylex(const string &strKey, const string &strMin, const string &strMax, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zremrangebylex", strKey, strMin, strMax, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zremrangebyrank(const string &strKey, long nStart, long nStop, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zremrangebyrank", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zremrangebyscore(const string &strKey, double dMin, double dMax, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zremrangebyscore", strKey, ConvertToString(dMin), ConvertToString(dMax), HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zrevrange(const string &strKey, long nStart, long nStop, vector<string> *pvecVal, Pipeline ppLine)
{
    return ExecuteImpl("zrevrange", strKey, ConvertToString(nStart), ConvertToString(nStop), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

int CRedisClient::Zrevrangebyscore(const string &strKey, double dMax, double dMin, vector<string> *pvecVal, Pipeline ppLine)
{
    return ExecuteImpl("zrevrangebyscore", strKey, ConvertToString(dMax), ConvertToString(dMin), HASH_SLOT(strKey), ppLine, BIND_VSTR(pvecVal));
}

int CRedisClient::Zrevrangebyscore(const string &strKey, double dMax, double dMin, map<string, double> *pmapVal, Pipeline ppLine)
{
    map<string, string> mapVal;
    int nRet = ExecuteImpl("zrevrangebyscore", strKey, ConvertToString(dMax), ConvertToString(dMin), string("WITHSCORES"), HASH_SLOT(strKey), ppLine, BIND_MAP(&mapVal));
    if (nRet == RC_SUCCESS && pmapVal)
    {
        pmapVal->clear();
        for (map<string, string>::const_iterator it = mapVal.begin(); it != mapVal.end(); it++)
        {
            pmapVal->insert(make_pair(it->first, atof(it->second.c_str())));
        }
    }
    return nRet;
}

int CRedisClient::Zrevrank(const string &strKey, const string &strElem, long *pnVal, Pipeline ppLine)
{
    return ExecuteImpl("zrevrank", strKey, strElem, HASH_SLOT(strKey), ppLine, BIND_INT(pnVal));
}

int CRedisClient::Zscore(const string &strKey, const string &strElem, double *pdVal, Pipeline ppLine)
{
    string strVal;
    int nRet = ExecuteImpl("zscore", strKey, strElem, HASH_SLOT(strKey), ppLine, BIND_STR(&strVal));
    if (nRet == RC_SUCCESS)
    {
        if (strVal.empty())
        {
            nRet = RC_OBJ_NOT_EXIST;
        }
        else if (pdVal)
        {
            *pdVal = atof(strVal.c_str());
        }
    }
    return nRet;
}

int CRedisClient::Time(timeval *ptmVal, Pipeline ppLine)
{
    return ExecuteImpl("time", -1, ppLine, BIND_TIME(ptmVal));
}

Pipeline CRedisClient::CreatePipeline()
{
    return new CRedisPipeline();
}

int CRedisClient::FlushPipeline(Pipeline ppLine)
{
    if (!ppLine)
    {
        return RC_PIPELINE_ERR;
    }

    CRedisPipeline *pPipeline = static_cast<CRedisPipeline *>(ppLine);
    int nRet = pPipeline->FlushCommand(this);
    return nRet;
}

#define MacroDefine(type, func)                                            \
    int CRedisClient::FetchReply(Pipeline ppLine, type tVal)               \
    {                                                                      \
        if (!ppLine)                                                       \
        {                                                                   \
            return RC_NO_RESOURCE;                                          \
        }                                                                   \
                                                                           \
        CRedisPipeline *pPipeline = static_cast<CRedisPipeline *>(ppLine); \
        return pPipeline->FetchNext(func(tVal));                           \
    }                                                                      

MacroDefine(long *, BIND_INT)
MacroDefine(string *, BIND_STR)
MacroDefine(vector<long> *, BIND_VINT)
MacroDefine(vector<string> *, BIND_VSTR)
#undef MacroDefine

int CRedisClient::FetchReply(Pipeline ppLine, redisReply **pReply)
{
    if (!ppLine)
    {
        return RC_NO_RESOURCE;
    }

    CRedisPipeline *pPipeline = (CRedisPipeline *)(ppLine);
    return pPipeline->FetchNext(pReply);
}

void CRedisClient::FreePipeline(Pipeline ppLine)
{
    if (!ppLine)
    {
        return;
    }

    CRedisPipeline *pPipeline = (CRedisPipeline *)(ppLine);
    delete pPipeline;
}

void CRedisClient::CleanServer()
{
    m_vecSlot.clear();
    for (size_t i = 0; i < m_vecRedisServ.size(); i++)
    {
        delete m_vecRedisServ[i];
    }
    m_vecRedisServ.clear();

    m_vecSlot.clear();
    m_vecRedisServ.clear();
}

int CRedisClient::Execute(CRedisCommand *pRedisCmd, Pipeline ppLine)
{
    if (ppLine)
    {
        CRedisPipeline *pPipeline = (CRedisPipeline *)(ppLine);
        pPipeline->QueueCommand(pRedisCmd);
        return RC_SUCCESS;
    }

    int nRet = SimpleExecute(pRedisCmd);
    return nRet;
}

int CRedisClient::SimpleExecute(CRedisCommand *pRedisCmd)
{
    CRedisServer *pRedisServ = GetMatchedServer(pRedisCmd);
    if(pRedisServ)
    {
        return pRedisServ->ServRequest(pRedisCmd);
    }
    else
    {
        return RC_RQST_ERR;
    }
}

bool CRedisClient::ConvertToMapInfo(const string &strVal, map<string, string> &mapVal)
{
    stringstream ss(strVal);
    string strLine;
    while (getline(ss, strLine))
    {
        if (strLine.empty() || strLine[0] == '\r' || strLine[0] == '#')
        {
            continue;
        }

        string::size_type nPos = strLine.find(':');
        if (nPos == string::npos)
        {
            return false;
        }
        mapVal.insert(make_pair(strLine.substr(0, nPos), strLine.substr(nPos + 1)));
    }
    return true;
}

CRedisServer *CRedisClient::GetMatchedServer(const CRedisCommand *pRedisCmd) const
{
    if (!m_bCluster)
    {
        return m_vecRedisServ.empty() ? NULL : m_vecRedisServ[0];
    }
    else if (pRedisCmd->GetSlot() != -1)
    {
        return FindServer(pRedisCmd->GetSlot());
    }
    else
    {
        srand(time(NULL));
        size_t index = (rand())%(m_vecRedisServ.size());
        return m_vecRedisServ[index];
    }
}

CRedisServer *CRedisClient::FindServer(int nSlot) const
{
    auto pairIter = equal_range(m_vecSlot.begin(), m_vecSlot.end(), nSlot, CompSlot());
    if (pairIter.first != m_vecSlot.end() && pairIter.first != pairIter.second)
    {
        vector<CRedisServer *>RedisServVec = pairIter.first->RedisServVec;
        for(size_t i = 0; i < RedisServVec.size(); ++i)
        {
            if(RedisServVec[i]->IsMaster())
            {
                PLOG_DEBUG("Get Server OK,slot=%d, server ip=%s, server port=%d", nSlot, RedisServVec[i]->GetHost().c_str(), RedisServVec[i]->GetPort());
                return RedisServVec[i];
            }
        }
        PLOG_INFO("Get Server failed because all the servers are slave, slot=%d", nSlot);
        return NULL;
    }
    else
    {
        PLOG_INFO("Get Server failed because can not match slot, slot=%d", nSlot);
        return NULL;
    }
}

CRedisServer *CRedisClient::FindServer(const string &strHost, int nPort)
{
    for (size_t i = 0; i < m_vecRedisServ.size(); i++)
    {
        CRedisServer *pRedisServ = m_vecRedisServ[i];
        if (strHost == pRedisServ->GetHost() && nPort == pRedisServ->GetPort())
        {
            return pRedisServ;
        }
    }
    return NULL;
}

bool CRedisClient::InSameNode(const string &strKey1, const string &strKey2)
{
    return m_bCluster ? FindServer(HASH_SLOT(strKey1)) == FindServer(HASH_SLOT(strKey2)) : true;
}

void CRedisClient::threadFun()
{
    while (!m_bExit)
    {
        if(m_bCluster)
        {
            LoadClusterSlots();
        }
        boost::this_thread::sleep_for(boost::chrono::seconds(30));
    }
}

bool CRedisClient::LoadClusterSlots()
{
    std::vector<CRedisServer *> vecRedisServ;
    std::vector<SlotRegion> vecSlot;
    CRedisCommand redisCmd("cluster");
    redisCmd.SetArgs("slots");

    CRedisServer *pSlotServ = NULL;
    if (m_vecRedisServ[0] != NULL && m_vecRedisServ[0]->ServRequest(&redisCmd) == RC_SUCCESS &&  redisCmd.FetchResult(BIND_SLOT(&vecSlot)) == RC_SUCCESS)
    {
        for (size_t i = 0; i < vecSlot.size(); ++i)
        {
            for(size_t j = 0; j < vecSlot[i].HostVec.size(); ++j)
            {
                string ip = vecSlot[i].HostVec[j].first;
                int port = vecSlot[i].HostVec[j].second;
                if (!(pSlotServ = FindServer(ip, port)))
                {
                    pSlotServ = new CRedisServer(ip, port, m_nTimeout, m_nConnNum);
                    pSlotServ->Initialize();
                    m_vecRedisServ.push_back(pSlotServ);
                }
                vecSlot[i].RedisServVec.push_back(pSlotServ);
            }
        }
        std::sort(vecSlot.begin(), vecSlot.end());
        m_vecSlot = vecSlot;
        return true;
    }
    return false;
}