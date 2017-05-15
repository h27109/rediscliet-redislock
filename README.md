# 介绍

一个简单易用的C++客户端，只依赖于hiredis。支持集群和主备。支持分布式锁。

# Usage
```c++
int main()
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

    for (int i = 0; i < 10; i++)
    {
        std::string strKey("key_" + boost::to_string(i));
        if (int iret = redisCli->Del(strKey) != RC_SUCCESS)
        {
            std::cout << "del key:" << strKey << "failed, error=" << iret << std::endl;
            break;
        }
        std::string strVal("value_" + boost::to_string(i));
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
```



