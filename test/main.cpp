#include <stdio.h>
#include "TestBase.hpp"
#include "TestGeneric.hpp"
#include "TestString.hpp"
#include "TestList.hpp"
#include "TestSet.hpp"
#include "TestHash.hpp"
#include "TestZset.hpp"
#include "TestConcur.hpp"

int main(int argc, char **argv)
{
    //std::string strHost = "172.20.13.1";
    //int port = 70
    //std::string strHost = "127.0.0.1";

    CRedisClient *redisCli = new CRedisClient();
    string server1 = "172.20.13.1";
    string server2 = "172.20.13.1";
    string server3 = "172.20.13.1";
    string server4 = "172.20.13.1";
    string server5 = "172.20.13.1";
    string server6 = "172.20.13.1";
    int port1 = 7000;
    int port2 = 7001;
    int port3 = 7002;
    int port4 = 7003;
    int port5 = 7004;
    int port6 = 7005;
    vector<pair<string, int>>servervec;
    servervec.push_back(pair<string, int>(server1, port1));
    servervec.push_back(pair<string, int>(server2, port2));
    servervec.push_back(pair<string, int>(server3, port3));
    servervec.push_back(pair<string, int>(server4, port4));
    servervec.push_back(pair<string, int>(server5, port5));
    servervec.push_back(pair<string, int>(server5, port6));
	//redisCli->Initialize(servervec, 2, 10, true);

    while (1)
    {
        CTestBase testBase;
        if (!testBase.StartTest(servervec))
            break;

        //CTestGeneric testKeys;
        //if (!testKeys.StartTest(strHost))
        //    break;

        CTestString testStr;
        if (!testStr.StartTest(servervec))
            break;

        //CTestList testList;
        //if (!testList.StartTest(strHost))
        //    break;

        //CTestSet testSet;
        //if (!testSet.StartTest(strHost))
        //    break;

        //CTestHash testHash;
        //if (!testHash.StartTest(strHost))
        //    break;

        CTestZset testZset;
        if (!testZset.StartTest(servervec))
            break;

        //CTestConcur testConcur;
        //if (!testConcur.StartTest(strHost))
        //    break;

        break;
    }
    return 0;
}

