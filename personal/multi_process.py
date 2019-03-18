# encoding: utf-8
import os
import time
import multiprocessing
from multiprocessing import Process
import Queue
import sys
import threading
import StringIO
import traceback
reload(sys)
sys.setdefaultencoding("utf8")

def multi_fork():
    print 'Process (%s) start...' % os.getpid()
    pid = os.fork()
    if pid==0:
        # fork process in
        print 'I am child process (%s) and my parent is %s.' % (os.getpid(), os.getppid())
        os._exit(1)
    else:
        # parent process in
        print 'I (%s) just created a child process (%s).' % (os.getpid(), pid)

def multi_multiprocessing():
    def run_proc(name):
        time.sleep(3)
        print 'Run child process %s (%s)...' % (name, os.getpid())
    print 'Parent process %s.' % os.getpid()
    processes = list()
    for i in range(5):
        p = Process(target=run_proc, args=('test',))
        print 'Process will start.'
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()
    print 'Process end.'

def multi_multiprocessing_pool_func(msg):
    print "msg:", msg
    time.sleep(3)
    print "end"

def multi_multiprocessing_pool_apply_async():
    pool = multiprocessing.Pool(processes = 3)
    for i in xrange(3):
        msg = "hello %d" %(i)
        pool.apply_async(multi_multiprocessing_pool_func, (msg, ))

    print "Mark~ Mark~ Mark~~~~~~~~~~~~~~~~~~~~~~"
    pool.close()
    pool.join()    # behind close() or terminate()
    print "Sub-process(es) done."

def multi_multiprocessing_pool_apply():
    pool = multiprocessing.Pool(processes = 3)
    for i in xrange(3):
        msg = "hello %d" %(i)
        pool.apply(multi_multiprocessing_pool_func, (msg, ))      

    print "Mark~ Mark~ Mark~~~~~~~~~~~~~~~~~~~~~~"
    pool.close()
    pool.join()    # behind close() or terminate()
    print "Sub-process(es) done."

def multi_multiprocessing_pool_func_ret(msg):
    print "msg:", msg
    time.sleep(3)
    print "end"
    return "done" + msg
def multi_multiprocessing_pool_apply_ret():
    pool = multiprocessing.Pool(processes=4)
    result = []
    for i in xrange(3):
        msg = "hello %d" %(i)
        result.append(pool.apply_async(multi_multiprocessing_pool_func_ret, (msg, )))
    pool.close()
    pool.join()
    for res in result:
        print ":::", res.get()
    print "Sub-process(es) done."

def multi_multiprocessing_pool_in_class1_f(x):
    return x*x
class multi_multiprocessing_pool_in_class1(object):
    # 类中使用进程池会一般会出现错误
    # PicklingError: Can't pickle <type 'instancemethod'>: attribute lookup __builtin__.instancemethod failed
    # 这时候你就要重新查看代码是否有不可序列化的变量了。
    # 如果有的话可以更改成全局变量解决。
    def __init__(self, func):
        self.f = func
    def go(self):
        pool = multiprocessing.Pool(processes=4)
        #result = pool.apply_async(self.f, [10])     
        #print result.get(timeout=1)           
        print pool.map(self.f, range(10))

def multi_multiprocessing_pool_in_class2_f(client, x):
    return client.f(x)
class multi_multiprocessing_pool_in_class2(object):
    def __init__(self):
        pass

    def f(self, x):
        return x*x

    def go(self):
        result = list()
        pool = multiprocessing.Pool(processes=4)
        for i in range(10):
            result.append(pool.apply_async(multi_multiprocessing_pool_in_class2_f, [self, i]))
        pool.close()
        pool.join()
        for res in result:
            print res.get(timeout=1)   


# multi_fork()
# print '================== multi_fork <END> ================='
# multi_multiprocessing()
# print '============ multi_multiprocessing <END> ============'
# multi_multiprocessing_pool_apply_async()
# print '===== multi_multiprocessing_pool_apply_async <END> ====='
# multi_multiprocessing_pool_apply()
# print '======== multi_multiprocessing_pool_apply <END> ========'
# multi_multiprocessing_pool_apply_ret()
# print '====== multi_multiprocessing_pool_apply_ret <END> ======'
# multi_multiprocessing_pool_in_class1(multi_multiprocessing_pool_in_class1_f).go()
# print '====== multi_multiprocessing_pool_in_class1 <END> ======'
# multi_multiprocessing_pool_in_class2().go()
# print '====== multi_multiprocessing_pool_in_class2 <END> ======'



class MyThread(threading.Thread):
    """Background thread connected to the requests/results queues."""
    def __init__(self, workQueue, resultQueue, timeout=0.1, **kwds):
        threading.Thread.__init__(self, **kwds)
        self.setDaemon(True)
        self._workQueue = workQueue
        self._resultQueue = resultQueue
        self._timeout = timeout
        self._dismissed = threading.Event()
        self.start()

    def run(self):
        """Repeatedly process the job queue until told to exit."""
        while True:
            if self._dismissed.isSet():
                break

            handlerKey = None  # unique key
            code = 0  # callback return code
            handlerRet = None
            errMsg = ""

            try:
                callable, args, kwds = self._workQueue.get(True, self._timeout)
            except Queue.Empty:
                continue
            except:
                exceptMsg = StringIO.StringIO()
                traceback.print_exc(file=exceptMsg)
                errMsg = exceptMsg.getvalue()
                code = 3301  # system error
                self._resultQueue.put((handlerKey, code, (callable, args, kwds), errMsg))
                break

            if self._dismissed.isSet():
                self._workQueue.put((callable, args, kwds))
                break

            try:
                if "handlerKey" in kwds:
                    handlerKey = kwds["handlerKey"]
                handlerRet = callable(*args, **kwds)  # block
                self._resultQueue.put((handlerKey, code, handlerRet, errMsg))
            except:
                exceptMsg = StringIO.StringIO()
                # traceback.print_exc(file=exceptMsg)
                errMsg = exceptMsg.getvalue()
                code = 3303
                self._resultQueue.put((handlerKey, code, handlerRet, errMsg))

    def dismiss(self):
        """Sets a flag to tell the thread to exit when done with current job."""
        self._dismissed.set()

class ThreadPool(object):
    def __init__(self, workerNums=3, timeout=0.1):
        self._workerNums = workerNums
        self._timeout = timeout
        self._workQueue = Queue.Queue()  # no maximum
        self._resultQueue = Queue.Queue()
        self.workers = []
        self.dismissedWorkers = []
        self._createWorkers(self._workerNums)

    def _createWorkers(self, workerNums):
        """Add num_workers worker threads to the pool."""
        for i in range(workerNums):
            worker = MyThread(self._workQueue, self._resultQueue, timeout=self._timeout)
            self.workers.append(worker)

    def _dismissWorkers(self, workerNums, _join=False):
        """Tell num_workers worker threads to quit after their current task."""
        dismissList = []
        for i in range(min(workerNums, len(self.workers))):
            worker = self.workers.pop()
            worker.dismiss()
            dismissList.append(worker)

        if _join:
            for worker in dismissList:
                worker.join()
        else:
            self.dismissedWorkers.extend(dismissList)

    def _joinAllDissmissedWorkers(self):
        """
        Perform Thread.join() on all
        worker threads that have been dismissed.
        """
        for worker in self.dismissedWorkers:
            worker.join()
        self.dismissedWorkers = []

    def addJob(self, callable, *args, **kwds):
        self._workQueue.put((callable, args, kwds))

    def getResult(self, block=False, timeout=0.1):
        try:
            item = self._resultQueue.get(block, timeout)
            return item
        except Queue.Empty, e:
            return None
        except:
            raise

    def waitForComplete(self, timeout=0.1):
        """
        Last function. To dismiss all worker threads. Delete ThreadPool.
        :param timeout
        """
        while True:
            workerNums = self._workQueue.qsize()  # 释放掉所有线程
            runWorkers = len(self.workers)

            if 0 == workerNums:
                time.sleep(timeout)  # waiting for thread to do job 
                self._dismissWorkers(runWorkers)
                break
            # if workerNums < runWorkers:  # 不能这样子乱取消
            #     self._dismissWorkers(runWorkers - workerNums)
            time.sleep(timeout)
        self._joinAllDissmissedWorkers()

def muti_thread():
    def doSomething(*args, **kwds):
        if "sleep" in kwds:
            sleep = kwds["sleep"]
        msgTxt = "sleep %fs.." % sleep
        time.sleep(sleep)
        return msgTxt

    for i in range(10):
        print doSomething(sleep=0.1, handlerKey="key-%d"%i)

    wm = ThreadPool(10)
    for i in range(10):
        wm.addJob(doSomething, sleep=1, handlerKey="key-%d"%i)
    wm.waitForComplete()
    for i in range(10):
        print wm.getResult()
    del wm

    def doSomething_(*args, **kwds):
        sleep = int(args[0])
        msgTxt = "sleep %ds.." % sleep
        time.sleep(sleep)
        return msgTxt

    wm = ThreadPool(10)
    result = []
    for i in range(10):
        data = 5
        wm.addJob(doSomething_, data)

    while 1:
        res = wm.getResult()
        if res:
            result.append(res)
        if 10 == len(result):
            break
        print "sleep 0.1 xxx"
        time.sleep(0.1)
    print time.time()
    wm.waitForComplete()
    print time.time()

def multi_multiprocessing_with_thread_pool():
    # 有一种情景下需要使用到多进程和多线程：
    #   在CPU密集型的情况下一个ip的处理速度是0.04秒前后，单线程运行的时间大概是3m32s，单个CPU使用率100%；
    #   使用进程池（size=10）时间大概是6m50s，其中只有1个进程的CPU使用率达到90%，其他均是在30%左右；
    #   使用线程池（size=10）时间大概是4m39s，单个CPU使用率100%
    # 可以看出使用多进程在这时候并不占优势，反而更慢。因为进程间的切换消耗了大部分资源和时间，而一个ip只需要0.04秒。
    # 而使用线程池由于只能利用单核CPU，则再怎么加大线程数量都没法提升速度，所以这时候应该使用多进程加多线程结合。
    # 机器有8个CPU，则使用8个进程加线程池，速度提升到35s，8个CPU的利用率均在50%左右，机器平均CPU75%左右。
    # self.getData()
    def doSomething_handle(*args, **kwds):
        if "sleep" in kwds:
            sleep = kwds["sleep"]
        msgTxt = "sleep %fs.." % sleep
        time.sleep(sleep)
        return msgTxt
    ipInfo = list()
    for x in xrange(1,1000000):
        ipInfo.append(x)
    ipNums = len(ipInfo)
    print ipNums
    step = ipNums / multiprocessing.cpu_count()
    ipList = list()
    i = 0
    j = 1
    processList = list()
    for ip in ipInfo:
        ipList.append(ip)
        i += 1
        if i == step * j or i == ipNums:
            j += 1
            def innerRun():
                wm = ThreadPool(5)
                for myIp in ipList:
                    wm.addJob(doSomething_handle, myIp)
                wm.waitForComplete()
            process = multiprocessing.Process(target=innerRun)
            process.start()
            processList.append(process)
            ipList = list()
    for process in processList:
        process.join()

multi_multiprocessing_with_thread_pool()
print '===== multi_multiprocessing_with_thread_pool <END> ====='
muti_thread()
print '=================== muti_thread <END> =================='