# colink-demo

流程：

+ 把provider的server设置好，get provider的id
+ 把client的server设置好，get client的id
+ client本地：在线等待query，找query是在哪个provider上。传参participants，params为query
+ client本地：执行protocol
  + client的protocol：传query到指定的provider上。
+ provider本地：执行protocol
  + provider的protocol：接收query，查询query，得到结果，传结果回到client。
+ client本地：执行protocol接收结果