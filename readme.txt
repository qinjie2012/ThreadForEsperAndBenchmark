1.项目采用Maven进行部署。poxm.xml
2.项目中把Esper用Esper-5.2.jar的源码带入，便于跟踪进行debug
3.项目中package com.esper.example.model中MyThreadPool为设计的线程核心类
4.Package com.espertech.esper.example.benchmark.*为设计的benchmark平台，使用时需要设计client工程和server工程
5.text*.txt为各种注册的语句类