### Template project for developing DStream Applications
==========

The primary focus of this project is to allow developer to quickly setup a [_**DStream**_](https://github.com/hortonworks/dstream) development 
environment to work on stand-alone Apache Tez as well as  [Apache NiFi](https://nifi.apache.org/) integrated _DStream_ applications. So as a result, this project comes with samples for each.

After cloning, your project will have all the required dependencies and build plug-ins to get started. 

#### Build and IDE Integration
This project is configured to work with both Maven and [Gradle](http://gradle.org/) for build and dependency management. 

**_Maven users:_**

Simply import the project into your IDE as Maven project

**_Gradle users (RECOMMENDED):_**

From the root of the project execute the following command:

For Eclipse: ```./gradlew clean eclipse ```

For Idea: ```./gradlew clean idea```

Then import the project as regular project (no extra plug-ins required). For more details see [IDE Integration](Getting-Started#ide-integration) section.


_**Apache NiFi**_

_DStream_ applications that need to be deployed to [Apache NiFi](https://nifi.apache.org/) need two things:

1. Since Apache NiFi integration is based on realizing DStream application as NiFi _Processor_, the application must implement a _Processor_ and define it in ```META-INF/services/org.apache.nifi.processor.Processor``` file (sample is provided with this project).
2. Application must be packaged into a NAR bundle.

To create a NAR bundle you can simply execute the following build command:

```
./gradlew clean nar
```
. . . and then copy the generated NAR file from ```build/libs/[app-name].nar``` to the ```lib``` directory of your NiFi installation.

However, you can simplify this process by executing a _deploy_ task instead, which will build, generate and deploy the NAR file 
to Apache NiFi:
```
./gradlew clean deploy -Pnifi_home=/Users/Downloads/nifi-0.2.1
```
In the above you can see that we are supplying the NiFi home directory to the task.

Once NiFi started you can now incorporate your processor as part of the NiFi flow. Below example illustrates how such flow may look like:
![](https://github.com/olegz/general-resources/blob/master/DStream-sample-nifi-flow.png)

For more details on NiFi integration, please follow documentation in [DStream-NiFi](https://github.com/hortonworks/dstream/tree/master/dstream-nifi) project.

======

For features overview and Getting started with _**DStream**_ project please follow [**Core Features Overview**](https://github.com/hortonworks/dstream/wiki/Core-Features-Overview) and [**Getting Started**](https://github.com/hortonworks/dstream/wiki) respectively.


=======
