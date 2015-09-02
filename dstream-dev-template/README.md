### Template project for developing DStream Applications
==========

The primary focus of this project is to allow developer to quickly setup a [_**DStream**_](https://github.com/hortonworks/dstream) development 
environment to work on both stand-alone and [Apache NiFi](https://nifi.apache.org/) integrated _DStream_ applications. So as a result, this project comes with both samples.

After cloning, your project will have all the required dependencies and build plug-ins to get started. You may then rename the project. 
> IMPORTANT: When renaming the project, ensure you rename the relevant properties in ```settings.gradle```, ```build.gradle```, ```gradle.properties```. Just look for ```#RENAME``` or ```//RENAME``` tags above the relevant property in these 3 files.


#### Build and Dependency Management
This project uses [Gradle](http://gradle.org/) for build and dependency management. To get the list of available build tasks simply execute ```./gradlew clean tasks``` from the root directory of the project.


_**Development environment setup**_

To setup an integrated development environment (i.e., Eclipse, Idea etc) please follow the [IDE Integration](Getting-Started#ide-integration) section.


_**Stand-alone mode**_

Stand-alone applications could be executed right from the IDE. If you want to package the application into a JAR 
and execute it outside of IDE simply build it with:

```
./gradlew clean installApp
``` 

or 

```
./gradlew clean distZip
```

For more information on this feature please follow [Application Plug-in](https://docs.gradle.org/current/userguide/application_plugin.html) 


_**Apache NiFi**_

_DStream_ applications that need to be deployed to [Apache NiFi](https://nifi.apache.org/) need two things:

1. Since Apache NiFi integration is based on realizing DStream application as NiFi _Processor_, the application must implement a _Processor_ and define it in ```META-INF/services/org.apache.nifi.processor.Processor``` file (sample is provided with this project).
2. Application must be first packaged into a NAR bundle.

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

For more details on NiFi integration, please follow documentation in [DStream-NiFi](https://github.com/hortonworks/dstream/tree/master/dstream-nifi) project.

======

For features overview and Getting started with _**DStream**_ project please follow [**Core Features Overview**](https://github.com/hortonworks/dstream/wiki/Core-Features-Overview) and [**Getting Started**](https://github.com/hortonworks/dstream/wiki) respectively.


=======
