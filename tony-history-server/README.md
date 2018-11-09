# Tony History Server

## Build and run the project

- To build and run the project, cd into root of project:
```
$ cd tony-history-server
```

- Build the project with Gradle (installed):
```
$ gradle build
```
- or with the wrapper:
```
$ ./gradlew build
```
**Note:** make sure you allow execution permission (`chmod +x gradlew`) to script before running if encountering permission issue. All the command line arguments below will be the same for both installed gradle and gradle wrapper script.

- To run the app:
```
$ gradle runPlayBinary
```
**Note:** this will only reload when receiving new request, __not__ when file changes. For reloading on file changes (hot reloading):
```
$ gradle runPlayBinary -t
```

- After the message `<=============> 100% EXECUTING ...` displays, go to <http://localhost:9000> on your browser to see the app.

- To run tests:
```
$ gradle testPlayBinary
```

For more info about developing with Play using Gradle, click [here](https://docs.gradle.org/current/userguide/play_plugin.html#play_continuous_build).

## Deploy to test cluster
Before using the script, ensure that you have set execution permission (`chmod +x buildAndDeploy.sh`)

- To bundle the history server app and deploy to another host
```
$ ./buildAndDeploy.sh user hostname.test.abc.com
```

See [script](./buildAndDeploy.sh) for more details.

- To start the server after bundling with `gradle dist`:
1. `unzip $root/build/distributions/tony-history-server.zip`
2. `$root/build/distributions/tony-history-server/bin/startTHS $JAVA_OPTS`

For instance, if you want to set app's port, you can pass it in as arguments:
`$root/build/distributions/tony-history-server/bin/startTHS -Dhttp.port=8080`

See [script](./startTHS) for more details.
